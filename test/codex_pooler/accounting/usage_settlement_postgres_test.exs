defmodule CodexPooler.Accounting.UsageSettlementPostgresTest do
  use ExUnit.Case, async: false
  use CodexPooler.CommittedWriteGuard

  import Ecto.Query
  import CodexPooler.AccountingTestSupport

  alias CodexPooler.Accounting
  alias CodexPooler.Accounting.{DailyRollup, HourlyModelUsageRollup, LedgerEntry, RequestLogFact}
  alias CodexPooler.Accounting.RequestLifecycle.LedgerEntries
  alias CodexPooler.Accounting.RequestReplay
  alias CodexPooler.Accounting.RequestReplayEntitlement
  alias CodexPooler.Gateway.Payloads.RequestOptions
  alias CodexPooler.Gateway.Persistence.BridgeOwnerLease
  alias CodexPooler.Gateway.Persistence.SessionContinuity
  alias CodexPooler.Gateway.Runtime.Finalization.AttemptSettlement
  alias CodexPooler.Gateway.Runtime.Finalization.ResponseUsage
  alias CodexPooler.Gateway.Transports.Websocket.WebsocketOwnerSession
  alias CodexPooler.Gateway.Websocket
  alias CodexPooler.Gateway.Websocket.DeliveryReceipt
  alias CodexPooler.Repo
  alias CodexPooler.RequestReplayFixtures
  alias CodexPoolerWeb.Runtime.BackendCodexTestSupport
  alias Ecto.Adapters.SQL.Sandbox

  @detection_budget 15_000

  for source <- [:direct, :json], reasoning <- [0, 2, 3] do
    test "#{source} settlement validates reasoning #{reasoning} against output 2 before committing" do
      fixture = Sandbox.unboxed_run(Repo, fn -> accounting_setup() end)

      on_exit(fn ->
        Sandbox.unboxed_run(Repo, fn -> BackendCodexTestSupport.cleanup_unboxed_pool!(fixture) end)
      end)

      Sandbox.unboxed_run(Repo, fn ->
        {:ok, reserved} = Accounting.reserve(fixture.auth, fixture.model, %{"model" => fixture.model.exposed_model_id, "max_output_tokens" => 5}, %{})
        {:ok, attempt} = Accounting.create_attempt(reserved.request, fixture.assignment)
        reasoning = unquote(reasoning)

        usage =
          case unquote(source) do
            :direct -> %{status: "usage_known", input_tokens: 10, output_tokens: 2, reasoning_tokens: reasoning, total_tokens: 12}
            :json -> ResponseUsage.from_json(CodexPooler.JSON.encode!(%{"usage" => %{"input_tokens" => 10, "output_tokens" => 2, "output_tokens_details" => %{"reasoning_tokens" => reasoning}, "total_tokens" => 12}}))
          end

        assert {:ok, finalized} = Accounting.finalize_success(reserved.request, attempt, usage, %{response_status_code: 200})
        request_id = reserved.request.id
        entry = Repo.one!(from(e in LedgerEntry, where: e.request_id == ^request_id and e.entry_kind == "settlement" and e.amount_status == "recorded"))

        if reasoning <= 2 do
          assert finalized.request.usage_status == "usage_known"
          assert entry.usage_status == "usage_known"
          assert {entry.input_tokens, entry.output_tokens, entry.reasoning_tokens || 0, entry.total_tokens} == {10, 2, reasoning, 12}
          assert Decimal.equal?(entry.settled_cost_micros, Decimal.new(140 + 10 * reasoning))
        else
          assert finalized.request.usage_status == "usage_unknown"
          assert finalized.attempt.usage_status == "usage_unknown"
          assert entry.usage_status == "usage_unknown"
          assert entry.details["usage_source"] == "invalid_usage_tokens"
          assert entry.details["settled_cost_micros"] == nil
          assert entry.details["estimated_from_reserve"] == true
          reservation = Repo.one!(from(e in LedgerEntry, where: e.request_id == ^request_id and e.entry_kind == "reservation"))
          assert entry.total_tokens == reservation.total_tokens
          assert entry.reasoning_tokens == reservation.reasoning_tokens
        end
      end)
    end
  end

  test "committed finalizers race one late measured usage correction through PostgreSQL locks" do
    fixture =
      Sandbox.unboxed_run(Repo, fn ->
        {:ok, fixture} = Repo.transaction(&committed_fixture/0)
        fixture
      end)

    on_exit(fn ->
      Sandbox.unboxed_run(Repo, fn ->
        BackendCodexTestSupport.cleanup_unboxed_pool!(fixture)
      end)
    end)

    parent = self()
    release = make_ref()

    tasks =
      for lane <- [:first, :second] do
        Task.async(fn ->
          Sandbox.unboxed_run(Repo, fn ->
            [[backend_pid]] = Repo.query!("SELECT pg_backend_pid()").rows
            send(parent, {:backend_ready, lane, backend_pid, self()})

            receive do
              {:release, ^release} ->
                Accounting.finalize_success_with_disposition(
                  fixture.failed.request,
                  fixture.failed.attempt,
                  fixture.usage,
                  %{
                    response_status_code: 200,
                    before_finalize: fn ->
                      if lane == :first do
                        send(parent, :first_finalizer_locked)

                        receive do
                          {:finish, ^release} -> :ok
                        after
                          @detection_budget -> raise "finalization lock barrier was not released"
                        end
                      end
                    end
                  }
                )
            after
              @detection_budget -> raise "finalizer start barrier was not released"
            end
          end)
        end)
      end

    ready =
      for _lane <- 1..2 do
        assert_receive {:backend_ready, lane, backend_pid, task_pid}, @detection_budget
        {lane, backend_pid, task_pid}
      end

    assert ready |> Enum.map(&elem(&1, 1)) |> Enum.uniq() |> length() == 2
    {:first, first_backend, first_pid} = Enum.find(ready, &(elem(&1, 0) == :first))
    {:second, second_backend, second_pid} = Enum.find(ready, &(elem(&1, 0) == :second))
    send(first_pid, {:release, release})
    assert_receive :first_finalizer_locked, @detection_budget
    send(second_pid, {:release, release})

    try do
      Sandbox.unboxed_run(Repo, fn ->
        assert_blocked_by!(
          second_backend,
          first_backend,
          System.monotonic_time(:millisecond) + @detection_budget
        )
      end)
    after
      send(first_pid, {:finish, release})
    end

    results = Enum.map(tasks, &Task.await(&1, @detection_budget))
    assert Enum.count(results, &match?({:ok, %{finalization_disposition: :replaced}}, &1)) == 1
    assert Enum.count(results, &match?({:ok, %{finalization_disposition: :reused}}, &1)) == 1

    Sandbox.unboxed_run(Repo, fn ->
      request_id = fixture.failed.request.id
      assert Repo.reload!(fixture.failed.request).status == "failed"
      assert Repo.reload!(fixture.failed.attempt).network_error_code == "owner_drained"
      entries = Repo.all(from(l in LedgerEntry, where: l.request_id == ^request_id))
      settlements = Enum.filter(entries, &(&1.entry_kind == "settlement"))
      assert length(settlements) == 2
      assert Enum.count(settlements, &(&1.amount_status == "voided")) == 1
      [recorded] = Enum.filter(settlements, &(&1.amount_status == "recorded"))
      assert recorded.correction_of_entry_id == fixture.failed.settlement.id
      assert recorded.pricing_snapshot_id == fixture.pricing.id
      assert recorded.usage_status == "usage_known"
      assert recorded.input_tokens == 16
      assert recorded.cached_input_tokens == 4
      assert recorded.output_tokens == 5
      assert recorded.reasoning_tokens == 2
      assert recorded.total_tokens == 21
      assert recorded.occurred_at == fixture.failed.settlement.occurred_at

      window =
        LedgerEntries.window_usages(
          fixture.api_key.id,
          all: DateTime.add(fixture.failed.settlement.occurred_at, -60, :second)
        ).all

      assert %{
               effective_request_count: 1,
               known_total_tokens: 21,
               provisional_total_tokens: 0,
               pending_total_tokens: 0,
               effective_total_tokens: 21
             } = window

      assert Decimal.equal?(recorded.settled_cost_micros, Decimal.new(623))
      assert Enum.count(entries, &(&1.entry_kind == "reservation")) == 1
      assert Enum.count(entries, &(&1.entry_kind == "release")) == 1
      fact = Repo.get_by!(RequestLogFact, request_id: request_id)
      assert fact.latest_settlement_entry_id == recorded.id
      assert fact.latest_total_tokens == 21
      assert Decimal.equal?(fact.latest_settled_cost_micros, Decimal.new(623))

      assert [daily] =
               Repo.all(
                 from(r in DailyRollup,
                   where: r.pool_id == ^fixture.pool.id and r.dimension_kind == "pool"
                 )
               )

      assert [hourly] =
               Repo.all(from(r in HourlyModelUsageRollup, where: r.pool_id == ^fixture.pool.id))

      for rollup <- [daily, hourly] do
        assert rollup.request_count == 1
        assert rollup.success_count == 0
        assert rollup.failure_count == 1
        assert rollup.input_tokens == 16
        assert rollup.cached_input_tokens == 4
        assert rollup.output_tokens == 5
        assert rollup.reasoning_tokens == 2
        assert rollup.total_tokens == 21
        assert Decimal.equal?(rollup.settled_cost_micros, Decimal.new(623))
      end
    end)
  end

  for generation <- [0, 1], order <- [:unknown_first, :known_first, :receipt_first, :delivered] do
    @tag correction_generation: generation, correction_order: order
    test "committed generation #{generation} #{order} reconciles every projection without changing ownership", %{correction_generation: generation, correction_order: order} do
      fixture = Sandbox.unboxed_run(Repo, fn -> correction_fixture() end)
      on_exit(fn -> Sandbox.unboxed_run(Repo, fn -> BackendCodexTestSupport.cleanup_unboxed_pool!(fixture) end) end)
      observer_opts = Repo.config() |> Keyword.take([:hostname, :port, :database, :username, :password])
      observer = start_supervised!({Postgrex, observer_opts})
      [[observer_backend]] = Postgrex.query!(observer, "SELECT pg_backend_pid()", []).rows
      [[writer_backend]] = Sandbox.unboxed_run(Repo, fn -> Repo.query!("SELECT pg_backend_pid()").rows end)
      assert observer_backend != writer_backend
      fixture = Map.put(fixture, :observer, observer)
      fixture = if generation == 1, do: consume_correction_replay(fixture), else: fixture
      usage = %{status: "usage_known", input_tokens: 10, output_tokens: 2, total_tokens: 12}
      context = %{request_id: fixture.request.id, attempt_id: fixture.attempt.id, replay_generation: generation, codex_session_id: fixture.session.id, delivery_failure: %{status: "failed", response_status_code: 499, error_code: "client_disconnected"}}
      receipt = DeliveryReceipt.build(%{outcome: "aborted"})
      before_state = Sandbox.unboxed_run(Repo, fn -> projection(fixture) end)

      first =
        Sandbox.unboxed_run(Repo, fn ->
          case order do
            :unknown_first ->
              assert {:ok, result} = AttemptSettlement.finalize_partial_stream_failure(fixture.request, fixture.attempt, %{status: "usage_unknown"}, %{response_status_code: 499, last_error_code: "client_disconnected"})
              result

            :receipt_first ->
              assert :ok = DeliveryReceipt.persist(fixture.attempt.id, receipt, context)
              nil

            _other ->
              if order == :delivered do
                delivered = DeliveryReceipt.build(%{outcome: "delivered", terminal_class: "response.completed"})
                assert :ok = DeliveryReceipt.persist(fixture.attempt.id, delivered, Map.delete(context, :delivery_failure))
              end

              assert {:ok, result} = AttemptSettlement.finalize_success(fixture.request, fixture.attempt, usage, %{response_status_code: 200})
              result
          end
        end)

      closure = Sandbox.unboxed_run(Repo, fn -> Repo.get_by(RequestReplayEntitlement, request_id: fixture.request.id) end)

      {successor, lease, later_request, later_turn} =
        Sandbox.unboxed_run(Repo, fn ->
          {:ok, later} = Accounting.claim_websocket_turn(fixture.auth, fixture.model, %{transport: "websocket", endpoint: "/backend-api/codex/responses", correlation_id: Ecto.UUID.generate()})
          later_options = RequestOptions.for_websocket(%{}) |> RequestOptions.put_continuity(semantic_turn_key: <<3::256>>)
          {:ok, later_turn} = SessionContinuity.start_codex_turn(fixture.session, later.request, later_options)
          key = Repo.reload!(fixture.api_key)
          key |> Ecto.Changeset.change(status: "revoked", runtime_revocation_epoch: key.runtime_revocation_epoch + 1) |> Repo.update!()
          session = Repo.reload!(fixture.session)
          session = session |> Ecto.Changeset.change(owner_lease_token: Ecto.UUID.generate()) |> Repo.update!()
          lease = Repo.get_by!(BridgeOwnerLease, codex_session_id: session.id, status: "active") |> Ecto.Changeset.change(lease_token: session.owner_lease_token) |> Repo.update!()
          {session, lease, Repo.reload!(later.request), Repo.reload!(later_turn)}
        end)

      # A distinct committed connection reads what the first operation actually wrote.
      first_state = Sandbox.unboxed_run(Repo, fn -> projection(fixture) end)

      handler = {__MODULE__, make_ref()}
      on_exit(fn -> :telemetry.detach(handler) end)
      :ok = :telemetry.attach(handler, [:codex_pooler, :repo, :query], &__MODULE__.capture_correction_locks/4, nil)

      corrected =
        Sandbox.unboxed_run(Repo, fn ->
          attrs = %{
            usage: usage,
            delivery_failure: context.delivery_failure,
            before_commit: fn result ->
              status = if result.request.status == "succeeded", do: "succeeded", else: "interrupted"
              SessionContinuity.complete_codex_turn({:ok, result}, status, result.request.last_error_code)
              :ok
            end
          }

          attrs = if order in [:known_first, :delivered], do: Map.delete(attrs, :usage), else: attrs
          Process.put({__MODULE__, :capture_locks}, [])
          assert {:ok, result} = Accounting.reconcile_request_with_disposition(fixture.request, fixture.attempt, attrs)
          lock_sources = Process.delete({__MODULE__, :capture_locks}) |> Enum.reverse() |> Enum.uniq()
          assert Enum.find_index(lock_sources, &(&1 == "codex_sessions")) < Enum.find_index(lock_sources, &(&1 == "requests"))
          assert Enum.find_index(lock_sources, &(&1 == "requests")) < Enum.find_index(lock_sources, &(&1 == "attempts"))
          CodexPooler.TestDiagnostics.puts("task1 correction lock_sources=#{inspect(lock_sources)}")
          assert :ok = DeliveryReceipt.persist(fixture.attempt.id, receipt, context)
          assert {:ok, duplicate} = Accounting.reconcile_request_with_disposition(fixture.request, fixture.attempt, attrs)
          assert duplicate.settlement.id == result.settlement.id
          result
        end)

      after_state =
        Sandbox.unboxed_run(Repo, fn ->
          assert Repo.reload!(successor) == successor
          assert Repo.reload!(lease) == lease
          assert Repo.reload!(later_request) == later_request
          assert Repo.reload!(later_turn) == later_turn
          if closure && closure.closed_at, do: assert(Repo.reload!(closure) == closure)
          if order in [:known_first, :delivered], do: assert(Repo.reload!(first.settlement) == first.settlement)
          projection(fixture)
        end)

      expected_status = if order == :delivered, do: "succeeded", else: "failed"
      assert after_state.request_status == expected_status
      assert after_state.attempt_status == expected_status
      assert after_state.turn_status == if(order == :delivered, do: "succeeded", else: "interrupted")
      assert after_state.recorded_settlements == 1
      assert after_state.voided_settlements == if(order == :unknown_first, do: 1, else: 0)
      assert after_state.fact_tokens == 12
      assert after_state.fact_attempt_status == expected_status
      assert after_state.window.known_total_tokens == 12
      assert after_state.window.provisional_total_tokens == 0
      assert after_state.window.effective_request_count == 1
      assert after_state.window.pending_total_tokens == 0

      for rollup <- after_state.rollups do
        assert rollup == %{request_count: 1, success_count: if(order == :delivered, do: 1, else: 0), failure_count: if(order == :delivered, do: 0, else: 1), total_tokens: 12, settled_cost_micros: rollup.settled_cost_micros}
        assert Decimal.equal?(rollup.settled_cost_micros, Decimal.new(140))
      end

      if first do
        assert corrected.request.completed_at == first.request.completed_at
        assert corrected.attempt.completed_at == first.attempt.completed_at
        assert corrected.settlement.occurred_at == first.settlement.occurred_at
      end

      if order in [:known_first, :delivered], do: assert(after_state.rollup_identity == first_state.rollup_identity)
      assert corrected.settlement.pricing_snapshot_id == fixture.pricing.id
      CodexPooler.TestDiagnostics.puts("task1 committed generation=#{generation} order=#{order} before=#{inspect(before_state)} first=#{inspect(first_state)} after=#{inspect(after_state)} assertions=passed")
    end
  end

  for delivered? <- [false, true] do
    @tag ordinary_failure_without_witness: true, delivered_control: delivered?
    test "ordinary late failure without an authoritative witness preserves committed success (delivered=#{delivered?})", %{delivered_control: delivered?} do
      fixture = Sandbox.unboxed_run(Repo, fn -> correction_fixture() end)
      on_exit(fn -> Sandbox.unboxed_run(Repo, fn -> BackendCodexTestSupport.cleanup_unboxed_pool!(fixture) end) end)
      observer = start_supervised!({Postgrex, Repo.config() |> Keyword.take([:hostname, :port, :database, :username, :password])})
      fixture = Map.put(fixture, :observer, observer)
      [[observer_backend]] = Postgrex.query!(observer, "SELECT pg_backend_pid()", []).rows
      [[writer_backend]] = Sandbox.unboxed_run(Repo, fn -> Repo.query!("SELECT pg_backend_pid()").rows end)
      assert observer_backend != writer_backend

      first =
        Sandbox.unboxed_run(Repo, fn ->
          if delivered? do
            receipt = DeliveryReceipt.build(%{outcome: "delivered", terminal_class: "response.completed"})
            assert :ok = DeliveryReceipt.persist(fixture.attempt.id, receipt, %{request_id: fixture.request.id, replay_generation: 0})
          end

          assert {:ok, result} = AttemptSettlement.finalize_success(fixture.request, fixture.attempt, %{status: "usage_known", input_tokens: 10, output_tokens: 2, total_tokens: 12}, %{response_status_code: 200})
          result
        end)

      before_state = Sandbox.unboxed_run(Repo, fn -> projection(fixture) end)

      second =
        Sandbox.unboxed_run(Repo, fn ->
          assert {:ok, result} = AttemptSettlement.finalize_partial_stream_failure(fixture.request, fixture.attempt, %{status: "usage_unknown"}, %{last_error_code: "client_disconnected", response_status_code: 499})
          result
        end)

      after_state =
        Sandbox.unboxed_run(Repo, fn ->
          assert Repo.reload!(first.settlement) == first.settlement
          assert second.settlement == first.settlement
          refute Map.has_key?(Repo.reload!(fixture.attempt).response_metadata, "delivery_failure")
          projection(fixture)
        end)

      CodexPooler.TestDiagnostics.puts("task1 ordinary_no_witness delivered=#{delivered?} before=#{inspect(before_state)} after=#{inspect(after_state)}")
      assert after_state == before_state
      assert {second.request.status, second.attempt.status, after_state.turn_status} == {"succeeded", "succeeded", "succeeded"}
      assert second.finalization_disposition == :reused
    end
  end

  def capture_correction_locks(_event, _measurements, metadata, _config) do
    case Process.get({__MODULE__, :capture_locks}) do
      sources when is_list(sources) ->
        if String.contains?(metadata.query, ["FOR UPDATE", "FOR KEY SHARE", "FOR SHARE"]), do: Process.put({__MODULE__, :capture_locks}, [metadata.source | sources])

      _disabled ->
        :ok
    end
  end

  defp correction_fixture do
    setup = accounting_setup()
    model = setup.model |> Ecto.Changeset.change(metadata: %{"source_assignment_ids" => [setup.assignment.id]}) |> Repo.update!()
    setup = %{setup | model: model}
    {:ok, reserved} = Accounting.reserve(setup.auth, setup.model, %{"model" => setup.model.exposed_model_id, "max_output_tokens" => 5}, %{transport: "websocket", endpoint: "/backend-api/codex/responses", correlation_id: Ecto.UUID.generate()})
    {:ok, session} = Websocket.start_codex_session(setup.auth, %{accepted_turn_state: Ecto.UUID.generate()})
    options = RequestOptions.for_websocket(%{}) |> RequestOptions.put_continuity(semantic_turn_key: <<1::256>>)
    {:ok, turn} = SessionContinuity.start_codex_turn(session, reserved.request, options)
    {:ok, attempt} = Accounting.create_attempt(reserved.request, setup.assignment, %{response_metadata: RequestReplayFixtures.replay_preparation_metadata()})
    session = Repo.reload!(session)
    Map.merge(setup, %{request: reserved.request, attempt: attempt, session: session, turn: turn, owner_lease_token: session.owner_lease_token, semantic_digest: <<1::256>>, replay_claim_digest: <<2::256>>})
  end

  defp consume_correction_replay(fixture) do
    {:ok, armed} = Sandbox.unboxed_run(Repo, fn -> RequestReplay.arm(RequestReplayFixtures.arm_input(fixture)) end)
    input = RequestReplayFixtures.consume_input(fixture, armed, :crypto.strong_rand_bytes(32))
    database_owner = Sandbox.start_owner!(Repo, sandbox: false)
    on_exit(fn -> Sandbox.stop_owner(database_owner) end)
    {:ok, owner} = WebsocketOwnerSession.lookup(fixture.session.id)
    :ok = Sandbox.allow(Repo, database_owner, owner)
    on_exit(fn -> RequestReplayFixtures.stop_replay_owner(fixture.session.id) end)

    Sandbox.unboxed_run(Repo, fn ->
      assert {:ok, consumed} = RequestReplay.consume(input)
      assert {:ok, _started} = RequestReplay.mark_started(consumed.consume_binding)
      %{fixture | request: consumed.request, attempt: consumed.attempt}
    end)
  end

  defp projection(fixture) do
    request = Repo.reload!(fixture.request)
    attempt = Repo.reload!(fixture.attempt)
    turn = Repo.reload!(fixture.turn)
    settlements = Repo.all(from l in LedgerEntry, where: l.request_id == ^request.id and l.entry_kind == "settlement")
    fact = Repo.get_by!(RequestLogFact, request_id: request.id)
    fields = [:request_count, :success_count, :failure_count, :total_tokens, :settled_cost_micros]
    daily = Repo.all(from r in DailyRollup, where: r.pool_id == ^fixture.pool.id and r.dimension_kind == "pool")
    hourly = Repo.all(from r in HourlyModelUsageRollup, where: r.pool_id == ^fixture.pool.id)
    observer_rows = Postgrex.query!(fixture.observer, "SELECT r.status, a.status, t.status, f.latest_total_tokens, count(l.id) FILTER (WHERE l.amount_status = 'recorded'), count(l.id) FILTER (WHERE l.amount_status = 'voided') FROM requests r JOIN attempts a ON a.id = $2 JOIN codex_turns t ON t.request_id = r.id JOIN request_log_facts f ON f.request_id = r.id LEFT JOIN ledger_entries l ON l.request_id = r.id AND l.entry_kind = 'settlement' WHERE r.id = $1 GROUP BY r.status, a.status, t.status, f.latest_total_tokens", [Ecto.UUID.dump!(request.id), Ecto.UUID.dump!(attempt.id)]).rows
    assert [[request.status, attempt.status, turn.status, fact.latest_total_tokens, Enum.count(settlements, &(&1.amount_status == "recorded")), Enum.count(settlements, &(&1.amount_status == "voided"))]] == observer_rows
    %{rollup_identity: Enum.map(daily ++ hourly, &Map.take(&1, [:id, :created_at, :rollup_date, :bucket_started_at, :request_count, :admitted_request_count, :input_tokens, :output_tokens, :total_tokens, :settled_cost_micros])), observer_rows: observer_rows, request_status: request.status, attempt_status: attempt.status, turn_status: turn.status, recorded_settlements: Enum.count(settlements, &(&1.amount_status == "recorded")), voided_settlements: Enum.count(settlements, &(&1.amount_status == "voided")), fact_tokens: fact.latest_total_tokens, fact_attempt_status: fact.latest_attempt_status, rollups: Enum.map(daily ++ hourly, &Map.take(&1, fields)), window: LedgerEntries.window_usages(fixture.api_key.id, all: DateTime.add(request.admitted_at, -60, :second)).all}
  end

  defp assert_blocked_by!(waiter, holder, deadline) do
    [[blocked?]] =
      Repo.query!("SELECT $1::integer = ANY(pg_blocking_pids($2::integer))", [holder, waiter]).rows

    if not blocked? do
      assert System.monotonic_time(:millisecond) < deadline,
             "second PostgreSQL finalizer never waited on the first"

      assert_blocked_by!(waiter, holder, deadline)
    end
  end

  defp committed_fixture do
    setup =
      accounting_setup(%{
        price_version: "usage-race-#{System.unique_integer([:positive])}",
        input_token_micros: Decimal.new(25),
        cached_input_token_micros: Decimal.new(5),
        output_token_micros: Decimal.new(50),
        reasoning_token_micros: Decimal.new(75),
        request_base_micros: Decimal.new(3)
      })

    {:ok, reserved} =
      Accounting.reserve(
        setup.auth,
        setup.model,
        %{"model" => setup.model.exposed_model_id, "max_output_tokens" => 5},
        %{}
      )

    {:ok, attempt} = Accounting.create_attempt(reserved.request, setup.assignment)

    {:ok, failed} =
      Accounting.finalize_failure(reserved.request, attempt, %{
        last_error_code: "owner_drained",
        usage: %{status: "usage_unknown", source: "owner_drained"}
      })

    Map.merge(setup, %{
      failed: failed,
      usage: %{
        status: "usage_known",
        source: "late_owner_completion",
        recorded_at: DateTime.utc_now() |> DateTime.truncate(:microsecond),
        input_tokens: 16,
        cached_input_tokens: 4,
        output_tokens: 5,
        reasoning_tokens: 2,
        total_tokens: 21
      }
    })
  end
end
