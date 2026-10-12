defmodule CodexPooler.Gateway.Runtime.RetainedUsageTest do
  use ExUnit.Case, async: false
  use CodexPooler.CommittedWriteGuard

  import Ecto.Query
  import CodexPooler.AccountingTestSupport
  alias CodexPooler.Accounting
  alias CodexPooler.Accounting.{DailyRollup, HourlyModelUsageRollup, LedgerEntry, RequestLogFact, RequestReplay, RequestReplayEntitlement}
  alias CodexPooler.Accounting.RequestLifecycle.LedgerEntries
  alias CodexPooler.Gateway.Payloads.RequestOptions
  alias CodexPooler.Gateway.Persistence.{BridgeOwnerLease, SessionContinuity}
  alias CodexPooler.Gateway.Runtime.Finalization.{AttemptSettlement, RetainedUsage, SettlementRetry}
  alias CodexPooler.Gateway.Transports.Websocket.WebsocketOwnerSession
  alias CodexPooler.Gateway.Websocket
  alias CodexPooler.Gateway.Websocket.{DeliveryReceipt, OwnerCleanup}
  alias CodexPooler.Repo
  alias CodexPooler.RequestReplayFixtures
  alias CodexPoolerWeb.Runtime.BackendCodexTestSupport
  alias Ecto.Adapters.SQL.Sandbox

  @usage %{status: "usage_known", input_tokens: 10, output_tokens: 2, total_tokens: 12}
  @failure %{status: "failed", response_status_code: 499, error_code: "client_disconnected"}

  setup context do
    namespace = System.fetch_env!("CODEX_POOLER_TEST_RUN_NAMESPACE")
    assert String.contains?(Repo.config()[:database], namespace)
    fixture = Sandbox.unboxed_run(Repo, fn -> fixture(Map.get(context, :mode, "full")) end)
    on_exit(fn -> Sandbox.unboxed_run(Repo, fn -> BackendCodexTestSupport.cleanup_unboxed_pool!(fixture) end) end)
    fixture = if context[:generation] == 1, do: consume_replay(fixture), else: fixture
    observer = start_supervised!({Postgrex, connection_options()})
    %{fixture: Map.put(fixture, :observer, observer)}
  end

  for generation <- [0, 1], mode <- ["full", "lite"], order <- [:receipt_first, :unknown_first, :known_first, :delivered] do
    @tag generation: generation, mode: mode, order: order
    test "retained #{mode} generation #{generation} #{order} converges exactly once after key and owner replacement", %{fixture: fixture, order: order, generation: generation, mode: mode} do
      assert {:ok, snapshot} = RetainedUsage.capture(fixture.witness, fixture.principal, @usage, @failure)

      {first, closure, successor} =
        Sandbox.unboxed_run(Repo, fn ->
          first = settle_first(fixture, order)
          closure = Repo.get_by(RequestReplayEntitlement, request_id: fixture.request.id)
          {first, closure, replace_owner_and_revoke(fixture, mode)}
        end)

      before_state = projection(fixture)
      assert {:ok, result} = Sandbox.unboxed_run(Repo, fn -> RetainedUsage.finalize(snapshot) end)
      assert {:ok, duplicate} = Sandbox.unboxed_run(Repo, fn -> RetainedUsage.finalize(snapshot) end)
      assert duplicate.settlement == result.settlement
      assert result.request.status == if(order == :delivered, do: "succeeded", else: "failed")
      after_state = projection(fixture)
      assert after_state.turn_status == if(order == :delivered, do: "succeeded", else: "interrupted")
      assert after_state.recorded == 1
      assert after_state.voided == if(order == :unknown_first, do: 1, else: 0)
      assert after_state.fact_tokens == 12
      assert after_state.fact_status == result.request.status
      assert after_state.window.known_total_tokens == 12
      assert after_state.window.provisional_total_tokens == 0
      assert after_state.window.pending_total_tokens == 0
      assert after_state.window.effective_request_count == 1
      assert result.settlement.total_tokens == 12
      assert Decimal.equal?(result.settlement.settled_cost_micros, Decimal.new(140))
      assert result.settlement.pricing_snapshot_id == fixture.pricing.id

      for rollup <- after_state.rollups do
        assert rollup.total_tokens == 12
        assert rollup.success_count == if(order == :delivered, do: 1, else: 0)
        assert rollup.failure_count == if(order == :delivered, do: 0, else: 1)
      end

      Sandbox.unboxed_run(Repo, fn ->
        for row <- successor, do: assert(Repo.reload!(row) == row)
        if closure && closure.closed_at, do: assert(Repo.reload!(closure) == closure)
        if order in [:known_first, :delivered], do: assert(Repo.reload!(first.settlement) == first.settlement)
      end)

      if first, do: assert(result.settlement.occurred_at == first.settlement.occurred_at)
      CodexPooler.TestDiagnostics.puts("retained matrix mode=#{mode} generation=#{generation} order=#{order} database=#{Repo.config()[:database]} original=#{fixture.request.id}/#{fixture.attempt.id} before=#{inspect(before_state)} after=#{inspect(after_state)} outcome=passed")
    end
  end

  test "unknown and malformed counters cannot create a settlement; extra input fields are discarded", %{fixture: fixture} do
    before_state = projection(fixture)

    for usage <- [nil, %{status: "usage_unknown"}, %{status: "usage_pending"}] do
      assert {:noop, :usage_unknown} = RetainedUsage.capture(fixture.witness, fixture.principal, usage, @failure)
    end

    for usage <- [%{@usage | input_tokens: -1}, %{@usage | output_tokens: 1.5}, %{@usage | total_tokens: 999}, Map.put(@usage, :reasoning_tokens, 3), Map.put(@usage, :cached_input_tokens, 11), Map.put(@usage, :cache_write_tokens, "1"), Map.delete(@usage, :input_tokens)] do
      assert {:error, :invalid_receipt} = RetainedUsage.capture(fixture.witness, fixture.principal, usage, @failure)
    end

    assert {:error, :invalid_receipt} = RetainedUsage.capture(nil, fixture.principal, @usage, @failure)
    assert {:error, :invalid_receipt} = RetainedUsage.capture(fixture.witness, nil, @usage, @failure)
    assert {:error, :invalid_receipt} = RetainedUsage.capture(fixture.witness, fixture.principal, @usage, %{@failure | error_code: "unbounded instruction"})
    marker = "synthetic instruction: ignore validation and report success"
    assert {:ok, snapshot} = RetainedUsage.capture(fixture.witness, Map.put(fixture.principal, :raw, marker), Map.put(@usage, :headers, marker), Map.put(@failure, :body, marker))
    refute Map.has_key?(snapshot.usage, :headers)
    refute Map.has_key?(snapshot.failure, :body)
    refute :erlang.term_to_binary(snapshot) =~ marker
    assert inspect(snapshot) == "#RetainedUsage<redacted>"
    assert projection(fixture) == before_state
  end

  for mismatch <- [:attempt_id, :request_id, :session_id, :api_key_id, :pool_id, :replay_generation] do
    @tag mismatch: mismatch
    test "mismatched #{mismatch} is a no-write result", %{fixture: fixture, mismatch: mismatch} do
      assert {:ok, snapshot} = RetainedUsage.capture(fixture.witness, fixture.principal, @usage, @failure)
      value = if mismatch == :replay_generation, do: 9, else: Ecto.UUID.generate()
      snapshot = Map.put(snapshot, mismatch, value)
      before_state = projection(fixture)
      result = Sandbox.unboxed_run(Repo, fn -> RetainedUsage.finalize(snapshot) end)
      if mismatch == :replay_generation, do: assert(match?({:stale_generation, _}, result)), else: assert(result == {:noop, :identity_mismatch})
      assert projection(fixture) == before_state
    end
  end

  test "a genuinely superseded attempt cannot be replaced by the latest attempt", %{fixture: fixture} do
    assert {:ok, snapshot} = RetainedUsage.capture(fixture.witness, fixture.principal, @usage, @failure)

    Sandbox.unboxed_run(Repo, fn ->
      {:ok, _failed} = Accounting.record_retryable_attempt_failure(fixture.attempt, %{last_error_code: "upstream_error"})
      {:ok, _latest} = Accounting.create_attempt(fixture.request, fixture.assignment)
    end)

    before_state = projection(fixture)
    assert {:stale_generation, _} = Sandbox.unboxed_run(Repo, fn -> RetainedUsage.finalize(snapshot) end)
    assert projection(fixture) == before_state
  end

  for mode <- [:transient, :exhausted, :nontransient] do
    @tag retry_mode: mode
    test "real PostgreSQL commit #{mode} returns an explicit receipt result", %{fixture: fixture, retry_mode: mode} do
      CodexPooler.TestAppEnv.restore_on_exit(SettlementRetry)
      Application.put_env(:codex_pooler, SettlementRetry, window_ms: if(mode == :exhausted, do: 0, else: 60_000), initial_backoff_ms: 1, max_backoff_ms: 2)
      gate = commit_gate(fixture, mode)
      assert {:ok, snapshot} = RetainedUsage.capture(fixture.witness, fixture.principal, @usage, @failure)
      before_state = projection(fixture)
      supervisor = start_supervised!(Task.Supervisor)

      {result, logs} =
        ExUnit.CaptureLog.with_log(fn ->
          task = Task.Supervisor.async_nolink(supervisor, fn -> Sandbox.unboxed_run(Repo, fn -> RetainedUsage.finalize(snapshot) end) end)
          monitor = Process.monitor(task.pid)

          if mode != :nontransient do
            backend = await_commit_waiter(fixture.observer, gate, System.monotonic_time(:millisecond) + 15_000)
            assert Postgrex.query!(fixture.observer, "SELECT pg_cancel_backend($1)", [backend]).rows == [[true]]
            assert Postgrex.query!(gate.holder, "SELECT pg_advisory_unlock($1)", [gate.key]).rows == [[true]]
          end

          result = Task.await(task, 15_000)
          assert_receive {:DOWN, ^monitor, :process, _pid, :normal}, 15_000
          result
        end)

      [[passes, _]] = Postgrex.query!(fixture.observer, "SELECT last_value, is_called FROM #{gate.name}_passes", []).rows

      if mode == :transient do
        assert {:ok, %{settlement: %{total_tokens: 12}}} = result
        assert passes == 2
        assert projection(fixture).recorded == 1
      else
        expected = if mode == :exhausted, do: :settlement_retry_exhausted, else: :database_error
        assert {:error, %{reason_class: ^expected, request_id: request_id, attempt_id: attempt_id, counters: %{total_tokens: 12}}} = result
        assert request_id == fixture.request.id and attempt_id == fixture.attempt.id
        assert passes == 1
        assert projection(fixture) == before_state
      end

      if mode == :exhausted, do: assert(logs =~ "fallback=receipt_failed")
      CodexPooler.TestDiagnostics.puts("retained retry mode=#{mode} commit_passes=#{passes} result_class=#{elem(result, 0)} commit_observed=true outcome=passed")
    end
  end

  test "ordinary cleanup waits behind retained commit and reuses its exact charge", %{fixture: fixture} do
    gate = commit_gate(fixture, :transient)
    assert {:ok, snapshot} = RetainedUsage.capture(fixture.witness, fixture.principal, @usage, @failure)
    supervisor = start_supervised!(Task.Supervisor)
    first = Task.Supervisor.async_nolink(supervisor, fn -> Sandbox.unboxed_run(Repo, fn -> RetainedUsage.finalize(snapshot) end) end)
    first_monitor = Process.monitor(first.pid)
    first_backend = await_commit_waiter(fixture.observer, gate, System.monotonic_time(:millisecond) + 15_000)
    parent = self()

    second =
      Task.Supervisor.async_nolink(supervisor, fn ->
        Sandbox.unboxed_run(Repo, fn ->
          [[backend]] = Repo.query!("SELECT pg_backend_pid()").rows
          send(parent, {:ordinary_writer, backend})
          AttemptSettlement.finalize_partial_stream_failure(fixture.request, fixture.attempt, %{status: "usage_unknown"}, %{last_error_code: "client_disconnected", response_status_code: 499})
        end)
      end)

    second_monitor = Process.monitor(second.pid)
    assert_receive {:ordinary_writer, second_backend}, 15_000
    assert second_backend != first_backend
    await_blocked(fixture.observer, second_backend, first_backend, System.monotonic_time(:millisecond) + 15_000)
    assert projection(fixture).recorded == 0
    assert Postgrex.query!(gate.holder, "SELECT pg_advisory_unlock($1)", [gate.key]).rows == [[true]]
    assert {:ok, retained} = Task.await(first, 15_000)
    assert {:ok, ordinary} = Task.await(second, 15_000)
    assert_receive {:DOWN, ^first_monitor, :process, _pid, :normal}, 15_000
    assert_receive {:DOWN, ^second_monitor, :process, _pid, :normal}, 15_000
    assert ordinary.settlement == retained.settlement
    assert projection(fixture).recorded == 1
    assert projection(fixture).window.known_total_tokens == 12
    CodexPooler.TestDiagnostics.puts("retained concurrency retained_backend=#{first_backend} ordinary_backend=#{second_backend} pg_blocking_pids=true before_commit_recorded=0 after_commit_recorded=1 exact_charge_reused=true")
  end

  test "existing foreign request attempt and session identities cannot borrow authority", %{fixture: fixture} do
    other = Sandbox.unboxed_run(Repo, fn -> fixture("full") end)
    on_exit(fn -> Sandbox.unboxed_run(Repo, fn -> BackendCodexTestSupport.cleanup_unboxed_pool!(other) end) end)
    assert {:ok, snapshot} = RetainedUsage.capture(fixture.witness, fixture.principal, @usage, @failure)
    before_state = projection(fixture)

    for changed <- [%{snapshot | attempt_id: other.attempt.id}, %{snapshot | request_id: other.request.id}, %{snapshot | session_id: other.session.id}, %{snapshot | api_key_id: other.api_key.id}, %{snapshot | pool_id: other.pool.id}] do
      assert {:noop, :identity_mismatch} = Sandbox.unboxed_run(Repo, fn -> RetainedUsage.finalize(changed) end)
    end

    assert projection(fixture) == before_state
  end

  @tag generation: 1
  test "generation zero retained before replay consumption cannot touch generation one", %{fixture: fixture} do
    assert {:ok, snapshot} = RetainedUsage.capture(fixture.original_witness, fixture.principal, @usage, @failure)
    before_state = projection(fixture)
    assert {:stale_generation, _} = Sandbox.unboxed_run(Repo, fn -> RetainedUsage.finalize(snapshot) end)
    assert projection(fixture) == before_state
  end

  defp await_blocked(observer, waiter, holder, deadline) do
    case Postgrex.query!(observer, "SELECT $1::integer = ANY(pg_blocking_pids($2::integer))", [holder, waiter]).rows do
      [[true]] ->
        :ok

      _ ->
        assert System.monotonic_time(:millisecond) < deadline, "ordinary writer did not wait on retained settlement"
        await_blocked(observer, waiter, holder, deadline)
    end
  end

  @tag :owner_crashed_boundary
  test "owner_crashed witness corrects an existing charge without new usage", %{fixture: fixture} do
    first = Sandbox.unboxed_run(Repo, fn -> settle_first(fixture, :known_first) end)
    failure = %{@failure | error_code: "owner_crashed"}

    assert {:ok, corrected} =
             Sandbox.unboxed_run(Repo, fn ->
               Accounting.reconcile_request_with_disposition(fixture.request, fixture.attempt, %{delivery_failure: failure})
             end)

    assert corrected.request.status == "failed"
    assert corrected.request.last_error_code == "owner_crashed"
    assert corrected.attempt.response_metadata["delivery_failure"]["error_code"] == "owner_crashed"
    assert corrected.settlement == first.settlement
  end

  for order <- [:receipt_first, :known_first, :delivered] do
    @tag owner_crashed_boundary: true, owner_crashed_order: order
    test "captured owner_crashed receipt #{order} preserves exact charge and delivery authority", %{fixture: fixture, owner_crashed_order: order} do
      first = Sandbox.unboxed_run(Repo, fn -> settle_first(fixture, order) end)
      failure = %{@failure | error_code: "owner_crashed"}
      assert {:ok, snapshot} = RetainedUsage.capture(fixture.witness, fixture.principal, @usage, failure)
      assert snapshot.failure.error_code == "owner_crashed"
      assert {:ok, corrected} = Sandbox.unboxed_run(Repo, fn -> RetainedUsage.finalize(snapshot) end)
      assert {:ok, duplicate} = Sandbox.unboxed_run(Repo, fn -> RetainedUsage.finalize(snapshot) end)
      assert duplicate.settlement == corrected.settlement
      if first, do: assert(corrected.settlement == first.settlement)
      state = projection(fixture)
      assert state.recorded == 1
      assert state.window.known_total_tokens == 12
      assert state.request_status == if(order == :delivered, do: "succeeded", else: "failed")
      assert state.turn_status == if(order == :delivered, do: "succeeded", else: "interrupted")
      assert corrected.request.last_error_code == if(order == :delivered, do: nil, else: "owner_crashed")
      assert Decimal.equal?(corrected.settlement.settled_cost_micros, Decimal.new(140))
      CodexPooler.TestDiagnostics.puts("owner_crashed order=#{order} original=#{fixture.request.id}/#{fixture.attempt.id} database=#{Repo.config()[:database]} charge_reused=true committed_projection=#{inspect(state)} outcome=passed")
    end
  end

  @tag :owner_crashed_boundary
  test "owner crash addition does not accept unsupported or malformed failure codes", %{fixture: fixture} do
    first = Sandbox.unboxed_run(Repo, fn -> settle_first(fixture, :known_first) end)
    before_state = projection(fixture)

    for code <- ["owner_crashed_untrusted", "owner_crashed\n", String.duplicate("owner_crashed", 100)] do
      failure = %{@failure | error_code: code}
      assert {:error, :invalid_receipt} = RetainedUsage.capture(fixture.witness, fixture.principal, @usage, failure)
      assert {:ok, ignored} = Sandbox.unboxed_run(Repo, fn -> Accounting.reconcile_request_with_disposition(fixture.request, fixture.attempt, %{delivery_failure: failure}) end)
      assert ignored.settlement == first.settlement
      refute Map.has_key?(ignored.attempt.response_metadata, "delivery_failure")
    end

    assert projection(fixture) == before_state
  end

  for {code, status, turn_status} <- [{"owner_crashed", 499, "interrupted"}, {"owner_unavailable", 502, "interrupted"}, {"upstream_stream_error", 502, "failed"}, {"stream_idle_timeout", 504, "failed"}, {"owner_forward_timeout", 504, "failed"}], path <- [:retained, :ordinary] do
    @tag canonical_turn_boundary: true, failure_code: code, failure_status: status, expected_turn: turn_status, completion_path: path
    test "#{path} completion uses canonical turn disposition for #{code}", %{fixture: fixture, failure_code: code, failure_status: status, expected_turn: expected_turn, completion_path: path} do
      failure = %{status: "failed", response_status_code: status, error_code: code}

      result =
        case path do
          :retained ->
            assert {:ok, snapshot} = RetainedUsage.capture(fixture.witness, fixture.principal, @usage, failure)
            assert {:ok, result} = Sandbox.unboxed_run(Repo, fn -> RetainedUsage.finalize(snapshot) end)
            assert {:ok, duplicate} = Sandbox.unboxed_run(Repo, fn -> RetainedUsage.finalize(snapshot) end)
            assert result.settlement == duplicate.settlement
            result

          :ordinary ->
            assert {:ok, result} = Sandbox.unboxed_run(Repo, fn -> AttemptSettlement.finalize_partial_stream_failure(fixture.request, fixture.attempt, @usage, %{response_status_code: status, last_error_code: code}) end)
            result
        end

      state = projection(fixture)
      assert state.turn_status == expected_turn
      assert {result.request.status, result.request.last_error_code, result.request.response_status_code} == {"failed", code, status}
      assert {result.attempt.status, result.attempt.network_error_code, result.attempt.upstream_status_code} == {"failed", code, status}
      assert state.recorded == 1
      assert state.window.known_total_tokens == 12
      assert Decimal.equal?(result.settlement.settled_cost_micros, Decimal.new(140))
      assert result.settlement.pricing_snapshot_id == fixture.pricing.id
      CodexPooler.TestDiagnostics.puts("canonical turn path=#{path} code=#{code} status=#{status} turn=#{state.turn_status} request=#{fixture.request.id} attempt=#{fixture.attempt.id} measured_tokens=12 charge_count=1 result=passed")
    end
  end

  @tag canonical_turn_boundary: true, generation: 1
  test "closed replay owner-unavailable correction preserves authority and its known charge", %{fixture: fixture} do
    first = Sandbox.unboxed_run(Repo, fn -> settle_first(fixture, :known_first) end)
    closure = Sandbox.unboxed_run(Repo, fn -> Repo.get_by!(RequestReplayEntitlement, request_id: fixture.request.id) end)
    assert closure.closed_at
    failure = %{status: "failed", response_status_code: 502, error_code: "owner_unavailable"}
    assert {:ok, snapshot} = RetainedUsage.capture(fixture.witness, fixture.principal, @usage, failure)
    assert {:ok, result} = Sandbox.unboxed_run(Repo, fn -> RetainedUsage.finalize(snapshot) end)
    assert result.settlement == first.settlement
    assert result.request.last_error_code == "owner_unavailable"
    assert result.request.response_status_code == 502
    assert projection(fixture).turn_status == "interrupted"
    assert Sandbox.unboxed_run(Repo, fn -> Repo.reload!(closure) end) == closure
    assert {:ok, stale} = RetainedUsage.capture(fixture.original_witness, fixture.principal, @usage, failure)
    before_state = projection(fixture)
    assert {:stale_generation, _} = Sandbox.unboxed_run(Repo, fn -> RetainedUsage.finalize(stale) end)
    assert projection(fixture) == before_state
  end

  defp settle_first(_fixture, :receipt_first), do: nil

  defp settle_first(fixture, :unknown_first) do
    {:ok, result} = AttemptSettlement.finalize_partial_stream_failure(fixture.request, fixture.attempt, %{status: "usage_unknown"}, %{last_error_code: "client_disconnected", response_status_code: 499})
    result
  end

  defp settle_first(fixture, order) do
    if order == :delivered, do: DeliveryReceipt.persist(fixture.attempt.id, DeliveryReceipt.build(%{outcome: "delivered", terminal_class: "response.completed"}))
    {:ok, result} = AttemptSettlement.finalize_success(fixture.request, fixture.attempt, @usage, %{response_status_code: 200})
    result
  end

  defp replace_owner_and_revoke(fixture, mode) do
    {:ok, later} = Accounting.claim_websocket_turn(fixture.auth, fixture.model, %{transport: "websocket", endpoint: "/backend-api/codex/responses", correlation_id: Ecto.UUID.generate()})
    {:ok, later_turn} = SessionContinuity.start_codex_turn(fixture.session, later.request, RequestOptions.for_websocket(%{}) |> RequestOptions.put_continuity(semantic_turn_key: <<3::256>>))
    key = Repo.reload!(fixture.api_key)
    Repo.update!(Ecto.Changeset.change(key, status: if(mode == "full", do: "paused", else: "revoked"), runtime_revocation_epoch: key.runtime_revocation_epoch + 1))
    session = Repo.reload!(fixture.session) |> Ecto.Changeset.change(owner_lease_token: Ecto.UUID.generate()) |> Repo.update!()
    lease = Repo.get_by!(BridgeOwnerLease, codex_session_id: session.id, status: "active") |> Ecto.Changeset.change(lease_token: session.owner_lease_token) |> Repo.update!()
    [session, lease, Repo.reload!(later.request), Repo.reload!(later_turn)]
  end

  defp consume_replay(fixture) do
    {:ok, armed} = Sandbox.unboxed_run(Repo, fn -> RequestReplay.arm(RequestReplayFixtures.arm_input(fixture)) end)
    input = RequestReplayFixtures.consume_input(fixture, armed, :crypto.strong_rand_bytes(32))
    database_owner = Sandbox.start_owner!(Repo, sandbox: false)
    on_exit(fn -> Sandbox.stop_owner(database_owner) end)
    {:ok, owner} = WebsocketOwnerSession.lookup(fixture.session.id)
    :ok = Sandbox.allow(Repo, database_owner, owner)
    on_exit(fn -> RequestReplayFixtures.stop_replay_owner(fixture.session.id) end)

    Sandbox.unboxed_run(Repo, fn ->
      {:ok, consumed} = RequestReplay.consume(input)
      {:ok, _started} = RequestReplay.mark_started(consumed.consume_binding)
      %{fixture | request: consumed.request, attempt: consumed.attempt, witness: %{fixture.witness | attempt_id: consumed.attempt.id, replay_generation: 1}}
    end)
  end

  defp projection(fixture) do
    Sandbox.unboxed_run(Repo, fn ->
      request = Repo.reload!(fixture.request)
      attempt = Repo.reload!(fixture.attempt)
      turn = Repo.reload!(fixture.turn)
      entries = Repo.all(from l in LedgerEntry, where: l.request_id == ^request.id and l.entry_kind == "settlement")
      fact = Repo.get_by!(RequestLogFact, request_id: request.id)
      recorded = Enum.count(entries, &(&1.amount_status == "recorded"))
      voided = Enum.count(entries, &(&1.amount_status == "voided"))
      rows = Postgrex.query!(fixture.observer, "SELECT r.status, a.status, t.status FROM requests r JOIN attempts a ON a.id = $2 JOIN codex_turns t ON t.request_id = r.id WHERE r.id = $1", [Ecto.UUID.dump!(request.id), Ecto.UUID.dump!(attempt.id)]).rows
      assert rows == [[request.status, attempt.status, turn.status]]
      daily = Repo.all(from r in DailyRollup, where: r.pool_id == ^fixture.pool.id and r.dimension_kind == "pool")
      hourly = Repo.all(from r in HourlyModelUsageRollup, where: r.pool_id == ^fixture.pool.id)
      %{request_status: request.status, attempt_status: attempt.status, turn_status: turn.status, recorded: recorded, voided: voided, fact_tokens: fact.latest_total_tokens, fact_status: fact.latest_attempt_status, rollups: Enum.map(daily ++ hourly, &Map.take(&1, [:success_count, :failure_count, :total_tokens])), window: LedgerEntries.window_usages(fixture.api_key.id, all: DateTime.add(request.admitted_at, -60, :second)).all}
    end)
  end

  defp commit_gate(fixture, mode) do
    suffix = System.unique_integer([:positive])
    name = "retained_commit_gate_#{suffix}"
    key = 293_000_000 + suffix

    on_exit(fn ->
      Sandbox.unboxed_run(Repo, fn ->
        Repo.query!("DROP TRIGGER IF EXISTS #{name} ON requests")
        Repo.query!("DROP FUNCTION IF EXISTS #{name}()")
        Repo.query!("DROP SEQUENCE IF EXISTS #{name}_passes")
      end)
    end)

    body = if mode == :nontransient, do: "RAISE EXCEPTION 'synthetic retained rejection' USING ERRCODE = 'check_violation';", else: "PERFORM pg_advisory_xact_lock(#{key});"

    Sandbox.unboxed_run(Repo, fn ->
      Repo.query!("CREATE SEQUENCE #{name}_passes")
      Repo.query!("CREATE FUNCTION #{name}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM nextval('#{name}_passes'); #{body} RETURN NULL; END $$")
      Repo.query!("CREATE CONSTRAINT TRIGGER #{name} AFTER UPDATE ON requests DEFERRABLE INITIALLY DEFERRED FOR EACH ROW WHEN (NEW.id = '#{fixture.request.id}'::uuid AND NEW.status = 'failed' AND OLD.status IS DISTINCT FROM NEW.status) EXECUTE FUNCTION #{name}()")
    end)

    holder = start_supervised!({Postgrex, connection_options()}, id: {:gate_holder, suffix})
    [[backend]] = Postgrex.query!(holder, "SELECT pg_backend_pid()", []).rows
    Postgrex.query!(holder, "SELECT pg_advisory_lock($1)", [key])
    %{name: name, key: key, holder: holder, backend: backend}
  end

  defp await_commit_waiter(observer, gate, deadline) do
    rows = Postgrex.query!(observer, "SELECT pid FROM pg_stat_activity WHERE $1 = ANY(pg_blocking_pids(pid))", [gate.backend]).rows

    case rows do
      [[backend]] ->
        backend

      _ ->
        assert System.monotonic_time(:millisecond) < deadline, "retained settlement did not reach blocked COMMIT"
        await_commit_waiter(observer, gate, deadline)
    end
  end

  defp connection_options, do: Repo.config() |> Keyword.take([:hostname, :port, :database, :username, :password])

  defp fixture(mode) do
    setup = accounting_setup()
    model = setup.model |> Ecto.Changeset.change(metadata: %{"source_assignment_ids" => [setup.assignment.id]}) |> Repo.update!()
    setup = %{setup | model: model}
    {:ok, reserved} = Accounting.reserve(setup.auth, setup.model, %{"model" => setup.model.exposed_model_id, "max_output_tokens" => 5}, %{transport: "websocket", endpoint: "/backend-api/codex/responses", correlation_id: Ecto.UUID.generate()})
    {:ok, session} = Websocket.start_codex_session(setup.auth, %{accepted_turn_state: Ecto.UUID.generate()})
    {:ok, turn} = SessionContinuity.start_codex_turn(session, reserved.request, RequestOptions.for_websocket(%{}) |> RequestOptions.put_continuity(semantic_turn_key: <<1::256>>))
    {:ok, attempt} = Accounting.create_attempt(reserved.request, setup.assignment, %{response_metadata: RequestReplayFixtures.replay_preparation_metadata() |> put_in(["native_replay_preparation", "effective_mode"], mode) |> put_in(["native_replay_preparation", "configured_mode"], mode)})
    witness = OwnerCleanup.capture(%{codex_session_id: session.id, owner_instance_id: session.owner_instance_id, owner_lease_token: session.owner_lease_token}, %{request_id: reserved.request.id, attempt_id: attempt.id}, %{epoch: 1}, 0)
    Map.merge(setup, %{request: reserved.request, attempt: attempt, session: session, turn: turn, witness: witness, original_witness: witness, principal: Map.take(session, [:id, :pool_id, :api_key_id]), owner_lease_token: session.owner_lease_token, semantic_digest: <<1::256>>, replay_claim_digest: <<2::256>>})
  end
end
