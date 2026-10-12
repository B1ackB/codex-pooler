defmodule CodexPooler.Dev.CodexRetainedUsageFixtureTest do
  use ExUnit.Case, async: true

  alias CodexPooler.Dev.CodexRetainedUsageFixture

  test "refuses foreign databases before resource acquisition" do
    assert_raise ArgumentError, fn -> CodexRetainedUsageFixture.validate_database!("codex_pooler_dev") end
    assert :ok = CodexRetainedUsageFixture.validate_database!("codex_pooler_relqa_0123456789abcdef")
  end

  test "terminal hold returns immediately and releases only its writer" do
    parent = self()

    writer = fn _, _ ->
      send(parent, :delivered)
      :ok
    end

    assert :ok = CodexRetainedUsageFixture.hold_writer(parent, "synthetic-thread", writer, "synthetic", :terminal)
    assert_receive {:retained_writer, "synthetic-thread", pid}
    assert Process.alive?(pid)
    refute_received :delivered
    monitor = Process.monitor(pid)
    send(pid, :deliver)
    assert_receive :delivered
    assert_receive {:DOWN, ^monitor, :process, ^pid, :normal}
  end

  test "discard finishes the held writer without delivery" do
    parent = self()

    writer = fn _, _ ->
      send(parent, :delivered)
      :ok
    end

    CodexRetainedUsageFixture.hold_writer(parent, "synthetic-thread", writer, "synthetic", :terminal)
    assert_receive {:retained_writer, _, pid}
    monitor = Process.monitor(pid)
    send(pid, :discard)
    assert_receive {:DOWN, ^monitor, :process, ^pid, :normal}
    refute_received :delivered
  end

  test "loaded BEAM hash uses the actual Erlang return shape and matches loaded bytes" do
    hashes = CodexRetainedUsageFixture.loaded_beam_hashes()
    assert map_size(hashes) == 5
    assert Enum.all?(hashes, fn {_module, digest} -> Regex.match?(~r/\A[a-f0-9]{64}\z/, digest) end)
  end

  test "coordinator death releases only its own held writer without delivering" do
    parent = self()

    coordinator =
      spawn(fn ->
        CodexRetainedUsageFixture.hold_writer(self(), "synthetic-thread", fn _, _ -> send(parent, :delivered) end, "synthetic", :terminal)

        receive do
          {:retained_writer, _, writer} ->
            send(parent, {:writer, writer})

            receive do
              :stop -> :ok
            end
        end
      end)

    assert_receive {:writer, writer}
    monitor = Process.monitor(writer)
    send(coordinator, :stop)
    assert_receive {:DOWN, ^monitor, :process, ^writer, :normal}
    refute_received :delivered
  end
end

defmodule CodexPooler.Dev.CodexRetainedUsageFixtureQueryTest do
  use CodexPoolerWeb.ConnCase, async: false
  import Phoenix.LiveViewTest
  import CodexPooler.PoolerFixtures

  alias CodexPooler.Accounting
  alias CodexPooler.Accounting.LedgerEntry
  alias CodexPooler.AccountingTestSupport
  alias CodexPooler.Dev.CodexRetainedUsageFixtureData
  alias CodexPooler.Dev.CodexRetainedUsageFixtureUI
  alias CodexPooler.Gateway.Payloads.RequestOptions
  alias CodexPooler.Gateway.Websocket
  alias CodexPooler.Pools
  alias CodexPooler.Repo

  setup :register_and_log_in_user

  test "fixture barrier holds real asynchronous detail SQL while the connected view accepts pause", %{conn: conn, scope: scope} do
    {:ok, pool} = Pools.create_pool(scope, %{slug: "retained-query-#{System.unique_integer([:positive])}", name: "Synthetic queries"})
    %{api_key: key} = active_api_key_fixture(pool)
    request = request_fixture(%{pool: pool, api_key: key})
    {:ok, view, _} = live(conn, ~p"/admin/request-logs?pool_id=#{pool.id}")
    render_async(view, 15_000)
    render_click(view, "open_request_log", %{"request-id" => request.id})
    render_async(view, 15_000)
    token = CodexRetainedUsageFixtureUI.arm_query_barrier(self(), request.id)
    on_exit(fn -> :telemetry.detach(token) end)
    send(view.pid, :refresh_request_logs_from_events)
    assert_receive {:ui_query_held, query, ^token}, 15_000
    monitor = Process.monitor(query)
    on_exit(fn -> send(query, {token, :release}) end)
    render_hook(view, "set_live_updates", %{"paused" => true})
    assert Process.alive?(view.pid)
    send(query, {token, :release})
    assert_receive {:DOWN, ^monitor, :process, ^query, _}, 15_000
    render_async(view, 15_000)
  end

  test "real unknown settlement estimates remain null while a measured zero remains known" do
    setup = AccountingTestSupport.accounting_setup()
    {:ok, session} = Websocket.start_codex_session(setup.auth, %{accepted_turn_state: Ecto.UUID.generate()})
    {:ok, %{request: request}} = Accounting.reserve(setup.auth, setup.model, %{"max_output_tokens" => 512}, %{requested_model: setup.model.exposed_model_id, transport: "websocket", endpoint: "/backend-api/codex/responses", correlation_id: Ecto.UUID.generate()})
    options = RequestOptions.for_websocket(%{}) |> RequestOptions.put_continuity(semantic_turn_key: <<1::256>>)
    {:ok, _turn} = Websocket.start_codex_turn(session, request, options)
    {:ok, attempt} = Accounting.create_attempt(request, setup.assignment, %{transport: "websocket"})
    {:ok, _} = Accounting.finalize_failure(request, attempt, %{last_error_code: "client_disconnected", response_status_code: 499})
    entry = Repo.get_by!(LedgerEntry, request_id: request.id, entry_kind: "settlement", amount_status: "recorded")
    assert is_integer(entry.total_tokens) and entry.total_tokens > 0
    [unknown] = CodexRetainedUsageFixtureData.rows(session.id)
    assert unknown.usageSource == "usage_unknown"
    assert is_nil(unknown.inputTokens) and is_nil(unknown.outputTokens) and is_nil(unknown.totalTokens)
    assert {:ok, _} = Accounting.finalize_success_with_disposition(request, attempt, %{status: "usage_known", input_tokens: 0, output_tokens: 0, total_tokens: 0})
    [known] = CodexRetainedUsageFixtureData.rows(session.id)
    assert {known.usageSource, known.inputTokens, known.outputTokens, known.totalTokens} == {"usage_known", nil, nil, nil}
    assert known.requestStatus == "failed"
  end
end

defmodule CodexPooler.Dev.CodexRetainedUsageFixtureDrainTraceTest do
  use ExUnit.Case, async: false
  alias CodexPooler.Dev.CodexRetainedUsageFixture
  alias CodexPooler.Dev.CodexRetainedUsageFixtureDrainTrace, as: DrainTrace
  alias CodexPooler.Gateway.Transports.Websocket.ActivityRegistry

  setup do
    registry = start_supervised!({ActivityRegistry, name: __MODULE__})

    on_exit(fn ->
      :erlang.trace_pattern({ActivityRegistry, :handle_call, 3}, false, [:local])
    end)

    %{registry: registry}
  end

  test "caller exit removes real registry trace and pattern while fixture stays alive", %{registry: registry} do
    assert {:ok, trace} = DrainTrace.start(self(), registry, fn -> GenServer.call(:missing_fixture_drain, :drain) end, 1_000)
    monitor = Process.monitor(trace)
    assert_receive {:drain_finished, ^trace, %{result: "error", fixture_failure: "drain_caller_exit"}}, 2_000
    assert_receive {:DOWN, ^monitor, :process, ^trace, :normal}
    assert_clean(registry)
  end

  test "killed caller, deadline and repeated stop clean exact scoped trace", %{registry: registry} do
    for mode <- [:kill, :deadline, :stop] do
      parent = self()

      {:ok, trace} =
        DrainTrace.start(
          self(),
          registry,
          fn ->
            send(parent, {:drain_caller, self()})

            receive do
              :finish -> :ok
            end
          end,
          200
        )

      monitor = Process.monitor(trace)
      assert_receive {:drain_caller, caller}
      assert {:flags, flags} = :erlang.trace_info(registry, :flags)
      assert :call in flags
      assert {:traced, :local} = :erlang.trace_info({ActivityRegistry, :handle_call, 3}, :traced)

      case mode do
        :kill ->
          Process.exit(caller, :kill)

        :deadline ->
          :ok

        :stop ->
          DrainTrace.stop(trace)
          DrainTrace.stop(trace)
      end

      assert_receive {:drain_finished, ^trace, %{result: "error"}}, 2_000
      assert_receive {:DOWN, ^monitor, :process, ^trace, :normal}
      refute Process.alive?(caller)
      assert_clean(registry)
    end
  end

  test "fixture death cleans trace even without terminate callback", %{registry: registry} do
    parent = self()

    fixture =
      spawn(fn ->
        receive do
          :stop -> :ok
        end
      end)

    {:ok, trace} =
      DrainTrace.start(
        fixture,
        registry,
        fn ->
          send(parent, {:drain_caller, self()})

          receive do
            :finish -> :ok
          end
        end,
        1_000
      )

    monitor = Process.monitor(trace)
    assert_receive {:drain_caller, caller}
    Process.exit(fixture, :kill)
    assert_receive {:DOWN, ^monitor, :process, ^trace, :normal}, 2_000
    refute Process.alive?(caller)
    assert_clean(registry)
  end

  test "actual registry callback closed result is forwarded and normal drain summary preserved", %{registry: registry} do
    parent = self()

    {:ok, trace} =
      DrainTrace.start(
        self(),
        registry,
        fn ->
          ActivityRegistry.begin_drain(name: __MODULE__)
          ActivityRegistry.receipt_cohort(name: __MODULE__)
          send(parent, :cohort_called)
          %{result: "ok", receipt_settlements_seen: 0}
        end,
        1_000
      )

    assert_receive :cohort_called
    assert_receive {:drain_cohort_closed, ^trace, epoch}, 2_000
    assert is_reference(epoch)
    assert_receive {:drain_finished, ^trace, %{result: "ok", receipt_settlements_seen: 0}}, 2_000
    assert_clean(registry)
  end

  test "fixture terminate callback clears drain trace before unrelated cleanup", %{registry: registry} do
    parent = self()

    {:ok, trace} =
      DrainTrace.start(
        self(),
        registry,
        fn ->
          send(parent, {:drain_caller, self()})

          receive do
            :finish -> :ok
          end
        end,
        1_000
      )

    assert_receive {:drain_caller, caller}
    {:ok, listener} = :gen_tcp.listen(0, [:binary, active: false, ip: {127, 0, 0, 1}])
    CodexRetainedUsageFixture.terminate(:shutdown, %{drain_trace: trace, listener: listener, ui_barrier: nil, threads: %{}, owner_trace: nil})
    assert_receive {:drain_finished, ^trace, %{result: "error", fixture_failure: "drain_fixture_stopped"}}
    refute Process.alive?(caller)
    assert_clean(registry)
  end

  defp assert_clean(registry) do
    assert {:flags, flags} = :erlang.trace_info(registry, :flags)
    refute :call in flags
    assert {:traced, false} = :erlang.trace_info({ActivityRegistry, :handle_call, 3}, :traced)
    assert Process.alive?(registry)
  end
end

defmodule CodexPooler.Dev.CodexRetainedUsageFixtureConsistencyTest do
  use CodexPooler.DataCase, async: false
  import CodexPooler.PoolerFixtures
  import CodexPooler.UnboxedFixture
  alias CodexPooler.{Accounting, AccountingTestSupport, AccountsFixtures, Repo}
  alias CodexPooler.Catalog.PricingSnapshot
  alias CodexPooler.Dev.CodexRetainedUsageFixture
  alias CodexPooler.Dev.CodexRetainedUsageFixtureData, as: Data
  alias CodexPooler.Gateway.Payloads.RequestOptions
  alias CodexPooler.Gateway.Runtime.Finalization.AttemptSettlement
  alias CodexPooler.Gateway.Transports.Websocket.ActivityRegistry
  alias CodexPooler.Gateway.Websocket
  alias CodexPooler.Upstreams.Schemas.{PoolUpstreamAssignment, UpstreamIdentity}
  alias Ecto.Adapters.SQL.Sandbox

  setup context do
    CodexPooler.DataCase.stop_sandbox(context.sandbox_owner, context.sandbox_settings_cache)
    Sandbox.mode(Repo, :auto)
    on_exit(fn -> Sandbox.mode(Repo, :manual) end)
    AccountsFixtures.committed_bootstrap_owner_fixture!()
    label = "retained-snapshot-#{System.unique_integer([:positive])}"

    register_unboxed_cleanup!(fn ->
      identities = Repo.all(from i in UpstreamIdentity, where: i.account_label == ^label, select: i.id)
      pools = Repo.all(from a in PoolUpstreamAssignment, where: a.upstream_identity_id in ^identities, select: a.pool_id)
      delete_committed_pools!(pools)
      Repo.delete_all(from i in UpstreamIdentity, where: i.id in ^identities)
      Repo.delete_all(from p in PricingSnapshot, where: p.price_version == ^label)
    end)

    setup = AccountingTestSupport.accounting_setup(%{account_label: label, price_version: label})
    {:ok, session} = Websocket.start_codex_session(setup.auth, %{session_header: Ecto.UUID.generate(), session_header_source: "x-codex-window-id", owner_instance_id: Atom.to_string(node())})
    {:ok, %{request: request}} = Accounting.reserve(setup.auth, setup.model, %{}, %{requested_model: setup.model.exposed_model_id, transport: "websocket", endpoint: "/backend-api/codex/responses", correlation_id: Ecto.UUID.generate()})
    options = RequestOptions.for_websocket(%{}) |> RequestOptions.put_continuity(semantic_turn_key: <<1::256>>)
    {:ok, _} = Websocket.start_codex_turn(session, request, options)
    {:ok, attempt} = Accounting.create_attempt(request, setup.assignment, %{transport: "websocket"})
    %{setup: setup, session: session, request: request, attempt: attempt}
  end

  test "commit after initial SQL cannot mix earlier request state with later measured ledger", context do
    parent = self()
    handler = {__MODULE__, make_ref()}
    once = :atomics.new(1, [])
    on_exit(fn -> :telemetry.detach(handler) end)

    :ok =
      :telemetry.attach(
        handler,
        [:codex_pooler, :repo, :query],
        fn _, _, metadata, _ ->
          if Process.get(:retained_snapshot_reader, false) and metadata[:source] == "codex_turns" and :atomics.compare_exchange(once, 1, 0, 1) == :ok do
            send(parent, {:snapshot_read, self()})

            receive do
              :continue_snapshot -> :ok
            after
              5_000 -> exit(:snapshot_barrier_timeout)
            end
          end
        end,
        nil
      )

    reader =
      spawn_monitor(fn ->
        Process.put(:retained_snapshot_reader, true)
        send(parent, {:snapshot_result, Data.rows(context.session.id)})
      end)

    {reader_pid, monitor} = reader

    on_exit(fn ->
      send(reader_pid, :continue_snapshot)
      Process.exit(reader_pid, :kill)
    end)

    assert_receive {:snapshot_read, ^reader_pid}, 5_000
    assert {:ok, _} = AttemptSettlement.finalize_success(context.request, context.attempt, %{status: "usage_known", input_tokens: 100, output_tokens: 20, total_tokens: 120}, %{})
    send(reader_pid, :continue_snapshot)
    assert_receive {:snapshot_result, [row]}, 5_000
    assert_receive {:DOWN, ^monitor, :process, ^reader_pid, :normal}, 5_000
    assert row.requestStatus == "in_progress"
    assert row.attemptStatus == "in_progress"
    assert row.turnStatus == "in_progress"
    assert row.effectiveSettlementCount == 0
    assert row.usageSource == "usage_unknown"
    assert is_nil(row.totalTokens)
    [fresh] = Data.rows(context.session.id)
    assert {fresh.requestStatus, fresh.attemptStatus, fresh.turnStatus, fresh.totalTokens, fresh.effectiveSettlementCount} == {"succeeded", "succeeded", "succeeded", 120, 1}
  end

  test "snapshot keeps pending receipt observation when its real commit follows the database read", context do
    parent = self()
    {:ok, owner} = CodexRetainedUsageFixture.start_owner(context.session, self(), "snapshot-thread", false)

    on_exit(fn ->
      if Process.alive?(owner), do: GenServer.stop(owner, :normal)
    end)

    worker =
      spawn(fn ->
        receive do
          {:complete, token} ->
            {:ok, _} = AttemptSettlement.finalize_success(context.request, context.attempt, %{status: "usage_known", input_tokens: 100, output_tokens: 20, total_tokens: 120}, %{})
            :ok = ActivityRegistry.complete_receipt_settlement(token, :inserted)
            send(parent, :receipt_committed)

          :stop ->
            :ok
        end
      end)

    on_exit(fn -> send(worker, :stop) end)
    identity = %{request_id: context.request.id, attempt_id: context.attempt.id, replay_generation: context.attempt.replay_generation}
    {:ok, producer} = ActivityRegistry.begin_receipt_producer(identity)
    {:ok, token} = ActivityRegistry.register_receipt_settlement(producer, identity, worker)
    :ok = ActivityRegistry.finish_receipt_producer(producer)
    state = %{nonce: "synthetic", closed_cohort: nil, threads: %{"snapshot-thread" => %{session: context.session, owner: owner, writers: [], bindings: %{}}}}
    command = {:control, %{"nonce" => "synthetic", "op" => "snapshot", "threadId" => "snapshot-thread"}}
    handler = {__MODULE__, make_ref()}
    on_exit(fn -> :telemetry.detach(handler) end)
    once = :atomics.new(1, [])

    :ok =
      :telemetry.attach(
        handler,
        [:codex_pooler, :repo, :query],
        fn _, _, metadata, _ ->
          if Process.get(:retained_snapshot_reader, false) and metadata[:source] == "codex_turns" and :atomics.compare_exchange(once, 1, 0, 1) == :ok do
            send(parent, {:snapshot_read, self()})

            receive do
              :continue_snapshot -> :ok
            after
              5_000 -> exit(:snapshot_barrier_timeout)
            end
          end
        end,
        nil
      )

    {reader, monitor} =
      spawn_monitor(fn ->
        Process.put(:retained_snapshot_reader, true)
        send(parent, {:observed_snapshot, CodexRetainedUsageFixture.handle_call(command, nil, state)})
      end)

    on_exit(fn ->
      send(reader, :continue_snapshot)
      Process.exit(reader, :kill)
    end)

    assert_receive {:snapshot_read, ^reader}, 5_000
    send(worker, {:complete, token})
    assert_receive :receipt_committed, 5_000
    send(reader, :continue_snapshot)
    assert_receive {:observed_snapshot, {:reply, snapshot, next_state}}, 5_000
    assert_receive {:DOWN, ^monitor, :process, ^reader, :normal}, 5_000
    assert %{entries: [%{receiptIdentity: ^identity}]} = snapshot.registry
    assert [%{registry: same_registry}] = snapshot.registries
    assert same_registry == snapshot.registry
    assert snapshot.owner.requestId == nil
    assert [%{requestStatus: "in_progress", usageSource: "usage_unknown", totalTokens: nil}] = snapshot.rows
    assert {:reply, fresh, _} = CodexRetainedUsageFixture.handle_call(command, nil, next_state)
    assert [%{registry: %{entries: []}}] = fresh.registries
    assert [%{requestStatus: "succeeded", usageSource: "usage_known", totalTokens: 120}] = fresh.rows
  end
end
