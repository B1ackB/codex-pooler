defmodule CodexPooler.Gateway.Websocket.DirectCleanupPostgresTest do
  use ExUnit.Case, async: false
  use CodexPooler.CommittedWriteGuard
  import Ecto.Query
  alias CodexPooler.Accounting
  alias CodexPooler.Accounting.{LedgerEntry, Request, RequestLogFact}
  alias CodexPooler.AccountingTestSupport
  alias CodexPooler.Gateway.Transports.Websocket.ActivityRegistry
  alias CodexPooler.Gateway.Websocket
  alias CodexPooler.Gateway.Websocket.{DeliveryReceipt, DeliveryReceiptHandoff, DirectCleanup}
  alias CodexPooler.Repo
  alias Ecto.Adapters.SQL.Sandbox

  for kill_phase <- [:before_commit, :after_commit] do
    test "#{kill_phase} coordinator death resolves exact bound transaction without guessed cleanup" do
      assert_killed_transaction(unquote(kill_phase))
    end
  end

  defp assert_killed_transaction(kill_phase) do
    {setup, session} = fixture()
    name = Module.concat(__MODULE__, "Registry#{System.unique_integer([:positive])}")
    start_supervised!({ActivityRegistry, name: name})
    parent = self()
    ref = make_ref()

    coordinator =
      spawn(fn ->
        Sandbox.unboxed_run(Repo, fn ->
          context = %DirectCleanup{
            registry: name,
            task: self(),
            ref: ref,
            parent: parent,
            session_id: session.id
          }

          {:ok, token} =
            ActivityRegistry.register(:direct, self(),
              name: name,
              direct_cleanup_ref: ref,
              direct_cleanup_parent: parent
            )

          :ok = ActivityRegistry.admit(token, name: name)
          :ok = ActivityRegistry.begin_direct_cleanup(context)
          send(parent, {:context, context})

          result =
            Accounting.claim_websocket_turn(setup.auth, setup.model, %{
              correlation_id: Ecto.UUID.generate(),
              endpoint: "/backend-api/codex/responses",
              direct_cleanup_bind: fn request ->
                :ok = DirectCleanup.bind(context, request)
                send(parent, {:bound, request.id})

                receive do
                  :commit -> :ok
                end
              end
            })

          send(parent, {:committed, result})

          receive do
            :finish -> :ok
          end
        end)
      end)

    monitor = Process.monitor(coordinator)
    assert_receive {:context, context}, 15_000
    assert_receive {:bound, request_id}, 15_000

    if kill_phase == :after_commit do
      send(coordinator, :commit)
      assert_receive {:committed, {:ok, _}}, 15_000
    end

    cleanup =
      Task.async(fn ->
        Sandbox.unboxed_run(Repo, fn -> DirectCleanup.cancel(context, "client_disconnected") end)
      end)

    assert Task.yield(cleanup, 0) == nil

    log =
      ExUnit.CaptureLog.capture_log(fn ->
        Process.exit(coordinator, :kill)
        assert_receive {:DOWN, ^monitor, :process, ^coordinator, :killed}, 15_000
        assert :ok = Task.await(cleanup, 15_000)
      end)

    if kill_phase == :before_commit, do: assert(log =~ "DBConnection.ConnectionError")

    Sandbox.unboxed_run(Repo, fn ->
      case kill_phase do
        :before_commit ->
          assert Repo.get(Request, request_id) == nil

        :after_commit ->
          assert %{status: "failed", last_error_code: "client_disconnected"} =
                   Repo.get!(Request, request_id)

          assert Repo.get(RequestLogFact, request_id)
      end

      assert Repo.aggregate(from(e in LedgerEntry, where: e.request_id == ^request_id), :count) ==
               0
    end)

    _consumed = ActivityRegistry.await_direct_cleanup(context)
    assert :sys.get_state(name).finished_direct == %{}
  end

  test "failed predecessor receipt cannot interrupt a replacement turn" do
    {setup, session} = fixture()

    Sandbox.unboxed_run(Repo, fn ->
      {:ok, old} =
        Accounting.reserve(setup.auth, setup.model, %{}, %{
          transport: "websocket",
          requested_model: setup.model.exposed_model_id,
          correlation_id: Ecto.UUID.generate()
        })

      {:ok, old_turn} = Websocket.start_codex_turn(session, old.request)
      old.request |> Ecto.Changeset.change(status: "failed") |> Repo.update!()
      old_turn |> Ecto.Changeset.change(status: "failed") |> Repo.update!()

      {:ok, current} =
        Accounting.reserve(setup.auth, setup.model, %{}, %{
          transport: "websocket",
          requested_model: setup.model.exposed_model_id,
          correlation_id: Ecto.UUID.generate()
        })

      {:ok, current_turn} = Websocket.start_codex_turn(session, current.request)
      current = %{current | request: Repo.reload!(current.request)}

      receipt = %{
        session_id: session.id,
        request_id: old.request.id,
        correlation_id: old.request.correlation_id,
        api_key_id: setup.api_key.id
      }

      assert :ok = DirectCleanup.interrupt(receipt, "client_disconnected")
      assert Repo.reload!(current.request) == current.request
      assert Repo.reload!(current_turn) == current_turn
      assert Repo.reload!(session).status == "active"

      assert :ok =
               DirectCleanup.interrupt(
                 %{receipt | api_key_id: Ecto.UUID.generate()},
                 "client_disconnected"
               )

      assert Repo.reload!(current.request) == current.request
    end)
  end

  for mode <- [:expiry, :fallback, :force_cut, :checked_out] do
    test "#{mode} bounds real locked receipt SQL with its original registry deadline" do
      {setup, session} = fixture()
      CodexPooler.TestAppEnv.restore_on_exit(Repo)
      Application.put_env(:codex_pooler, Repo, Keyword.put(Repo.config(), :timeout, 800))
      on_exit(fn -> Sandbox.mode(Repo, :manual) end)
      :ok = Sandbox.mode(Repo, :auto)
      registry = start_supervised!({ActivityRegistry, name: nil})
      assert {:ok, reserved} = Accounting.reserve(setup.auth, setup.model, %{}, %{transport: "websocket", requested_model: setup.model.exposed_model_id, correlation_id: Ecto.UUID.generate()})
      assert {:ok, attempt} = Accounting.create_attempt(reserved.request, setup.assignment)
      assert {:ok, _turn} = Websocket.start_codex_turn(session, reserved.request)
      identity = %{request_id: reserved.request.id, attempt_id: attempt.id, replay_generation: 0}
      {:ok, token} = DeliveryReceiptHandoff.enroll(self(), identity, name: registry)
      context = Map.put(identity, :codex_session_id, session.id)
      receipt = DeliveryReceipt.build(%{outcome: "aborted"})
      parent = self()

      holder =
        Task.async(fn ->
          Repo.transaction(
            fn ->
              Repo.one!(from s in CodexPooler.Gateway.Persistence.CodexSession, where: s.id == ^session.id, lock: "FOR UPDATE")
              %{rows: [[backend]]} = Repo.query!("SELECT pg_backend_pid()")
              send(parent, {:held_receipt_session, backend})

              receive do
                :release -> :ok
              end
            end,
            timeout: 5_000
          )
        end)

      assert_receive {:held_receipt_session, holder_backend}, 2_000

      actor =
        spawn(fn ->
          settle = fn ->
            {:ok, envelope} = ActivityRegistry.take_delivery_cleanup(token, context, receipt, name: registry)
            send(parent, {:receipt_assigned, self()})
            DeliveryReceiptHandoff.settle(envelope)
          end

          if unquote(mode) == :checked_out, do: Repo.checkout(settle, timeout: 5_000), else: settle.()
        end)

      on_exit(fn -> if Process.alive?(actor), do: Process.exit(actor, :kill) end)
      monitor = Process.monitor(actor)

      try do
        log =
          ExUnit.CaptureLog.capture_log(fn ->
            assert_receive {:receipt_assigned, ^actor}, 2_000
            deadline = :sys.get_state(registry).delivery_receipts[token].deadline_ms
            assert is_integer(deadline)
            original_backend = await_receipt_waiter!(holder_backend)

            case unquote(mode) do
              :fallback ->
                Process.exit(actor, :kill)
                assert_receive {:DOWN, ^monitor, :process, ^actor, :killed}, 2_000
                fallback = await_fallback_actor!(registry, token, actor)
                fallback_monitor = Process.monitor(fallback)
                assert :sys.get_state(registry).delivery_receipts[token].deadline_ms == deadline
                fallback_backend = await_receipt_waiter!(holder_backend, original_backend)
                assert fallback_backend != original_backend
                assert_receive {:DOWN, ^fallback_monitor, :process, ^fallback, _reason}, 2_000

              :force_cut ->
                ActivityRegistry.begin_drain(name: registry)
                assert :ok = ActivityRegistry.force_receipt_cohort(name: registry)
                assert_receive {:DOWN, ^monitor, :process, ^actor, :killed}, 2_000
                assert System.monotonic_time(:millisecond) < deadline
                assert %{closed?: true, producers_failed?: true} = ActivityRegistry.receipt_cohort(name: registry)

              mode when mode in [:expiry, :checked_out] ->
                assert_receive {:DOWN, ^monitor, :process, ^actor, _reason}, 2_000
                assert System.monotonic_time(:millisecond) >= deadline - 25
            end

            await_no_receipt_waiter!(holder_backend)
            assert Process.alive?(holder.pid)
            assert is_nil(Repo.reload!(attempt).response_metadata["downstream_delivery"])
          end)

        if unquote(mode) != :force_cut, do: assert(log =~ "receipt deadline expired" or log =~ "receipt not persisted" or log =~ "DBConnection")
      after
        send(holder.pid, :release)
        assert {:ok, :ok} = Task.await(holder, 5_000)
      end
    end
  end

  test "the first receipt deadline also cuts a worker queued for a real occupied pool" do
    {setup, _session} = fixture()
    CodexPooler.TestAppEnv.restore_on_exit(Repo)
    Application.put_env(:codex_pooler, Repo, Keyword.put(Repo.config(), :timeout, 800))
    on_exit(fn -> Sandbox.mode(Repo, :manual) end)
    :ok = Sandbox.mode(Repo, :auto)
    assert {:ok, reserved} = Accounting.reserve(setup.auth, setup.model, %{}, %{transport: "websocket", requested_model: setup.model.exposed_model_id, correlation_id: Ecto.UUID.generate()})
    assert {:ok, attempt} = Accounting.create_attempt(reserved.request, setup.assignment)
    pool = start_supervised!({Repo, name: nil, pool: DBConnection.ConnectionPool, pool_size: 1})
    registry = start_supervised!({ActivityRegistry, name: nil})
    parent = self()

    holder =
      Task.async(fn ->
        Repo.put_dynamic_repo(pool)

        Repo.checkout(
          fn ->
            send(parent, :receipt_pool_held)

            receive do
              :release -> :ok
            end
          end,
          timeout: 5_000
        )
      end)

    assert_receive :receipt_pool_held, 2_000
    identity = %{request_id: reserved.request.id, attempt_id: attempt.id, replay_generation: 0}
    {:ok, token} = DeliveryReceiptHandoff.enroll(self(), identity, name: registry)
    receipt = DeliveryReceipt.build(%{outcome: "aborted"})

    actor =
      spawn(fn ->
        Repo.put_dynamic_repo(pool)
        {:ok, envelope} = ActivityRegistry.take_delivery_cleanup(token, identity, receipt, name: registry)
        send(parent, :receipt_pool_assigned)
        DeliveryReceiptHandoff.settle(envelope)
      end)

    monitor = Process.monitor(actor)

    try do
      log =
        ExUnit.CaptureLog.capture_log(fn ->
          assert_receive :receipt_pool_assigned, 2_000
          deadline = :sys.get_state(registry).delivery_receipts[token].deadline_ms
          await_receipt_checkout!(actor)
          assert Process.alive?(holder.pid)
          assert_receive {:DOWN, ^monitor, :process, ^actor, :killed}, 2_000
          assert System.monotonic_time(:millisecond) >= deadline
          assert is_nil(Repo.reload!(attempt).response_metadata["downstream_delivery"])
        end)

      assert log =~ "receipt deadline expired"
    after
      send(holder.pid, :release)
      assert Task.await(holder, 5_000) == :ok
    end

    previous = Repo.put_dynamic_repo(pool)

    try do
      assert %{rows: [[1]]} = Repo.query!("SELECT 1")
    after
      Repo.put_dynamic_repo(previous)
    end
  end

  defp await_receipt_checkout!(actor, deadline \\ System.monotonic_time(:millisecond) + 2_000) do
    {:current_stacktrace, frames} = Process.info(actor, :current_stacktrace)

    unless Enum.any?(frames, fn {module, function, _arity, _location} -> module == DBConnection.Holder and function == :checkout_call end) do
      assert System.monotonic_time(:millisecond) < deadline

      receive do
      after
        1 -> await_receipt_checkout!(actor, deadline)
      end
    end
  end

  defp await_receipt_waiter!(holder, previous \\ nil, deadline \\ System.monotonic_time(:millisecond) + 2_000) do
    %{rows: rows} = Repo.query!("SELECT pid FROM pg_stat_activity WHERE $1 = ANY(pg_blocking_pids(pid))", [holder])

    case Enum.find(rows, fn [pid] -> pid != previous end) do
      [pid] ->
        pid

      nil ->
        assert System.monotonic_time(:millisecond) < deadline

        receive do
        after
          1 -> await_receipt_waiter!(holder, previous, deadline)
        end
    end
  end

  defp await_no_receipt_waiter!(holder, deadline \\ System.monotonic_time(:millisecond) + 2_000) do
    %{rows: rows} = Repo.query!("SELECT pid FROM pg_stat_activity WHERE $1 = ANY(pg_blocking_pids(pid))", [holder])

    if rows != [] do
      assert System.monotonic_time(:millisecond) < deadline

      receive do
      after
        1 -> await_no_receipt_waiter!(holder, deadline)
      end
    end
  end

  defp await_fallback_actor!(registry, token, previous, deadline \\ System.monotonic_time(:millisecond) + 2_000) do
    case :sys.get_state(registry).delivery_receipts[token] do
      %{actor: actor, fallback?: true} when actor != previous ->
        actor

      _ ->
        assert System.monotonic_time(:millisecond) < deadline

        receive do
        after
          1 -> await_fallback_actor!(registry, token, previous, deadline)
        end
    end
  end

  defp fixture do
    Sandbox.unboxed_run(Repo, fn ->
      setup = AccountingTestSupport.accounting_setup()

      {:ok, session} =
        Websocket.start_codex_session(setup.auth, %{accepted_turn_state: Ecto.UUID.generate()})

      on_exit(fn ->
        Sandbox.unboxed_run(Repo, fn ->
          CodexPooler.PoolerFixtures.delete_committed_pools!([setup.pool.id])
          Repo.delete!(setup.identity)
          Repo.delete!(setup.pricing)
        end)
      end)

      {setup, session}
    end)
  end
end
