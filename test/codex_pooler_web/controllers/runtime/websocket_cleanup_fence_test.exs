defmodule CodexPoolerWeb.Runtime.WebsocketCleanupFenceTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias CodexPoolerWeb.Runtime.WebsocketCleanupFence
  alias CodexPoolerWeb.WebsocketControlPath

  test "an earlier socket's cleanup cannot complete the next socket's fence" do
    :ok = WebsocketCleanupFence.install!(server: self())
    earlier = start_fence_socket!()
    current = start_fence_socket!()

    send(earlier, :finish)
    assert :ok = WebsocketCleanupFence.await_listener_socket_cleanup!(earlier)

    assert_raise ExUnit.AssertionError, ~r/listener socket cleanup did not finish/, fn ->
      WebsocketCleanupFence.await_listener_socket_cleanup!(current, 0)
    end

    send(current, :finish)
    assert :ok = WebsocketCleanupFence.await_listener_socket_cleanup!(current)
  end

  defp start_fence_socket! do
    before = WebsocketCleanupFence.listener_sockets()
    parent = self()

    socket =
      spawn_link(fn ->
        Process.put(:"$ancestors", [parent])
        :telemetry.execute([:bandit, :websocket, :start], %{}, %{})

        receive do
          :finish -> :telemetry.execute([:codex_pooler, :gateway, :websocket_control, :cleanup_finished], %{}, %{caller: self()})
        end
      end)

    on_exit(fn -> Process.exit(socket, :kill) end)
    assert WebsocketCleanupFence.await_new_listener_socket!(before) == socket
    socket
  end

  @slow_handler :websocket_cleanup_fence_test_slow_config
  # Long enough to outlast the fence's release under load; on_exit time is
  # outside the duration guard.
  @slow_change_ms 250
  @detection_timeout_ms 5_000
  @session_cleanup_timeout_ms 15_000

  # A handler whose configuration change is still running when the fence
  # releases. `:logger` runs handler additions, removals and configuration
  # changes one at a time, so the capture handler swap the fence's holder
  # makes on release queues behind this change. A removal snapshots the
  # primary config (global level included) when it is requested and writes
  # that snapshot back when it runs; the queue turns the narrow race between
  # that write and a later level restore into an ordered one. A handler
  # configuration change never writes the primary config itself.
  @doc false
  def log(_event, _config), do: :ok

  @doc false
  def changing_config(_set_or_update, _old_config, %{config: %{notify: pid} = config} = new_config) do
    send(pid, {__MODULE__, :change_started})
    Process.sleep(@slow_change_ms)
    {:ok, %{new_config | config: %{config | phase: :changed}}}
  end

  # A test that raises the global level and restores it in an `on_exit`
  # registered before the fence was left at `:info` when that restore ran
  # while the holder's capture handler swap was still pending: the swap wrote
  # back the level it had snapshotted before the restore (gate wave3h,
  # findings#206 row 206-160). The fence now returns only once its holder has
  # closed both captures, so the swap is over and the console handler is back
  # when the next callback runs, and the restore is the last level write.
  test "the fence release finishes its capture handler swap before earlier on_exit callbacks run" do
    configured_level = Logger.level()

    # Registered first, so it runs last.
    on_exit(fn ->
      settled? = await_handlers_settled(System.monotonic_time(:millisecond) + @detection_timeout_ms)
      level = Logger.level()
      _removed = :logger.remove_handler(@slow_handler)
      Logger.configure(level: configured_level)
      assert settled?, "the logger handler swap did not settle"
      assert level == configured_level
    end)

    Logger.configure(level: :info)
    on_exit(fn -> Logger.configure(level: configured_level) end)

    # Runs right after the fence's release.
    on_exit(fn -> assert :default in :logger.get_handler_ids() end)

    :ok = WebsocketCleanupFence.install!()

    # Registered after the fence, so it runs before the fence's release.
    on_exit(fn -> occupy_handler_queue!() end)
  end

  # `DataCase.stop_sandbox/2` calls it before the owner stops, in every test: a
  # session cleanup deferred past `terminate/2` that outlived its test failed
  # on a sandbox `OwnershipError` and lost its writes (findings#206 row 206-405).
  test "await_session_cleanups!/0 returns only once an in-flight session cleanup has finished" do
    test_pid = self()

    log =
      capture_log(fn ->
        :ok =
          WebsocketControlPath.cleanup(fn ->
            send(test_pid, {:cleanup_started, self()})

            receive do
              :release -> :ok
            end
          end)
      end)

    assert log =~ "phase=terminate reason=cleanup_deferred"
    assert_receive {:cleanup_started, cleanup}, @session_cleanup_timeout_ms
    on_exit(fn -> send(cleanup, :release) end)

    waiter = Task.async(fn -> WebsocketCleanupFence.await_session_cleanups!() end)

    # It found the held cleanup once it monitors it; it must still be waiting.
    assert await_monitored_by(cleanup, waiter.pid, System.monotonic_time(:millisecond) + @session_cleanup_timeout_ms)
    assert Task.yield(waiter, 0) == nil

    send(cleanup, :release)
    assert Task.await(waiter, @session_cleanup_timeout_ms) == :ok
    refute Process.alive?(cleanup)
  end

  describe "registered receipt ownership" do
    setup tags do
      CodexPooler.DataCase.setup_sandbox(tags)
    end

    test "the sandbox fence waits for real ResponseTask receipt SQL", %{sandbox_owner: owner} do
      alias CodexPooler.Accounting.Attempt
      alias CodexPooler.Gateway.Transports.Websocket.ActivityRegistry
      alias CodexPooler.Gateway.Websocket.{DeliveryReceipt, DeliveryReceiptHandoff, ResponseTask}
      alias CodexPooler.Repo

      setup = CodexPooler.AccountingTestSupport.accounting_setup()
      {:ok, reserved} = CodexPooler.Accounting.reserve(setup.auth, setup.model, %{"model" => setup.model.exposed_model_id, "input" => []}, %{endpoint: "/backend-api/codex/responses", transport: "websocket", correlation_id: Ecto.UUID.generate()})
      {:ok, attempt} = CodexPooler.Accounting.create_attempt(reserved.request, setup.assignment)
      identity = %{request_id: reserved.request.id, attempt_id: attempt.id, replay_generation: 0}
      parent = self()
      handler = {__MODULE__, make_ref()}
      on_exit(fn -> :telemetry.detach(handler) end)

      {:ok, actor} =
        ResponseTask.start(
          parent,
          :direct,
          fn _ ->
            {:ok, _} = DeliveryReceiptHandoff.enroll(parent, identity)
            :ok
          end,
          fn _, _ -> :ok end
        )

      monitor = Process.monitor(actor)
      on_exit(fn -> send(actor, {handler, :release}) end)
      assert_receive {:downstream_receipt_enrolled, ^actor, ^identity, token}
      assert_receive {:websocket_response_activity, ^actor, activity}
      assert_receive {:codex_response_done, ^actor, :ok}
      :ok = :telemetry.attach(handler, [:codex_pooler, :repo, :query], &__MODULE__.hold_receipt_query/4, %{actor: actor, parent: parent, tag: handler})
      receipt = DeliveryReceipt.build(%{outcome: "delivered", terminal_class: "response.completed"})

      try do
        :ok = DeliveryReceiptHandoff.publish(token, identity, receipt, {:response_task, actor, activity, :completed})
        assert_receive {^handler, :sql_held}, 5_000
        assert Process.alive?(owner)
        assert %{actor: ^actor, finished?: false} = :sys.get_state(ActivityRegistry).delivery_receipts[token]

        assert_raise ExUnit.AssertionError, ~r/receipt.*still running/, fn ->
          WebsocketCleanupFence.await_sandbox_cleanups!(owner, 100)
        end
      after
        send(actor, {handler, :release})
        assert_receive {:DOWN, ^monitor, :process, ^actor, :normal}, 5_000
        :telemetry.detach(handler)
      end

      assert Repo.get!(Attempt, attempt.id).response_metadata["downstream_delivery"] == receipt
    end

    test "an unpublished receipt is fenced without waiting for a foreign owner's actor", %{sandbox_owner: owner} do
      alias CodexPooler.Gateway.Websocket.DeliveryReceiptHandoff
      alias CodexPooler.Repo
      alias Ecto.Adapters.SQL.Sandbox

      foreign = Sandbox.start_owner!(Repo, shared: false)
      on_exit(fn -> if Process.alive?(foreign), do: Sandbox.stop_owner(foreign) end)
      assert Repo.query!("SELECT pg_backend_pid()", [], caller: owner).rows != Repo.query!("SELECT pg_backend_pid()", [], caller: foreign).rows
      receipt = start_owned_receipt!(foreign)

      try do
        assert :ok = WebsocketCleanupFence.await_sandbox_cleanups!(owner, 100)
        assert Process.alive?(receipt.actor)

        assert_raise ExUnit.AssertionError, ~r/receipt obligation.*still running/, fn ->
          WebsocketCleanupFence.await_sandbox_cleanups!(foreign, 100)
        end
      after
        :ok = DeliveryReceiptHandoff.publish(receipt.token, receipt.identity, receipt.receipt, {:response_task, receipt.actor, receipt.activity, :completed})
        assert_receive {:DOWN, ref, :process, actor, :normal}, 5_000
        assert ref == receipt.monitor and actor == receipt.actor
      end

      assert :ok = WebsocketCleanupFence.await_sandbox_cleanups!(foreign, 100)
      assert Repo.get!(CodexPooler.Accounting.Attempt, receipt.identity.attempt_id, caller: foreign).response_metadata["downstream_delivery"] == receipt.receipt
      Sandbox.stop_owner(foreign)
      refute Process.alive?(foreign)
    end

    test "the sandbox owner survives receipt actor DOWN until fallback SQL completes", %{sandbox_owner: owner, sandbox_settings_cache: cache} do
      alias CodexPooler.Gateway.Transports.Websocket.ActivityRegistry
      alias CodexPooler.Gateway.Websocket.DeliveryReceiptHandoff

      receipt = start_owned_receipt!(owner)
      owner_monitor = Process.monitor(owner)
      waiter = Task.async(fn -> CodexPooler.DataCase.stop_sandbox(owner, cache) end)
      assert await_fence_wait(waiter.pid, System.monotonic_time(:millisecond) + 5_000)
      Process.exit(receipt.actor, :kill)
      assert_receive {:DOWN, ref, :process, actor, :killed}
      assert ref == receipt.monitor and actor == receipt.actor
      handler = {__MODULE__, make_ref()}
      on_exit(fn -> :telemetry.detach(handler) end)
      :ok = :telemetry.attach(handler, [:codex_pooler, :repo, :query], &__MODULE__.hold_receipt_query/4, %{actor: :registered, token: receipt.token, parent: self(), tag: handler})
      :ok = DeliveryReceiptHandoff.publish(receipt.token, receipt.identity, receipt.receipt, {:response_task, receipt.actor, receipt.activity, :completed})
      assert_receive {^handler, :sql_held, fallback}, 5_000
      fallback_monitor = Process.monitor(fallback)
      on_exit(fn -> send(fallback, {handler, :release}) end)
      assert %{actor: ^fallback, fallback?: true, finished?: false} = :sys.get_state(ActivityRegistry).delivery_receipts[receipt.token]

      try do
        assert await_fence_wait(waiter.pid, System.monotonic_time(:millisecond) + 5_000)
        for _ <- 1..3, do: send(waiter.pid, :unrelated_interruption)
        assert Process.alive?(owner)
        assert Task.yield(waiter, 0) == nil
        assert_raise ExUnit.AssertionError, ~r/receipt.*still running/, fn -> WebsocketCleanupFence.await_sandbox_cleanups!(owner, 100) end
      after
        send(fallback, {handler, :release})
        assert :ok = Task.await(waiter, 5_000)
        :telemetry.detach(handler)
      end

      assert_receive {:DOWN, ^fallback_monitor, :process, ^fallback, :normal}
      assert_receive {:DOWN, ^owner_monitor, :process, ^owner, :normal}
      assert %{finished?: true, result: :ok} = :sys.get_state(ActivityRegistry).delivery_receipts[receipt.token]
    end
  end

  defp start_owned_receipt!(owner) do
    alias CodexPooler.Gateway.Websocket.{DeliveryReceipt, DeliveryReceiptHandoff, ResponseTask}
    alias Ecto.Adapters.SQL.Sandbox
    parent = self()

    {:ok, actor} =
      ResponseTask.start(
        parent,
        :direct,
        fn _ ->
          receive do
            :create_fixture -> :ok
          end

          setup = CodexPooler.AccountingTestSupport.accounting_setup()
          {:ok, reserved} = CodexPooler.Accounting.reserve(setup.auth, setup.model, %{"model" => setup.model.exposed_model_id, "input" => []}, %{endpoint: "/backend-api/codex/responses", transport: "websocket", correlation_id: Ecto.UUID.generate()})
          {:ok, attempt} = CodexPooler.Accounting.create_attempt(reserved.request, setup.assignment)
          identity = %{request_id: reserved.request.id, attempt_id: attempt.id, replay_generation: 0}
          {:ok, _} = DeliveryReceiptHandoff.enroll(parent, identity)
          send(parent, {:receipt_identity, self(), identity})
          :ok
        end,
        fn _, _ -> :ok end
      )

    monitor = Process.monitor(actor)
    on_exit(fn -> if Process.alive?(actor), do: Process.exit(actor, :kill) end)
    :ok = Sandbox.allow(CodexPooler.Repo, owner, actor)
    send(actor, :create_fixture)
    assert_receive {:receipt_identity, ^actor, identity}
    assert_receive {:downstream_receipt_enrolled, ^actor, ^identity, token}
    assert_receive {:websocket_response_activity, ^actor, activity}
    assert_receive {:codex_response_done, ^actor, :ok}
    %{actor: actor, monitor: monitor, identity: identity, token: token, activity: activity, receipt: DeliveryReceipt.build(%{outcome: "delivered", terminal_class: "response.completed"})}
  end

  @doc false
  def hold_receipt_query(_event, _measurements, %{query: "begin"}, config) do
    registered? = config.actor == :registered and :sys.get_state(CodexPooler.Gateway.Transports.Websocket.ActivityRegistry).delivery_receipts[config.token].actor == self()

    if config.actor == self() or registered? do
      message = if registered?, do: {config.tag, :sql_held, self()}, else: {config.tag, :sql_held}
      send(config.parent, message)

      receive do
        {tag, :release} when tag == config.tag -> :ok
      after
        5_000 -> raise "receipt SQL hold was not released"
      end
    end
  end

  def hold_receipt_query(_event, _measurements, _metadata, _config), do: :ok

  defp await_fence_wait(pid, deadline) do
    {:current_stacktrace, stack} = Process.info(pid, :current_stacktrace)

    if Enum.any?(stack, fn {module, function, _, _} -> module == WebsocketCleanupFence and function == :await_receipts end) do
      true
    else
      assert System.monotonic_time(:millisecond) < deadline, "fence did not observe the pending receipt"

      receive do
      after
        1 -> await_fence_wait(pid, deadline)
      end
    end
  end

  defp await_monitored_by(pid, monitor_pid, deadline) do
    monitored_by =
      case Process.info(pid, :monitored_by) do
        {:monitored_by, pids} -> pids
        nil -> []
      end

    cond do
      monitor_pid in monitored_by ->
        true

      System.monotonic_time(:millisecond) >= deadline ->
        false

      true ->
        receive do
        after
          1 -> await_monitored_by(pid, monitor_pid, deadline)
        end
    end
  end

  defp occupy_handler_queue! do
    notify = self()
    :ok = :logger.add_handler(@slow_handler, __MODULE__, %{config: %{notify: notify, phase: :added}})
    _changer = spawn(fn -> :logger.update_handler_config(@slow_handler, :config, %{notify: notify, phase: :changing}) end)

    receive do
      {__MODULE__, :change_started} -> :ok
    after
      @detection_timeout_ms -> flunk("the slow handler configuration change did not start")
    end
  end

  defp await_handlers_settled(deadline) do
    changed? = match?({:ok, %{config: %{phase: :changed}}}, :logger.get_handler_config(@slow_handler))

    cond do
      changed? and :default in :logger.get_handler_ids() ->
        true

      System.monotonic_time(:millisecond) >= deadline ->
        false

      true ->
        receive do
        after
          10 -> await_handlers_settled(deadline)
        end
    end
  end
end
