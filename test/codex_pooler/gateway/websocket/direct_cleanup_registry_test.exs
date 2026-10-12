defmodule CodexPooler.Gateway.Websocket.DirectCleanupRegistryTest do
  use ExUnit.Case, async: true

  alias CodexPooler.Gateway.Transports.Websocket.ActivityRegistry
  alias CodexPooler.Gateway.Websocket.{DeliveryReceipt, DirectCleanup}

  test "cancellation before claim fences later mutation" do
    {context, task} = coordinator()
    assert :none = ActivityRegistry.await_direct_cleanup(context)
    send(task, {:begin, self()})
    assert_receive {:began, {:error, :cancelled}}
    send(task, :stop)
  end

  test "cancellation waits committed identity publication and prevents dispatch" do
    {context, task} = coordinator()
    send(task, {:begin, self()})
    assert_receive {:began, :ok}
    waiter = Task.async(fn -> ActivityRegistry.await_direct_cleanup(context) end)
    await_registered_waiter(context, waiter.pid)
    assert Task.yield(waiter, 0) == nil
    receipt = receipt(context)
    send(task, {:bind, receipt, self()})
    assert_receive :bound
    assert_receive {:direct_request_cleanup, ^task, _, ^receipt}
    assert Task.yield(waiter, 0) == nil
    send(task, {:ready, self()})
    assert_receive {:ready_result, {:error, :cancelled}}
    assert {:ok, ^receipt} = Task.await(waiter)
    send(task, :stop)
  end

  test "coordinator death preserves exact bound identity and releases pending waiter" do
    {context, task} = coordinator()
    send(task, {:begin, self()})
    assert_receive {:began, :ok}
    receipt = receipt(context)
    send(task, {:bind, receipt, self()})
    assert_receive :bound
    waiter = Task.async(fn -> ActivityRegistry.await_direct_cleanup(context) end)
    await_registered_waiter(context, waiter.pid)
    Process.exit(task, :kill)
    assert {:ok, ^receipt} = Task.await(waiter)
    assert {:ok, ^receipt} = ActivityRegistry.await_direct_cleanup(context)
    assert :none = ActivityRegistry.await_direct_cleanup(context)
  end

  test "forged task reference cannot cancel or publish another task" do
    {context, task} = coordinator()
    forged = %{context | ref: make_ref()}
    assert :none = ActivityRegistry.await_direct_cleanup(forged)
    assert :ok = ActivityRegistry.bind_direct_cleanup(forged, receipt(context))
    send(task, {:begin, self()})
    assert_receive {:began, :ok}
    send(task, :stop)
  end

  test "finished receipt rejects a forged context without erasing its authority" do
    {context, task} = coordinator()
    send(task, {:begin, self()})
    assert_receive {:began, :ok}
    expected = receipt(context)
    send(task, {:bind, expected, self()})
    assert_receive :bound
    monitor = Process.monitor(task)
    send(task, :stop)
    assert_receive {:DOWN, ^monitor, :process, ^task, :normal}

    assert :none =
             ActivityRegistry.await_direct_cleanup(%{context | session_id: Ecto.UUID.generate()})

    assert {:ok, ^expected} = ActivityRegistry.await_direct_cleanup(context)
    assert :sys.get_state(context.registry).finished_direct == %{}
  end

  test "finished receipt is removed when its socket parent exits" do
    parent =
      spawn(fn ->
        receive do
          :stop -> :ok
        end
      end)

    {context, task} = coordinator(parent)
    send(task, {:begin, self()})
    assert_receive {:began, :ok}
    send(task, {:bind, receipt(context), self()})
    assert_receive :bound
    monitor = Process.monitor(task)
    send(task, :stop)
    assert_receive {:DOWN, ^monitor, :process, ^task, :normal}
    registry_state = :sys.get_state(context.registry)
    assert Map.has_key?(registry_state.finished_direct, context.ref)
    parent_monitor = Process.monitor(parent)
    send(parent, :stop)
    assert_receive {:DOWN, ^parent_monitor, :process, ^parent, :normal}
    await_empty_archive(context.registry, System.monotonic_time(:millisecond) + 15_000)
    assert ActivityRegistry.await_direct_cleanup(context) == :none
  end

  # A closing socket may stop a pre-visible direct task only while it waits on
  # its upstream request: stopping it anywhere else, inside a query or its own
  # settlement, dropped its connection under load (findings#206 row 206-110).
  test "a stop is granted only inside the upstream wait and the task then exits without settling" do
    {context, task} = coordinator()
    send(task, {:begin, self()})
    assert_receive {:began, :ok}

    # Reserving, binding, settling: not waiting upstream, so not stoppable.
    assert :busy = ActivityRegistry.stop_direct_upstream_wait(context)

    send(task, {:upstream_wait, self()})
    assert_receive {:upstream_waiting, request_pid}
    assert :stop = ActivityRegistry.stop_direct_upstream_wait(context)

    # The request returns after the stop was granted: the task exits with the
    # socket's stop reason instead of going on to settle.
    monitor = Process.monitor(task)
    send(request_pid, :upstream_answered)
    assert_receive {:DOWN, ^monitor, :process, ^task, {:shutdown, :client_disconnected}}
    refute_received {:upstream_wait_result, _result}
  end

  test "an upstream wait nobody stops returns the request result and leaves the task stoppable only while waiting" do
    {context, task} = coordinator()
    send(task, {:begin, self()})
    assert_receive {:began, :ok}
    send(task, {:upstream_wait, self()})
    assert_receive {:upstream_waiting, request_pid}
    send(request_pid, :upstream_answered)
    assert_receive {:upstream_wait_result, :answered}
    assert :busy = ActivityRegistry.stop_direct_upstream_wait(context)
    send(task, :stop)
  end

  for disposition <- [:ok, {:error, :database_failure}] do
    test "a granted executor waits for exact receipt completion #{inspect(disposition)}" do
      fixture = admitted_waiting_coordinator()
      %{context: context, task: task, packet: packet, receipt_token: token} = fixture
      assert {:stop, %{token: ^token}} = DirectCleanup.grant_upstream_stop(context, packet)
      monitor = Process.monitor(task)
      send(task, :upstream_answered)
      await_executor_waiter(context.registry, token, task)
      assert Process.alive?(task)
      assert :ok = ActivityRegistry.finish_delivery_receipt(token, unquote(Macro.escape(disposition)), name: context.registry)
      assert_receive {:DOWN, ^monitor, :process, ^task, {:shutdown, :client_disconnected}}
      refute_received {:upstream_wait_result, _}
    end
  end

  test "an already committed stopped receipt releases its exact executor immediately" do
    %{context: context, task: task, packet: packet, receipt_token: token} = admitted_waiting_coordinator()
    assert {:stop, %{token: ^token}} = DirectCleanup.grant_upstream_stop(context, packet)
    assert :ok = ActivityRegistry.finish_delivery_receipt(token, :ok, name: context.registry)
    monitor = Process.monitor(task)
    send(task, :upstream_answered)
    assert_receive {:DOWN, ^monitor, :process, ^task, {:shutdown, :client_disconnected}}
  end

  test "foreign caller and mismatched private scope cannot obtain or release a stop fence" do
    %{context: context, task: task, packet: {identity, receipt}, receipt_token: token} = admitted_waiting_coordinator()

    for field <- [:request_id, :attempt_id, :replay_generation, :codex_session_id] do
      value = if field == :replay_generation, do: 1, else: Ecto.UUID.generate()
      assert {:error, :delivery_identity_unavailable} = DirectCleanup.grant_upstream_stop(context, {Map.put(identity, field, value), receipt})
    end

    assert :busy = DirectCleanup.grant_upstream_stop(%{context | ref: make_ref()}, {identity, receipt})
    assert :busy = DirectCleanup.grant_upstream_stop(%{context | parent: task}, {identity, receipt})
    assert :busy = DirectCleanup.grant_upstream_stop(%{context | session_id: Ecto.UUID.generate()}, {identity, receipt})
    assert {:error, :delivery_identity_unavailable} = DirectCleanup.grant_upstream_stop(context)
    assert {:stop, _} = DirectCleanup.grant_upstream_stop(context, {identity, receipt})
    assert {:error, :stale_direct_cleanup} = ActivityRegistry.leave_direct_upstream_wait(context)
    assert :sys.get_state(context.registry).delivery_receipts[token].executor_waiters == []
    assert Process.alive?(task)
    assert :ok = ActivityRegistry.finish_delivery_receipt(token, :ok, name: context.registry)
    send(task, :upstream_answered)
  end

  test "an admitted attempt with no enrolled receipt fails closed before granting stop" do
    %{context: context, task: task, packet: packet} = admitted_waiting_coordinator(false)
    assert {:error, :delivery_identity_unavailable} = DirectCleanup.grant_upstream_stop(context, packet)
    assert {:error, :delivery_identity_unavailable} = DirectCleanup.grant_upstream_stop(context)
    send(task, :upstream_answered)
    assert_receive {:upstream_wait_result, :answered}
    send(task, :stop)
  end

  test "socket death does not release the original executor before its assigned receipt" do
    socket =
      spawn(fn ->
        receive do
          :stop -> :ok
        end
      end)

    %{context: context, task: task, packet: packet, receipt_token: token} = admitted_waiting_coordinator(true, socket)
    assert {:stop, _} = DirectCleanup.grant_upstream_stop(context, packet)
    monitor = Process.monitor(task)
    socket_monitor = Process.monitor(socket)
    send(task, :upstream_answered)
    await_executor_waiter(context.registry, token, task)
    send(socket, :stop)
    assert_receive {:DOWN, ^socket_monitor, :process, ^socket, :normal}
    assert Process.alive?(task)
    assert :ok = ActivityRegistry.finish_delivery_receipt(token, :ok, name: context.registry)
    assert_receive {:DOWN, ^monitor, :process, ^task, {:shutdown, :client_disconnected}}
  end

  test "repeated stop grants retain the first receipt deadline and private capability" do
    %{context: context, task: task, packet: packet, receipt_token: token} = admitted_waiting_coordinator()
    assert {:stop, envelope} = DirectCleanup.grant_upstream_stop(context, packet)
    first = :sys.get_state(context.registry).delivery_receipts[token]
    assert {:stop, ^envelope} = DirectCleanup.grant_upstream_stop(context, packet)
    repeated = :sys.get_state(context.registry).delivery_receipts[token]
    assert repeated.deadline_ms == first.deadline_ms
    assert repeated.deadline_timer == first.deadline_timer
    send(context.registry, {:delivery_receipt_deadline, token, first.deadline_ms})
    assert :sys.get_state(context.registry).delivery_receipts[token].finished? == false
    assert Process.alive?(task)
    assert :ok = ActivityRegistry.finish_delivery_receipt(token, :ok, name: context.registry)
    send(task, :upstream_answered)
  end

  test "force cut ends the assigned cleanup and releases its executor as failed" do
    %{context: context, task: task, packet: packet, receipt_token: token} = admitted_waiting_coordinator()
    parent = self()

    cleanup =
      spawn(fn ->
        send(parent, {:forced_cleanup_grant, DirectCleanup.grant_upstream_stop(context, packet)})

        receive do
          :stop -> :ok
        end
      end)

    on_exit(fn -> if Process.alive?(cleanup), do: Process.exit(cleanup, :kill) end)
    assert_receive {:forced_cleanup_grant, {:stop, _}}
    cleanup_monitor = Process.monitor(cleanup)
    task_monitor = Process.monitor(task)
    send(task, :upstream_answered)
    await_executor_waiter(context.registry, token, task)
    assert Process.alive?(task)
    ActivityRegistry.begin_drain(name: context.registry)
    assert :ok = ActivityRegistry.force_receipt_cohort(name: context.registry)
    assert_receive {:DOWN, ^cleanup_monitor, :process, ^cleanup, :killed}
    assert_receive {:DOWN, ^task_monitor, :process, ^task, {:shutdown, :client_disconnected}}
    assert %{closed?: true, producers_failed?: true, pending_producers: 0} = ActivityRegistry.receipt_cohort(name: context.registry)
  end

  defp admitted_waiting_coordinator(enroll? \\ true, socket \\ self()) do
    {context, task} = coordinator(socket)
    send(task, {:begin, self()})
    assert_receive {:began, :ok}
    bound = Map.merge(receipt(context), %{attempt_id: Ecto.UUID.generate(), replay_generation: 0})
    send(task, {:bind_admitted, bound, enroll?, self()})
    assert_receive {:admitted_bound, token}
    send(task, {:upstream_wait, self()})
    assert_receive {:upstream_waiting, ^task}
    packet = {Map.take(bound, [:request_id, :attempt_id, :replay_generation]) |> Map.put(:codex_session_id, context.session_id), DeliveryReceipt.build(%{outcome: "aborted"})}
    %{context: context, task: task, packet: packet, receipt_token: token}
  end

  defp await_executor_waiter(registry, token, task, deadline \\ System.monotonic_time(:millisecond) + 2_000) do
    waiters = :sys.get_state(registry).delivery_receipts[token].executor_waiters

    unless Enum.any?(waiters, fn {pid, _} -> pid == task end) do
      assert System.monotonic_time(:millisecond) < deadline

      receive do
      after
        1 -> await_executor_waiter(registry, token, task, deadline)
      end
    end
  end

  defp coordinator(parent \\ self()) do
    name = Module.concat(__MODULE__, "Registry#{System.unique_integer([:positive])}")
    start_supervised!({ActivityRegistry, name: name})
    starter = self()
    ref = make_ref()

    task =
      spawn(fn ->
        {:ok, token} =
          ActivityRegistry.register(:direct, self(),
            name: name,
            direct_cleanup_ref: ref,
            direct_cleanup_parent: parent
          )

        :ok = ActivityRegistry.admit(token, name: name)

        context = %DirectCleanup{
          registry: name,
          task: self(),
          ref: ref,
          parent: parent,
          session_id: Ecto.UUID.generate()
        }

        send(starter, {:coordinator, context})
        loop(context)
      end)

    on_exit(fn -> if Process.alive?(task), do: Process.exit(task, :kill) end)
    assert_receive {:coordinator, context}
    {context, task}
  end

  defp loop(context) do
    receive do
      {:begin, parent} ->
        send(parent, {:began, ActivityRegistry.begin_direct_cleanup(context)})
        loop(context)

      {:bind_admitted, receipt, enroll?, parent} ->
        token =
          if enroll? do
            {:ok, token} = ActivityRegistry.enroll_delivery_receipt(context.parent, Map.take(receipt, [:request_id, :attempt_id, :replay_generation]), name: context.registry)
            token
          end

        :ok = ActivityRegistry.bind_direct_cleanup(context, receipt)
        send(parent, {:admitted_bound, token})
        loop(context)

      {:bind, receipt, parent} ->
        :ok = ActivityRegistry.bind_direct_cleanup(context, receipt)
        send(parent, :bound)
        loop(context)

      {:ready, parent} ->
        send(parent, {:ready_result, ActivityRegistry.ready_direct_cleanup(context)})
        loop(context)

      {:upstream_wait, parent} ->
        result =
          DirectCleanup.upstream_wait(context, fn ->
            send(parent, {:upstream_waiting, self()})

            receive do
              :upstream_answered -> :answered
            end
          end)

        send(parent, {:upstream_wait_result, result})
        loop(context)

      :stop ->
        :ok
    end
  end

  defp receipt(context),
    do: %{
      session_id: context.session_id,
      request_id: Ecto.UUID.generate(),
      correlation_id: Ecto.UUID.generate(),
      api_key_id: Ecto.UUID.generate()
    }

  defp await_empty_archive(registry, deadline) do
    if :sys.get_state(registry).finished_direct != %{} do
      assert System.monotonic_time(:millisecond) < deadline

      receive do
      after
        10 -> await_empty_archive(registry, deadline)
      end
    end
  end

  defp await_registered_waiter(context, pid) do
    await_registered_waiter(context, pid, System.monotonic_time(:millisecond) + 15_000)
  end

  defp await_registered_waiter(context, pid, deadline) do
    registered? =
      context.registry
      |> :sys.get_state()
      |> Map.fetch!(:activities)
      |> Enum.any?(fn {_token, entry} -> registered_waiter?(entry, context, pid) end)

    unless registered? do
      assert System.monotonic_time(:millisecond) < deadline,
             "direct cleanup waiter was not registered"

      receive do
      after
        10 -> await_registered_waiter(context, pid, deadline)
      end
    end
  end

  defp registered_waiter?(%{direct_cleanup: %{context: context, waiters: waiters}}, context, pid),
    do: Enum.any?(waiters, fn {waiter, _tag} -> waiter == pid end)

  defp registered_waiter?(_entry, _context, _pid), do: false
end
