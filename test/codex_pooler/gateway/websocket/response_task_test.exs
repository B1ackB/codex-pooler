defmodule CodexPooler.Gateway.Websocket.ResponseTaskTest do
  use ExUnit.Case, async: false

  alias CodexPooler.Gateway.Transports.Websocket.ActivityRegistry
  alias CodexPooler.Gateway.Websocket.ResponseTask
  alias CodexPooler.Platform.{ExecutionIdentity, ExecutionRegistry}
  alias CodexPooler.TestProcess

  # Failure-detection budget for a response task or watcher that must exit on
  # a socket-death signal; never a scenario timer.
  @shutdown_detection_ms 15_000

  setup do
    registry = :"websocket-response-task-registry-#{System.unique_integer([:positive])}"
    start_supervised!({ActivityRegistry, name: registry})
    {:ok, registry: registry}
  end

  test "a delivered response completes its execution rather than leaving it to the process exit",
       %{registry: registry} do
    parent = self()

    {:ok, pid} =
      ResponseTask.start(
        parent,
        :direct,
        fn _task_pid ->
          send(parent, {:execution, ExecutionIdentity.local()})
          :ok
        end,
        fn _task_pid, _reason -> :ok end,
        activity_registry: registry
      )

    assert_receive {:execution, %{owner_execution_id: execution_id}}
    monitor = Process.monitor(pid)
    assert_receive {:websocket_response_activity, ^pid, token}
    assert_receive {:codex_response_done, ^pid, :ok}
    send(pid, {:websocket_response_delivery_ack, token, :completed})
    assert_receive {:DOWN, ^monitor, :process, ^pid, :normal}

    # The proof is pending publication with the delivery's end kind, not the
    # exit's (no publisher runs in the test environment, so it stays pending).
    proof =
      ExecutionRegistry.pending(10_000)
      |> Enum.find(&(&1.owner_execution_id == execution_id))

    assert %{end_kind: "completed"} = proof
  end

  test "registers and gates before invoking upstream work", %{registry: registry} do
    parent = self()

    {:ok, pid} =
      ResponseTask.start(
        parent,
        :direct,
        fn task_pid ->
          send(parent, {:upstream_started, task_pid, ActivityRegistry.activities(name: registry)})
          :ok
        end,
        fn _task_pid, _reason -> :ok end,
        activity_registry: registry
      )

    assert_receive {:upstream_started, ^pid, [%{kind: :direct, pid: ^pid, status: :admitted}]}
    monitor = Process.monitor(pid)
    assert_receive {:websocket_response_activity, ^pid, token}
    assert_receive {:codex_response_done, ^pid, :ok}
    assert {:active, :admitted} = ActivityRegistry.status(token, name: registry)
    assert :ok = ResponseTask.acknowledge_delivery(pid, token)
    assert_receive {:DOWN, ^monitor, :process, ^pid, :normal}
    assert ActivityRegistry.activities(name: registry) == []
  end

  test "admitted proxy dispatch runs in the registered response task before delivery ack", %{
    registry: registry
  } do
    parent = self()

    {:ok, pid} =
      ResponseTask.start(
        parent,
        :proxy,
        fn task_pid ->
          send(parent, {:proxy_dispatch_started, self(), task_pid})
          :ok
        end,
        fn _task_pid, _reason -> :ok end,
        activity_registry: registry
      )

    monitor = Process.monitor(pid)
    assert_receive {:proxy_dispatch_started, ^pid, ^pid}
    assert_receive {:websocket_response_activity, ^pid, token}
    assert_receive {:codex_response_done, ^pid, :ok}
    assert Process.alive?(pid)
    assert :ok = ResponseTask.acknowledge_delivery(pid, token)
    assert_receive {:DOWN, ^monitor, :process, ^pid, :normal}
  end

  test "queued or new work after cutoff returns owner_drained without upstream work", %{
    registry: registry
  } do
    assert {_epoch, []} = ActivityRegistry.begin_drain(name: registry)
    parent = self()

    {:ok, pid} =
      ResponseTask.start(
        parent,
        :proxy,
        fn _task_pid ->
          send(parent, :forbidden_upstream_dispatch)
          :ok
        end,
        fn _task_pid, _reason -> :ok end,
        activity_registry: registry
      )

    assert_receive {:codex_response_done, ^pid, {:error, :owner_drained}}
    refute_received :forbidden_upstream_dispatch
  end

  test "deadline cancellation invokes the reason-preserving cleanup once and reports owner_drained",
       %{
         registry: registry
       } do
    parent = self()

    {:ok, pid} =
      ResponseTask.start(
        parent,
        :proxy,
        fn _task_pid ->
          send(parent, {:proxy_upstream_started, ExecutionIdentity.local()})

          receive do
            :release_proxy_work -> :ok
          end
        end,
        fn task_pid, reason -> send(parent, {:proxy_cancelled, task_pid, reason}) end,
        activity_registry: registry
      )

    assert_receive {:proxy_upstream_started, execution}
    assert {_epoch, [%{token: token, pid: ^pid}]} = ActivityRegistry.begin_drain(name: registry)
    monitor = Process.monitor(pid)
    assert :ok = ActivityRegistry.cancel(token, :owner_drained, name: registry)
    assert_receive {:proxy_cancelled, ^pid, :owner_drained}
    assert_receive {:websocket_response_activity, ^pid, ^token}

    assert_receive {:websocket_response_activity_cancelled, ^pid, ^token, ack_pid, :owner_drained}

    assert :ok = ResponseTask.acknowledge_delivery(ack_pid, token)
    assert_receive {:codex_response_done, ^pid, {:error, :owner_drained}}
    assert_receive {:DOWN, ^monitor, :process, ^pid, :killed}
    assert :dead = ExecutionRegistry.status(execution.owner_execution_id, pid)
    [proof] = Enum.filter(ExecutionRegistry.pending(10_000), &(&1.owner_execution_id == execution.owner_execution_id))
    assert proof.interruption_code == "owner_drained"
    assert {:finished, :aborted} = ActivityRegistry.status(token, name: registry)
    refute_received {:proxy_cancelled, ^pid, :owner_drained}
    refute_received {:codex_response_done, ^pid, {:error, :owner_drained}}
  end

  for delivery <- [:aborted, :completed] do
    @tag delivery: delivery
    test "cancellation admitted before watcher handoff settles #{delivery} without dispatch", %{
      registry: registry,
      delivery: delivery
    } do
      parent = self()

      {:ok, pid} =
        ResponseTask.start(
          parent,
          :proxy,
          fn _task_pid ->
            send(parent, :forbidden_gap_dispatch)
            :ok
          end,
          fn task_pid, reason -> send(parent, {:gap_cancelled, task_pid, reason}) end,
          activity_registry: registry,
          before_cancel_recipient_handoff: fn token ->
            send(parent, {:before_cancel_recipient_handoff, self(), token})

            receive do
              :release_cancel_recipient_handoff -> :ok
            end
          end
        )

      on_exit(fn -> if Process.alive?(pid), do: Process.exit(pid, :kill) end)
      monitor = Process.monitor(pid)
      assert_receive {:before_cancel_recipient_handoff, ^pid, token}
      assert {_epoch, [%{token: ^token, pid: ^pid}]} = ActivityRegistry.begin_drain(name: registry)
      assert :ok = ActivityRegistry.cancel(token, :owner_drained, name: registry)
      assert {:active, :cancelling} = ActivityRegistry.status(token, name: registry)
      send(pid, :release_cancel_recipient_handoff)

      refute_received :forbidden_gap_dispatch
      assert_receive {:gap_cancelled, ^pid, :owner_drained}
      assert_receive {:websocket_response_activity, ^pid, ^token}

      assert_receive {:websocket_response_activity_cancelled, ^pid, ^token, ack_pid, :owner_drained}

      assert :ok = ResponseTask.acknowledge_delivery(ack_pid, token, delivery)
      if delivery == :aborted, do: assert_receive({:codex_response_done, ^pid, {:error, :owner_drained}})
      assert_receive {:DOWN, ^monitor, :process, ^pid, :normal}
      assert {:finished, ^delivery} = ActivityRegistry.status(token, name: registry)
      refute_received :forbidden_gap_dispatch
      refute_received {:codex_response_done, ^pid, {:error, :owner_drained}}
    end
  end

  test "cancellation accepted before completion handoff cannot be lost to the old watcher", %{
    registry: registry
  } do
    parent = self()
    termination_ref = make_ref()

    {:ok, pid} =
      ResponseTask.start(
        parent,
        :proxy,
        fn _task_pid -> :ok end,
        fn task_pid, reason -> send(parent, {:handoff_cancelled, task_pid, reason}) end,
        activity_registry: registry,
        before_completion_handoff: fn token, watcher ->
          send(parent, {:before_completion_handoff, self(), token, watcher})

          receive do
            :release_completion_handoff -> :ok
          end
        end,
        before_cancelled_coordinator_termination: fn token, coordinator ->
          send(parent, {:before_cancelled_coordinator_termination, self(), token, coordinator})

          receive do
            {:release_cancelled_coordinator_termination, ^termination_ref} -> :ok
          end
        end
      )

    monitor = Process.monitor(pid)
    assert_receive {:before_completion_handoff, ^pid, token, watcher}
    watcher_monitor = Process.monitor(watcher)
    assert {_epoch, [%{token: ^token, pid: ^pid}]} = ActivityRegistry.begin_drain(name: registry)
    assert :ok = ActivityRegistry.cancel(token, :owner_drained, name: registry)
    assert_receive {:handoff_cancelled, ^pid, :owner_drained}
    send(pid, :release_completion_handoff)

    assert_receive {:websocket_response_activity, ^pid, ^token}

    assert_receive {:websocket_response_activity_cancelled, ^pid, ^token, ^watcher, :owner_drained}

    assert :ok = ResponseTask.acknowledge_delivery(watcher, token)
    assert_receive {:codex_response_done, ^pid, {:error, :owner_drained}}
    assert_receive {:before_cancelled_coordinator_termination, ^watcher, ^token, ^pid}
    assert Process.alive?(pid)
    refute_received {:DOWN, ^monitor, :process, ^pid, _reason}

    send(watcher, {:release_cancelled_coordinator_termination, termination_ref})

    assert_receive {:DOWN, ^watcher_monitor, :process, ^watcher, :normal}
    assert_receive {:DOWN, ^monitor, :process, ^pid, :killed}
    assert {:finished, :aborted} = ActivityRegistry.status(token, name: registry)
    refute_received {:codex_response_done, ^pid, :ok}
    refute_received {:handoff_cancelled, ^pid, :owner_drained}
    assert ActivityRegistry.activities(name: registry) == []
  end

  test "cancellation ownership survives activity removal while its original executor is held", %{registry: registry} do
    %{pid: pid, monitor: monitor, token: token, watcher: watcher, watcher_monitor: watcher_monitor, execution: execution} = held_completion!(registry)
    cancel_and_hold_watcher!(registry, pid, token, watcher)
    assert ActivityRegistry.status(token, name: registry) == :unknown
    send(pid, {:release_held_completion, token})
    await_cancellation_wait!(pid)
    assert ExecutionRegistry.status(execution.owner_execution_id, pid) == :alive
    refute Enum.any?(ExecutionRegistry.pending(10_000), &(&1.owner_execution_id == execution.owner_execution_id))
    send(watcher, {:release_held_watcher, token})
    assert_receive {:DOWN, ^monitor, :process, ^pid, :killed}, @shutdown_detection_ms
    assert_receive {:DOWN, ^watcher_monitor, :process, ^watcher, :normal}, @shutdown_detection_ms
    assert :dead = ExecutionRegistry.status(execution.owner_execution_id, pid)
    [proof] = Enum.filter(ExecutionRegistry.pending(10_000), &(&1.owner_execution_id == execution.owner_execution_id))
    assert %{end_kind: "process_down", interruption_code: "owner_drained"} = proof
    refute_received {:codex_response_done, ^pid, :ok}
    refute_received {:codex_response_done, ^pid, {:error, :owner_drained}}
    assert ActivityRegistry.activities(name: registry) == []
  end

  test "completed cancellation settlement precedes watcher DOWN after activity removal", %{registry: registry} do
    %{pid: pid, monitor: monitor, token: token, watcher: watcher, watcher_monitor: watcher_monitor} = held_completion!(registry)
    assert {_epoch, [_entry]} = ActivityRegistry.begin_drain(name: registry)
    assert :ok = ActivityRegistry.cancel(token, :owner_drained, name: registry)
    assert_receive {:held_cancel_callback, ^watcher, ^pid}, @shutdown_detection_ms
    assert_receive {:websocket_response_activity_cancelled, ^pid, ^token, ^watcher, :owner_drained}, @shutdown_detection_ms
    ResponseTask.acknowledge_delivery(watcher, token, :completed)
    assert_receive {:DOWN, ^watcher_monitor, :process, ^watcher, :normal}, @shutdown_detection_ms
    assert {:finished, :completed} = ActivityRegistry.status(token, name: registry)
    assert Process.alive?(pid)
    send(pid, {:release_held_completion, token})
    assert_receive {:DOWN, ^monitor, :process, ^pid, :normal}, @shutdown_detection_ms
    refute_received {:codex_response_done, ^pid, :ok}
    refute_received {:codex_response_done, ^pid, {:error, :owner_drained}}
    assert ActivityRegistry.activities(name: registry) == []
  end

  @tag capture_log: true
  test "another watcher's cancellation cannot authorize a missing activity", %{registry: registry} do
    foreign = held_completion!(registry)
    own = held_completion!(registry)
    cancel_and_hold_watcher!(registry, foreign.pid, foreign.token, foreign.watcher)
    ActivityRegistry.unregister(own.token, :failed, name: registry)
    send(own.pid, {:release_held_completion, own.token})
    assert_receive {:DOWN, monitor, :process, pid, {%CaseClauseError{term: {:error, :unknown_activity}}, _stack}}, @shutdown_detection_ms
    assert monitor == own.monitor and pid == own.pid
    assert_receive {:DOWN, watcher_monitor, :process, watcher, :normal}, @shutdown_detection_ms
    assert watcher_monitor == own.watcher_monitor and watcher == own.watcher
    send(foreign.pid, {:release_held_completion, foreign.token})
    await_cancellation_wait!(foreign.pid)
    send(foreign.watcher, {:release_held_watcher, foreign.token})
    assert_receive {:DOWN, monitor, :process, pid, :killed}, @shutdown_detection_ms
    assert monitor == foreign.monitor and pid == foreign.pid
    assert_receive {:DOWN, watcher_monitor, :process, watcher, :normal}, @shutdown_detection_ms
    assert watcher_monitor == foreign.watcher_monitor and watcher == foreign.watcher
    refute_received {:codex_response_done, _, :ok}
    assert ActivityRegistry.activities(name: registry) == []
  end

  for handoff <- [:before_watcher_death, :after_watcher_death] do
    @tag capture_log: true, handoff: handoff
    test "removed activity fails when its watcher dies #{handoff}", %{registry: registry, handoff: handoff} do
      %{pid: pid, monitor: monitor, token: token, watcher: watcher, watcher_monitor: watcher_monitor} = held_completion!(registry)
      cancel_and_hold_watcher!(registry, pid, token, watcher)

      if handoff == :before_watcher_death do
        send(pid, {:release_held_completion, token})
        await_cancellation_wait!(pid)
      end

      Process.exit(watcher, :kill)
      assert_receive {:DOWN, ^watcher_monitor, :process, ^watcher, :killed}, @shutdown_detection_ms
      if handoff == :after_watcher_death, do: send(pid, {:release_held_completion, token})
      assert_receive {:DOWN, ^monitor, :process, ^pid, :websocket_response_cancellation_watcher_down}, @shutdown_detection_ms
      refute_received {:codex_response_done, ^pid, :ok}
      assert ActivityRegistry.activities(name: registry) == []
    end
  end

  @tag capture_log: true
  test "registry loss during watcher completion cannot strand a recovered missing activity", %{registry: registry} do
    %{pid: pid, monitor: monitor, token: token, watcher: watcher, watcher_monitor: watcher_monitor} = held_completion!(registry)
    assert :ok = ActivityRegistry.cancel(token, :owner_drained, name: registry)
    assert_receive {:held_cancel_callback, ^watcher, ^pid}, @shutdown_detection_ms
    assert_receive {:websocket_response_activity_cancelled, ^pid, ^token, ^watcher, :owner_drained}, @shutdown_detection_ms
    registry_pid = Process.whereis(registry)
    :ok = :sys.suspend(registry_pid)

    on_exit(fn ->
      if Process.alive?(registry_pid), do: :sys.resume(registry_pid)
    end)

    ResponseTask.acknowledge_delivery(watcher, token, :aborted)
    await_registry_completion!(registry_pid, watcher, token, System.monotonic_time(:millisecond) + @shutdown_detection_ms)
    Process.exit(registry_pid, :kill)
    assert_receive {:DOWN, ^watcher_monitor, :process, ^watcher, _reason}, @shutdown_detection_ms
    await_registry_restart!(registry, registry_pid, System.monotonic_time(:millisecond) + @shutdown_detection_ms)
    assert ActivityRegistry.status(token, name: registry) == :unknown
    send(pid, {:release_held_completion, token})
    assert_receive {:DOWN, ^monitor, :process, ^pid, :websocket_response_cancellation_watcher_down}, @shutdown_detection_ms
    refute_received {:codex_response_done, ^pid, :ok}
    assert ActivityRegistry.activities(name: registry) == []
  end

  test "natural delivery ack wins after drain cancellation is marked but before it settles", %{
    registry: registry
  } do
    parent = self()
    completion_ref = make_ref()

    {:ok, pid} =
      ResponseTask.start(
        parent,
        :proxy,
        fn _task_pid -> :ok end,
        fn task_pid, reason ->
          send(parent, {:natural_winner_cancel_started, task_pid, reason})
        end,
        activity_registry: registry,
        before_completion_handoff: fn token, watcher ->
          send(parent, {:natural_winner_completion_ready, self(), token, watcher})

          receive do
            {:release_natural_winner_completion, ^completion_ref} -> :ok
          end
        end
      )

    monitor = Process.monitor(pid)
    assert_receive {:natural_winner_completion_ready, ^pid, token, watcher}

    # The coordinator is parked inside `before_completion_handoff` until the
    # release below, so nothing has yet sent the watcher its stop message, and
    # its other three receive clauses are fenced on this turn's own token and
    # on its own monitor of a coordinator that is still alive. The watcher is
    # therefore alive here, and the release below is what lets the coordinator
    # stop it. `Process.monitor/1` only queues its request until this process
    # is scheduled out, and the release goes to another process: a run that
    # stalled right after the monitor saw the watcher's DOWN as `:noproc` five
    # assertions later, although the watcher had been alive when it was
    # monitored (findings#221 row 221-75, explained by findings#303 row
    # 303-8). `monitor_flushed/1` sends the request before the release.
    assert Process.alive?(watcher),
           "the cancellation watcher died before the completion handoff was released"

    watcher_monitor = TestProcess.monitor_flushed(watcher)
    send(pid, {:release_natural_winner_completion, completion_ref})
    assert_receive {:websocket_response_activity, ^pid, ^token}
    assert_receive {:codex_response_done, ^pid, :ok}
    assert {_epoch, [%{token: ^token, pid: ^pid}]} = ActivityRegistry.begin_drain(name: registry)
    assert :ok = ActivityRegistry.cancel(token, :owner_drained, name: registry)
    assert {:active, :cancelling} = ActivityRegistry.status(token, name: registry)
    assert_receive {:natural_winner_cancel_started, ^pid, :owner_drained}
    assert_receive {:websocket_response_activity_cancelled, ^pid, ^token, :owner_drained}

    send(pid, {:websocket_response_delivery_ack, token, :completed})

    assert_receive {:DOWN, ^monitor, :process, ^pid, :normal}
    assert_receive {:DOWN, ^watcher_monitor, :process, ^watcher, :normal}
    assert {:finished, :completed} = ActivityRegistry.status(token, name: registry)
    refute_received {:codex_response_done, ^pid, {:error, :owner_drained}}
    assert ActivityRegistry.activities(name: registry) == []
  end

  for kind <- [:direct, :proxy] do
    test "tracked #{kind} delivery wait settles aborted and exits when its socket dies without acknowledging",
         %{registry: registry} do
      test_pid = self()
      socket = spawn(fn -> forward_socket_messages(test_pid) end)

      {:ok, pid} =
        ResponseTask.start(
          socket,
          unquote(kind),
          fn _task_pid -> :ok end,
          fn _task_pid, _reason -> :ok end,
          activity_registry: registry
        )

      on_exit(fn -> if Process.alive?(pid), do: Process.exit(pid, :kill) end)
      monitor = Process.monitor(pid)
      assert_receive {:socket_received, {:websocket_response_activity, ^pid, token}}
      assert_receive {:socket_received, {:codex_response_done, ^pid, :ok}}

      assert {_epoch, [%{token: ^token, pid: ^pid}]} =
               ActivityRegistry.begin_drain(name: registry)

      assert {:active, :admitted} = ActivityRegistry.status(token, name: registry)

      Process.exit(socket, :kill)

      assert_receive {:DOWN, ^monitor, :process, ^pid, :normal}, @shutdown_detection_ms
      assert {:finished, :aborted} = ActivityRegistry.status(token, name: registry)
      assert ActivityRegistry.activities(name: registry) == []
    end
  end

  test "a delivery acknowledgement sent before the socket dies still settles completed", %{
    registry: registry
  } do
    test_pid = self()

    socket =
      spawn(fn ->
        receive do
          {:websocket_response_activity, task_pid, token} ->
            send(test_pid, {:socket_saw_activity, task_pid, token})

            receive do
              :acknowledge_and_die ->
                ResponseTask.acknowledge_delivery(task_pid, token, :completed)
                exit(:kill_after_ack)
            end
        end
      end)

    {:ok, pid} =
      ResponseTask.start(
        socket,
        :direct,
        fn _task_pid -> :ok end,
        fn _task_pid, _reason -> :ok end,
        activity_registry: registry
      )

    monitor = Process.monitor(pid)
    assert_receive {:socket_saw_activity, ^pid, token}
    assert {_epoch, [%{token: ^token}]} = ActivityRegistry.begin_drain(name: registry)
    send(socket, :acknowledge_and_die)

    assert_receive {:DOWN, ^monitor, :process, ^pid, :normal}, @shutdown_detection_ms
    assert {:finished, :completed} = ActivityRegistry.status(token, name: registry)
    assert ActivityRegistry.activities(name: registry) == []
  end

  test "a cancellation watcher waiting for delivery settles aborted when its socket dies", %{
    registry: registry
  } do
    test_pid = self()
    socket = spawn(fn -> forward_socket_messages(test_pid) end)

    {:ok, pid} =
      ResponseTask.start(
        socket,
        :proxy,
        fn _task_pid ->
          send(test_pid, :proxy_upstream_started)

          receive do
            :release_proxy_work -> :ok
          end
        end,
        fn task_pid, reason -> send(test_pid, {:proxy_cancelled, task_pid, reason}) end,
        activity_registry: registry
      )

    on_exit(fn -> if Process.alive?(pid), do: Process.exit(pid, :kill) end)
    assert_receive :proxy_upstream_started
    assert {_epoch, [%{token: token, pid: ^pid}]} = ActivityRegistry.begin_drain(name: registry)
    monitor = Process.monitor(pid)
    assert :ok = ActivityRegistry.cancel(token, :owner_drained, name: registry)
    assert_receive {:proxy_cancelled, ^pid, :owner_drained}

    assert_receive {:socket_received, {:websocket_response_activity_cancelled, ^pid, ^token, watcher, :owner_drained}}

    on_exit(fn -> if Process.alive?(watcher), do: Process.exit(watcher, :kill) end)
    watcher_monitor = TestProcess.monitor_flushed(watcher)

    Process.exit(socket, :kill)

    assert_receive {:DOWN, ^watcher_monitor, :process, ^watcher, :normal},
                   @shutdown_detection_ms

    assert_receive {:DOWN, ^monitor, :process, ^pid, :killed}, @shutdown_detection_ms
    assert {:finished, :aborted} = ActivityRegistry.status(token, name: registry)
    assert ActivityRegistry.activities(name: registry) == []
    refute_received {:proxy_cancelled, ^pid, :owner_drained}
  end

  test "a delivered local-owner result completes its execution like the direct kind", %{
    registry: registry
  } do
    # With owner forwarding on, a fresh session's task is `:local_owner`, the
    # kind production selects; its delivery ack must retire the execution as
    # `completed` too, not leave it to the process exit (findings#217).
    parent = self()

    {:ok, pid} =
      ResponseTask.start(
        parent,
        :local_owner,
        fn _task_pid ->
          send(parent, {:execution, ExecutionIdentity.local()})
          {:socket_response_result, :owner_completion_pending, :ok}
        end,
        fn _task_pid, _reason -> :ok end,
        activity_registry: registry
      )

    assert_receive {:execution, %{owner_execution_id: execution_id}}
    monitor = Process.monitor(pid)
    assert_receive {:websocket_response_activity, ^pid, token}

    assert_receive {:codex_response_done, ^pid, {:socket_response_result, :owner_completion_pending, :ok}}

    assert :ok = ResponseTask.acknowledge_delivery(pid, token, :completed)
    assert_receive {:DOWN, ^monitor, :process, ^pid, :normal}

    proof =
      ExecutionRegistry.pending(10_000)
      |> Enum.find(&(&1.owner_execution_id == execution_id))

    assert %{end_kind: "completed"} = proof
  end

  test "untracked local-owner submitted work waits for delivery without double-counting", %{
    registry: registry
  } do
    parent = self()

    {:ok, pid} =
      ResponseTask.start(
        parent,
        :local_owner,
        fn _task_pid -> {:socket_response_result, :owner_completion_pending, :ok} end,
        fn _task_pid, _reason -> :ok end,
        activity_registry: registry
      )

    monitor = Process.monitor(pid)
    assert_receive {:websocket_response_activity, ^pid, token}

    assert_receive {:codex_response_done, ^pid, {:socket_response_result, :owner_completion_pending, :ok}}

    assert Process.alive?(pid)
    assert {_epoch, []} = ActivityRegistry.begin_drain(name: registry)
    assert :ok = ResponseTask.acknowledge_delivery(pid, token, :completed)
    assert_receive {:DOWN, ^monitor, :process, ^pid, :normal}
  end

  test "untracked local-owner submitted work exits when its socket dies without acknowledging", %{
    registry: registry
  } do
    test_pid = self()

    socket =
      spawn(fn ->
        receive do
          {:websocket_response_activity, task_pid, token} ->
            send(test_pid, {:socket_saw_activity, task_pid, token})
        end

        receive do
          :never_acknowledge -> :ok
        end
      end)

    {:ok, pid} =
      ResponseTask.start(
        socket,
        :local_owner,
        fn _task_pid -> {:socket_response_result, :owner_completion_pending, :ok} end,
        fn _task_pid, _reason -> :ok end,
        activity_registry: registry
      )

    monitor = Process.monitor(pid)
    assert_receive {:socket_saw_activity, ^pid, _token}
    assert Process.alive?(pid)

    Process.exit(socket, :kill)
    assert_receive {:DOWN, ^monitor, :process, ^pid, :normal}
    assert {_epoch, []} = ActivityRegistry.begin_drain(name: registry)
  end

  test "untracked local-owner local completion exits without delivery acknowledgement", %{
    registry: registry
  } do
    parent = self()

    {:ok, pid} =
      ResponseTask.start(
        parent,
        :local_owner,
        fn _task_pid -> {:socket_response_result, :local_complete, :ok} end,
        fn _task_pid, _reason -> :ok end,
        activity_registry: registry
      )

    monitor = Process.monitor(pid)
    assert_receive {:codex_response_done, ^pid, {:socket_response_result, :local_complete, :ok}}
    refute_receive {:websocket_response_activity, ^pid, _token}, 0
    assert_receive {:DOWN, ^monitor, :process, ^pid, :normal}
    assert {_epoch, []} = ActivityRegistry.begin_drain(name: registry)
  end

  test "untracked local-owner failure exits without waiting for terminal delivery", %{
    registry: registry
  } do
    parent = self()

    {:ok, pid} =
      ResponseTask.start(
        parent,
        :local_owner,
        fn _task_pid ->
          {:socket_response_result, :owner_completion_pending, {:error, :client_disconnected}}
        end,
        fn _task_pid, _reason -> :ok end,
        activity_registry: registry
      )

    monitor = Process.monitor(pid)

    assert_receive {:codex_response_done, ^pid, {:socket_response_result, :owner_completion_pending, {:error, :client_disconnected}}}

    refute_receive {:websocket_response_activity, ^pid, _token}, 0
    assert_receive {:DOWN, ^monitor, :process, ^pid, :normal}
    assert {_epoch, []} = ActivityRegistry.begin_drain(name: registry)
  end

  defp held_completion!(registry) do
    parent = self()

    {:ok, pid} =
      ResponseTask.start(
        parent,
        :proxy,
        fn task ->
          send(parent, {:held_execution, task, ExecutionIdentity.local()})
          :ok
        end,
        fn task, :owner_drained -> send(parent, {:held_cancel_callback, self(), task}) end,
        activity_registry: registry,
        before_completion_handoff: fn token, watcher ->
          send(parent, {:held_completion, self(), token, watcher})

          receive do
            {:release_held_completion, ^token} -> :ok
          after
            @shutdown_detection_ms -> flunk("held completion was not released")
          end
        end,
        before_cancelled_coordinator_termination: fn token, task ->
          send(parent, {:held_watcher, self(), token, task})

          receive do
            {:release_held_watcher, ^token} -> :ok
          after
            @shutdown_detection_ms -> flunk("held watcher was not released")
          end
        end
      )

    on_exit(fn -> if Process.alive?(pid), do: Process.exit(pid, :kill) end)
    monitor = TestProcess.monitor_flushed(pid)
    assert_receive {:held_execution, ^pid, execution}, @shutdown_detection_ms
    assert_receive {:held_completion, ^pid, token, watcher}, @shutdown_detection_ms
    on_exit(fn -> if Process.alive?(watcher), do: Process.exit(watcher, :kill) end)
    %{pid: pid, monitor: monitor, token: token, watcher: watcher, watcher_monitor: TestProcess.monitor_flushed(watcher), execution: execution}
  end

  defp cancel_and_hold_watcher!(registry, pid, token, watcher) do
    assert :ok = ActivityRegistry.cancel(token, :owner_drained, name: registry)
    assert_receive {:held_cancel_callback, ^watcher, ^pid}, @shutdown_detection_ms
    assert_receive {:websocket_response_activity_cancelled, ^pid, ^token, ^watcher, :owner_drained}, @shutdown_detection_ms
    ResponseTask.acknowledge_delivery(watcher, token, :aborted)
    assert_receive {:codex_response_done, ^pid, {:error, :owner_drained}}, @shutdown_detection_ms
    assert_receive {:held_watcher, ^watcher, ^token, ^pid}, @shutdown_detection_ms
  end

  defp await_cancellation_wait!(pid), do: await_cancellation_wait!(pid, System.monotonic_time(:millisecond) + @shutdown_detection_ms)

  defp await_cancellation_wait!(pid, deadline) do
    case Process.info(pid, :current_function) do
      {:current_function, {ResponseTask, :await_cancellation_settled, 3}} ->
        :ok

      observed ->
        assert System.monotonic_time(:millisecond) < deadline, "coordinator did not reach cancellation wait: #{inspect(observed)}"
        Process.sleep(1)
        await_cancellation_wait!(pid, deadline)
    end
  end

  defp await_registry_completion!(registry, watcher, token, deadline) do
    {:messages, messages} = Process.info(registry, :messages)

    if Enum.any?(messages, &match?({:"$gen_call", {^watcher, _}, {:complete, ^token, :aborted}}, &1)) do
      :ok
    else
      assert System.monotonic_time(:millisecond) < deadline, "watcher did not request activity completion"
      Process.sleep(1)
      await_registry_completion!(registry, watcher, token, deadline)
    end
  end

  defp await_registry_restart!(registry, previous, deadline) do
    case Process.whereis(registry) do
      pid when is_pid(pid) and pid != previous ->
        :ok

      _ ->
        assert System.monotonic_time(:millisecond) < deadline, "activity registry did not restart"
        Process.sleep(1)
        await_registry_restart!(registry, previous, deadline)
    end
  end

  defp forward_socket_messages(test_pid) do
    receive do
      message ->
        send(test_pid, {:socket_received, message})
        forward_socket_messages(test_pid)
    end
  end
end
