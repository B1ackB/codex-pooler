defmodule CodexPoolerWeb.Runtime.BackendCodexWebsocketOwnerForwarding.TerminateDeliveryReceiptTest do
  # A socket that closes while its response task is still running acknowledges
  # the task during terminate and then drains it. The task consumes the first
  # acknowledgement it receives, and the socket pushed at most one terminal, so
  # the turn gets exactly one delivery receipt: the drain must not acknowledge
  # and record the same task a second time (findings#225, row 225-100, where
  # production logged `aborted` then `delivered` for one request).
  use CodexPoolerWeb.ConnCase, async: false

  import Ecto.Query, only: [from: 2]

  @moduletag capture_log: true

  import CodexPoolerWeb.Runtime.BackendCodexTestSupport
  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport
  import CodexPoolerWeb.Runtime.BackendCodexWebsocketOwnerForwardingSupport

  alias CodexPooler.Access
  alias CodexPooler.Accounting.{Attempt, LedgerEntry, Request}
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway.Transports.Websocket.{ActivityRegistry, WebsocketOwnerSession}
  alias CodexPooler.Repo
  alias CodexPoolerWeb.CodexResponsesSocket
  alias CodexPoolerWeb.Runtime.WebsocketCleanupFence
  alias Ecto.Adapters.SQL.Sandbox

  setup do
    CodexPooler.TestAppEnv.restore_on_exit(:websocket_owner_forwarding_enabled)
    Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, true)
  end

  @tag slow: "terminates a real owner-forwarded socket under a running turn and waits for its owner-side task to settle (0.7 s alone, 1.04 s under partition load)"
  test "a socket closing under a running turn records one delivery receipt for it" do
    release_ref = make_ref()
    upstream_boundary = blocking_owner_upstream_boundary(self(), release_ref)
    upstream = start_upstream(FakeUpstream.json_response(%{"unexpected" => true}))
    setup = gateway_setup(upstream)
    {:ok, auth} = Access.authenticate_authorization_header(setup.authorization)

    {:ok, state} =
      owner_socket(auth, "ws-owner-terminate-receipt", "ws-owner-terminate-receipt", websocket_owner_forwarder_opts: [upstream: upstream_boundary])

    payload = turn_payload(setup, "ws-owner-terminate-receipt-turn", "a turn still running at close")
    assert {:ok, state} = CodexResponsesSocket.handle_in({payload, [opcode: :text]}, state)
    {state, _worker} = assert_blocking_owner_upstream_received!(state, release_ref)

    {:ok, owner_pid} = WebsocketOwnerSession.lookup(state.codex_session.id)
    Sandbox.allow(Repo, self(), owner_pid)

    {result, log} = with_info_log(fn -> CodexResponsesSocket.terminate(:closed, state) end)
    assert result == :ok

    [request] = request_logs(setup.pool.id)
    receipts = Regex.scan(~r/websocket downstream terminal pushed request_id=#{request.id} [^\n]*/, log)

    assert length(receipts) == 1, "expected one delivery receipt, got #{length(receipts)}"
    assert [[receipt]] = receipts
    assert receipt =~ "outcome=aborted"

    [attempt] = Repo.all(from(a in Attempt, where: a.request_id == ^request.id))
    assert attempt.response_metadata["downstream_delivery"]["outcome"] == "aborted"
    assert attempt.status == "retryable_failed"
    refute Map.has_key?(attempt.response_metadata, "delivery_failure")
    assert Repo.get!(Request, request.id).status == "in_progress"
    assert Repo.get_by!(CodexPooler.Gateway.Persistence.CodexTurn, request_id: request.id).status == "in_progress"
    assert %{status: "armed", eligible_attempt_id: eligible_id, replay_generation: 1, replay_attempt_id: nil, closed_at: nil} = Repo.get_by!(CodexPooler.Accounting.RequestReplayEntitlement, request_id: request.id)
    assert eligible_id == attempt.id
    assert Repo.all(from entry in LedgerEntry, where: entry.request_id == ^request.id, select: entry.entry_kind) == ["reservation"]

    await_owner_cleanup!(state.codex_session.id)
  end

  @detection_timeout_ms 15_000

  for mode <- ["full", "lite"] do
    @tag mode: mode
    test "#{mode}: a physical cut records the completed items of an untracked local-owner error task", %{mode: mode} do
      scenario = real_receipt_scenario!(mode)
      socket = WebsocketCleanupFence.await_new_listener_socket!(scenario.sockets_before)
      gate = install_local_completion_gate!(socket)
      {conn, websocket} = public_websocket_send_text!(scenario.conn, scenario.websocket, scenario.ref, scenario.payload)
      {conn, _websocket} = read_completed_items!(conn, websocket, scenario.ref, scenario.upstream, scenario.release_ref, 2)
      state = listener_socket_state(socket)
      [task] = MapSet.to_list(state.tasks)
      %{request_id: request_id, attempt_id: attempt_id} = state.direct_cleanup_receipts[task]
      refute Map.has_key?(Map.get(state, :response_task_activities, %{}), task)
      assert state.downstream_delivery_evidence[task].completed_items == 2
      assert length(state.downstream_delivery_evidence[task].completed_item_digests) == 2
      task_monitor = Process.monitor(task)
      socket_monitor = Process.monitor(socket)
      before_reconnect = WebsocketCleanupFence.listener_sockets()
      {reconnect_conn, _websocket, _ref} = public_websocket_connect!(scenario.port, scenario.setup, scenario.turn_state)
      reconnect_socket = WebsocketCleanupFence.await_new_listener_socket!(before_reconnect)
      reconnect_state = listener_socket_state(reconnect_socket)
      assert reconnect_state.codex_session.id == state.codex_session.id
      owner_state = :sys.get_state(state.websocket_owner_pid)
      generation_pid = owner_state.active_turn.task_pid
      generation_monitor = Process.monitor(generation_pid)
      assert {:ok, _scope} = WebsocketOwnerSession.take_over_inherited_turn(state.websocket_owner_pid, reconnect_state.websocket_owner_downstream)
      assert_receive {:DOWN, ^generation_monitor, :process, ^generation_pid, _reason}, @detection_timeout_ms
      assert_receive {:local_completion_held, ^task, gate_ref}, @detection_timeout_ms
      assert gate_ref == gate.ref
      assert ActivityRegistry.delivery_target(task) == :unknown

      {:ok, log} =
        with_info_log(fn ->
          {:ok, _closed} = Mint.HTTP.close(conn)
          assert_receive {:local_socket_cleanup_finished, ^socket}, @detection_timeout_ms
          await_post_cleanup_drain!(socket, System.monotonic_time(:millisecond) + @detection_timeout_ms)
          send(task, {:release_local_completion, gate.ref})
          WebsocketCleanupFence.await_listener_socket_cleanup!(socket)
          assert_receive {:DOWN, ^socket_monitor, :process, ^socket, _reason}, @detection_timeout_ms
          assert_receive {:DOWN, ^task_monitor, :process, ^task, _reason}, @detection_timeout_ms
          :ok
        end)

      {:ok, _closed} = Mint.HTTP.close(reconnect_conn)
      WebsocketCleanupFence.await_listener_socket_cleanup!(reconnect_socket)

      request = Repo.get!(Request, request_id)
      attempt = Repo.get!(Attempt, attempt_id)
      assert request.request_metadata["routing"]["model_serving_mode"] == mode
      assert request.status == "failed"
      assert request.last_error_code == "client_disconnected"
      assert attempt.status == "failed"
      assert attempt.network_error_code == "client_disconnected"
      receipt = attempt.response_metadata["downstream_delivery"]
      assert is_map(receipt), "the settled final Attempt lost its actual socket output receipt"
      assert receipt["outcome"] == "aborted"
      assert receipt["terminal_class"] == "none"
      assert receipt["highest_frame_class"] == "item_done"
      assert receipt["completed_items"] == 2
      assert length(receipt["completed_item_digests"]) == 2
      assert length(Regex.scan(~r/websocket downstream terminal pushed request_id=#{request_id} [^\n]*/, log)) == 1
      assert_exact_ledger!(request_id)
      assert FakeUpstream.count(scenario.upstream) == 1
    end
  end

  test "a late local-owner success handoff records its bound delivered receipt exactly once" do
    scenario = real_receipt_scenario!()
    socket = WebsocketCleanupFence.await_new_listener_socket!(scenario.sockets_before)
    gate = install_local_completion_gate!(socket)
    {conn, websocket} = public_websocket_send_text!(scenario.conn, scenario.websocket, scenario.ref, scenario.payload)
    {conn, websocket} = read_completed_items!(conn, websocket, scenario.ref, scenario.upstream, scenario.release_ref, 2)
    assert_receive {:fake_upstream_frame_barrier, 3, _handler, release_ref}, @detection_timeout_ms
    assert release_ref == scenario.release_ref
    assert :ok = FakeUpstream.release_frame(scenario.upstream, scenario.release_ref)
    {conn, _websocket} = receive_response_terminal!(conn, websocket, scenario.ref)
    assert_receive {:local_completion_held, task, gate_ref}, @detection_timeout_ms
    assert gate_ref == gate.ref
    state = listener_socket_state(socket)
    %{request_id: request_id, attempt_id: attempt_id} = state.direct_cleanup_receipts[task]
    refute Map.has_key?(Map.get(state, :response_task_activities, %{}), task)
    task_monitor = Process.monitor(task)
    socket_monitor = Process.monitor(socket)

    {:ok, log} =
      with_info_log(fn ->
        {:ok, _closed} = Mint.HTTP.close(conn)
        assert_receive {:local_socket_cleanup_finished, ^socket}, @detection_timeout_ms
        await_post_cleanup_drain!(socket, System.monotonic_time(:millisecond) + @detection_timeout_ms)
        send(task, {:release_local_completion, gate.ref})
        assert_receive {:DOWN, ^task_monitor, :process, ^task, :normal}, @detection_timeout_ms
        WebsocketCleanupFence.await_listener_socket_cleanup!(socket)
        assert_receive {:DOWN, ^socket_monitor, :process, ^socket, _reason}, @detection_timeout_ms
        :ok
      end)

    attempt = Repo.get!(Attempt, attempt_id)
    assert attempt.status == "succeeded"
    assert %{"outcome" => "delivered", "terminal_class" => "response.completed", "completed_items" => 2} = receipt = attempt.response_metadata["downstream_delivery"]
    assert length(receipt["completed_item_digests"]) == 2
    assert length(Regex.scan(~r/websocket downstream terminal pushed request_id=#{request_id} [^\n]*/, log)) == 1
    assert_exact_ledger!(request_id)
    assert FakeUpstream.count(scenario.upstream) == 1
  end

  defp real_receipt_scenario!(mode \\ "full") do
    scope = model_serving_scope()
    release_ref = make_ref()
    upstream = start_upstream(FakeUpstream.barrier_websocket_frames(receipt_events(), notify: self(), release_ref: release_ref))
    setup = gateway_setup(upstream)
    set_model_serving_mode!(scope, setup, mode)
    port = start_public_endpoint!()
    sockets_before = WebsocketCleanupFence.listener_sockets()
    turn_state = "receipt-cut-#{System.unique_integer([:positive])}"
    {conn, websocket, ref} = public_websocket_connect!(port, setup, turn_state)
    payload = turn_payload(setup, "receipt-cut-turn-#{System.unique_integer([:positive])}", "synthetic receipt turn")
    %{conn: conn, websocket: websocket, ref: ref, upstream: upstream, setup: setup, payload: payload, release_ref: release_ref, sockets_before: sockets_before, port: port, turn_state: turn_state}
  end

  defp receipt_events do
    items = for ordinal <- 1..2, do: %{"type" => "reasoning", "id" => "rs_receipt_#{ordinal}", "summary" => [], "encrypted_content" => "synthetic_receipt_#{ordinal}"}

    ([%{"type" => "response.created", "response" => %{"id" => "resp_receipt_cut", "status" => "in_progress", "output" => []}}] ++ Enum.map(items, &%{"type" => "response.output_item.done", "item" => &1}) ++ [%{"type" => "response.completed", "response" => %{"id" => "resp_receipt_cut", "status" => "completed", "output" => items, "usage" => %{"input_tokens" => 4, "output_tokens" => 2, "total_tokens" => 6}}}])
    |> Enum.map(&CodexPooler.JSON.encode!/1)
  end

  defp read_completed_items!(conn, websocket, ref, upstream, release_ref, count) do
    for ordinal <- 0..count do
      assert_receive {:fake_upstream_frame_barrier, ^ordinal, _handler, ^release_ref}, @detection_timeout_ms
      assert :ok = FakeUpstream.release_frame(upstream, release_ref)
    end

    receive_item_count!(conn, websocket, ref, count, 0)
  end

  defp receive_item_count!(conn, websocket, _ref, count, count), do: {conn, websocket}

  defp receive_item_count!(conn, websocket, ref, count, seen) do
    message = receive_mint_socket_message!(conn, @detection_timeout_ms, "expected actual completed-item writes")
    assert {:ok, conn, responses} = Mint.WebSocket.stream(conn, message)

    {websocket, frames} =
      Enum.reduce(responses, {websocket, []}, fn
        {:data, ^ref, data}, {websocket, frames} ->
          assert {:ok, websocket, decoded} = Mint.WebSocket.decode(websocket, data)
          {websocket, frames ++ decoded}

        _response, acc ->
          acc
      end)

    added =
      Enum.count(frames, fn
        {:text, data} -> CodexPooler.JSON.decode!(data)["type"] == "response.output_item.done"
        _frame -> false
      end)

    receive_item_count!(conn, websocket, ref, count, seen + added)
  end

  defp receive_response_terminal!(conn, websocket, ref) do
    message = receive_mint_socket_message!(conn, @detection_timeout_ms, "expected written terminal")
    assert {:ok, conn, responses} = Mint.WebSocket.stream(conn, message)

    {websocket, terminal?} =
      Enum.reduce(responses, {websocket, false}, fn
        {:data, ^ref, data}, {websocket, terminal?} ->
          assert {:ok, websocket, frames} = Mint.WebSocket.decode(websocket, data)

          found? =
            Enum.any?(frames, fn
              {:text, data} -> CodexPooler.JSON.decode!(data)["type"] == "response.completed"
              _frame -> false
            end)

          {websocket, terminal? or found?}

        _response, acc ->
          acc
      end)

    if terminal?, do: {conn, websocket}, else: receive_response_terminal!(conn, websocket, ref)
  end

  defp listener_socket_state(socket) do
    {_name, data} = :sys.get_state(socket)
    data.connection.websock_state
  end

  defp install_local_completion_gate!(socket) do
    test = self()
    ref = make_ref()
    handler = {__MODULE__, ref}
    on_exit(fn -> :telemetry.detach(handler) end)
    :ok = :telemetry.attach(handler, [:codex_pooler, :gateway, :websocket_control, :cleanup_finished], &__MODULE__.notify_local_cleanup/4, %{test: test, socket: socket})

    gate = fn ->
      monitor = Process.monitor(test)
      send(test, {:local_completion_held, self(), ref})

      try do
        receive do
          {:release_local_completion, ^ref} -> :ok
          {:DOWN, ^monitor, :process, ^test, _reason} -> :ok
        after
          @detection_timeout_ms -> raise "local completion handoff was not released"
        end
      after
        Process.demonitor(monitor, [:flush])
      end
    end

    :sys.replace_state(socket, fn {name, data} ->
      state = Map.put(data.connection.websock_state, :response_task_start_options, before_local_completion_handoff: gate)
      {name, %{data | connection: %{data.connection | websock_state: state}}}
    end)

    %{ref: ref}
  end

  def notify_local_cleanup(_event, _measurements, %{caller: socket}, %{test: test, socket: socket}), do: send(test, {:local_socket_cleanup_finished, socket})
  def notify_local_cleanup(_event, _measurements, _metadata, _config), do: :ok

  defp await_post_cleanup_drain!(socket, deadline) do
    case Process.info(socket, :current_function) do
      {:current_function, {CodexResponsesSocket, :do_await_response_tasks, 5}} ->
        :ok

      _phase ->
        assert System.monotonic_time(:millisecond) < deadline, "socket did not enter its post-cleanup drain"

        receive do
        after
          1 -> await_post_cleanup_drain!(socket, deadline)
        end
    end
  end

  defp assert_exact_ledger!(request_id) do
    kinds = Repo.all(from entry in LedgerEntry, where: entry.request_id == ^request_id, select: entry.entry_kind)
    assert Enum.sort(kinds) == ["release", "reservation", "settlement"]
  end

  defp turn_payload(setup, turn_id, content) do
    websocket_payload(setup, content, %{
      "request_id" => turn_id,
      "client_metadata" => %{
        "turn_id" => turn_id,
        "x-codex-turn-metadata" => CodexPooler.JSON.encode!(%{"turn_id" => turn_id, "request_kind" => "turn"})
      }
    })
  end
end
