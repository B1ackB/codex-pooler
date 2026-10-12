defmodule CodexPoolerWeb.Runtime.BackendCodexWebsocketOwnerForwarding.MailboxDisconnectTest do
  use CodexPoolerWeb.ConnCase, async: false

  @moduletag capture_log: true

  import Ecto.Query
  import CodexPoolerWeb.Runtime.BackendCodexTestSupport
  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport, only: [model_serving_scope: 0, set_model_serving_mode!: 3, with_info_log: 1]
  import CodexPoolerWeb.Runtime.BackendCodexWebsocketOwnerForwardingSupport, only: [enter_peer_owner_topology!: 0, start_peer_window_owner!: 2, request_logs: 1]

  alias CodexPooler.Accounting.Attempt
  alias CodexPooler.Accounting.LedgerEntry
  alias CodexPooler.Events
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway.Persistence.CodexSession
  alias CodexPooler.Gateway.Transports.Websocket.WebsocketOwnerSession
  alias CodexPooler.Repo
  alias CodexPoolerWeb.Runtime.WebsocketCleanupFence

  @detection_timeout_ms 15_000
  @thread_id "019a0000-0000-7000-8000-00000000e363"
  @window_id "#{@thread_id}:0"

  setup do
    CodexPooler.TestAppEnv.restore_on_exit(:websocket_owner_forwarding_enabled, nil)
    Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, true)
    :ok
  end

  for topology <- [:local, :peer], mode <- ["full", "lite"] do
    @tag topology: topology, mode: mode
    if topology == :peer, do: @tag(slow: "boots a real second BEAM node and shares committed fixture rows")

    test "a reasoning-done mailbox cut closes its provider socket and leaves the immediate full-history successor intact (#{topology} owner, #{mode})", %{topology: topology, mode: mode} do
      {predecessor, logs} = with_info_log(fn -> run_scenario(topology, mode) end)

      if topology == :local do
        [attempt_id] = Repo.all(from(attempt in Attempt, where: attempt.request_id == ^predecessor.id, select: attempt.id))
        line = Enum.find(String.split(logs, "\n"), &String.contains?(&1, "upstream websocket request connection closed"))
        assert is_binary(line)
        assert line =~ "request_id=#{predecessor.id}"
        assert line =~ "attempt_id=#{attempt_id}"
        assert line =~ "reason_code=request_caller_down closed_by=pooler close_completed=true"
        assert line =~ "last_upstream_event_type=response.output_item "
        assert line =~ "text_frame_count=3"
        assert line =~ "terminal_seen=false"
      end
    end
  end

  defp run_scenario(topology, mode) do
    predecessor_gate = make_ref()
    successor_gate = make_ref()

    # This existing fake mode pushes native websocket text frames and holds
    # only the terminal. Unlike the frame-by-frame barrier it returns to the
    # server loop, so a real peer TCP close is observable while held.
    upstream =
      start_upstream(
        FakeUpstream.strict_sequence([
          expected_request(FakeUpstream.delayed_terminal_sse_stream(reasoning_frames(), completed_frame("resp_mailbox_predecessor"), notify: self(), release_ref: predecessor_gate)),
          expected_request(FakeUpstream.delayed_terminal_sse_stream([], completed_frame("resp_mailbox_successor"), notify: self(), release_ref: successor_gate))
        ])
      )

    setup = topology_setup!(topology, upstream)
    set_model_serving_mode!(model_serving_scope(), setup, mode)
    :ok = Events.subscribe_pool(setup.pool.id, ["request_logs"])
    port = start_public_endpoint!()
    {conn_a, ws_a, ref_a} = connect!(port, setup, mode)
    original_history = [user_item("question"), function_call(), function_output()]
    {conn_a, ws_a} = public_websocket_send_text!(conn_a, ws_a, ref_a, encode(payload(setup, original_history, "turn-mailbox")))
    assert_receive {:fake_upstream_timeout_barrier, :before_terminal, predecessor_handler, ^predecessor_gate}, @detection_timeout_ms
    predecessor_monitor = Process.monitor(predecessor_handler)
    {conn_a, _ws_a, types} = receive_types!(conn_a, ws_a, ref_a, 3)
    assert types == ["response.created", "response.output_item.added", "response.output_item.done"]

    owner = owner_pid(topology, setup)
    assert node(owner) == if(topology == :peer, do: setup.peer_owner.node, else: node())
    %{downstream: %{pid: socket_a}} = :sys.get_state(owner)
    hold = hold_session_cleanup!(socket_a)

    # The mailbox update arrives after reasoning is done but before the whole
    # response completes. Hold A's detach so B really overlaps its cleanup.
    _closed = Mint.HTTP.close(conn_a)
    assert_receive {^hold, :held, cleanup}, @detection_timeout_ms
    {conn_b, ws_b, ref_b} = connect!(port, setup, mode)
    history = original_history ++ [reasoning_item(), mailbox_item()]
    {conn_b, ws_b} = public_websocket_send_text!(conn_b, ws_b, ref_b, encode(payload(setup, history, "turn-mailbox")))

    successor_handler =
      receive do
        {:fake_upstream_timeout_barrier, :before_terminal, handler, ^successor_gate} -> handler
      after
        @detection_timeout_ms -> flunk("the provider never received the mailbox successor while predecessor cleanup was held")
      end

    assert successor_handler != predecessor_handler

    # The provider has physically received B, but cannot complete it. Its
    # predecessor must die without releasing its terminal. This detects a
    # cancellation that only settles rows while leaving generation running.
    assert_receive {:DOWN, ^predecessor_monitor, :process, ^predecessor_handler, _reason}, @detection_timeout_ms
    assert Process.alive?(successor_handler)

    cleanup_monitor = Process.monitor(cleanup)
    send(cleanup, {hold, :release})
    assert_receive {:DOWN, ^cleanup_monitor, :process, ^cleanup, _reason}, @detection_timeout_ms
    assert Process.alive?(successor_handler)
    send(successor_handler, {:fake_upstream_release_timeout, successor_gate})
    # The public terminal can arrive before accounting commits. Observe the
    # request's finalized event before asserting its persisted ledger rows.
    assert_receive {Events, %{reason: "request_finalized", payload: %{"status" => "succeeded"}}}, @detection_timeout_ms
    {conn_b, _ws_b, text} = public_websocket_receive_text!(conn_b, ws_b, ref_b)
    terminal = CodexPooler.JSON.decode!(text)
    assert {terminal["type"], get_in(terminal, ["response", "id"])} == {"response.completed", "resp_mailbox_successor"}
    _closed = Mint.HTTP.close(conn_b)
    WebsocketCleanupFence.await_session_cleanups!()
    assert_accounting!(setup.pool.id, mode)

    assert [first, second] = FakeUpstream.requests(upstream)
    assert first.method == "WEBSOCKET" and second.method == "WEBSOCKET"
    assert first.websocket_connection_id != second.websocket_connection_id
    # Lite prepends its declared tools and instruction messages on both sends.
    prefix_count = if mode == "lite", do: 2, else: 0
    assert length(first.json["input"]) == 3 + prefix_count
    assert length(second.json["input"]) == 5 + prefix_count
    refute Map.has_key?(second.json, "previous_response_id")
    assert FakeUpstream.count(upstream) == 2
    assert :ok = FakeUpstream.verify!(upstream)
    CodexPooler.TestDiagnostics.puts(fn -> CodexPooler.JSON.encode!(%{scenario: "mailbox_owner_physical_disconnect", topology: topology, mode: mode, actual_remote_owner: node(owner) != node(), provider_down_observed: true, terminal_reason: "client_disconnected", physical_sends: 2, request_count: 2, settlements_per_request: 1, provider_connections_distinct: first.websocket_connection_id != second.websocket_connection_id}) end)
    hd(request_logs(setup.pool.id))
  end

  defp assert_accounting!(pool_id, mode) do
    assert [predecessor, successor] = request_logs(pool_id)
    assert {predecessor.status, predecessor.response_status_code, predecessor.last_error_code} == {"failed", 499, "client_disconnected"}
    assert {successor.status, successor.response_status_code} == {"succeeded", 200}
    assert Repo.one!(from(entry in LedgerEntry, where: entry.request_id == ^predecessor.id and entry.entry_kind == "settlement" and entry.amount_status == "recorded", select: entry.usage_status)) == "usage_unknown"

    for request <- [predecessor, successor] do
      assert request.request_metadata["routing"]["model_serving_mode"] == mode
      kinds = Repo.all(from(entry in LedgerEntry, where: entry.request_id == ^request.id, select: entry.entry_kind))
      assert Enum.frequencies(kinds) == %{"reservation" => 1, "release" => 1, "settlement" => 1}
      assert Repo.aggregate(from(entry in LedgerEntry, where: entry.request_id == ^request.id and entry.entry_kind == "settlement" and entry.amount_status == "recorded"), :count) == 1
    end
  end

  defp topology_setup!(:peer, upstream) do
    enter_peer_owner_topology!()
    setup = gateway_setup(upstream)
    Map.put(setup, :peer_owner, start_peer_window_owner!(setup, @window_id))
  end

  defp topology_setup!(:local, upstream), do: gateway_setup(upstream)

  defp owner_pid(:peer, setup), do: setup.peer_owner.owner_pid

  defp owner_pid(:local, setup) do
    [session_id] = Repo.all(from(session in CodexSession, where: session.pool_id == ^setup.pool.id, select: session.id))
    assert {:ok, owner} = WebsocketOwnerSession.lookup(session_id)
    owner
  end

  defp hold_session_cleanup!(socket) do
    hold = make_ref()
    handler_id = {__MODULE__, hold}
    on_exit(fn -> :telemetry.detach(handler_id) end)
    :ok = :telemetry.attach(handler_id, [:codex_pooler, :repo, :query], &__MODULE__.hold_session_cleanup_query/4, %{hold: hold, test: self(), socket: socket})
    hold
  end

  @doc false
  def hold_session_cleanup_query(_event, _measurements, _metadata, %{hold: hold, test: test, socket: socket}) do
    if socket in Process.get(:"$callers", []) and match?({CodexPoolerWeb.WebsocketControlPath, _function, _arity}, Process.get(:"$initial_call")) and is_nil(Process.get({__MODULE__, hold})) do
      Process.put({__MODULE__, hold}, :held)
      monitor = Process.monitor(test)
      send(test, {hold, :held, self()})

      receive do
        {^hold, :release} -> :ok
        {:DOWN, ^monitor, :process, ^test, _reason} -> :ok
      after
        @detection_timeout_ms -> :ok
      end

      Process.demonitor(monitor, [:flush])
    end

    :ok
  end

  defp connect!(port, setup, mode) do
    headers = [{"x-codex-window-id", @window_id}]
    headers = if mode == "lite", do: [{"x-openai-internal-codex-responses-lite", "true"} | headers], else: headers
    {conn, websocket, ref, _headers} = public_websocket_connect_with_request_headers!(port, setup, Ecto.UUID.generate(), "/backend-api/codex/responses", headers)
    {conn, websocket, ref}
  end

  defp expected_request(respond), do: FakeUpstream.expect_request(path: "/backend-api/codex/responses", json: [valid: true, equals: %{"type" => "response.create"}], respond: respond)

  defp payload(setup, input, turn_id) do
    %{
      "type" => "response.create",
      "model" => setup.model.exposed_model_id,
      "instructions" => "synthetic instructions",
      "stream" => true,
      "store" => false,
      "tools" => [],
      "client_metadata" => %{
        "session_id" => @thread_id,
        "thread_id" => @thread_id,
        "turn_id" => turn_id,
        "x-codex-window-id" => @window_id,
        "x-codex-turn-metadata" => encode(%{"session_id" => @thread_id, "thread_id" => @thread_id, "turn_id" => turn_id, "request_kind" => "turn"})
      },
      "input" => input
    }
  end

  defp user_item(text), do: %{"type" => "message", "role" => "user", "content" => [%{"type" => "input_text", "text" => "synthetic " <> text}]}
  defp function_call, do: %{"type" => "function_call", "call_id" => "call_mailbox", "name" => "shell", "arguments" => "{}"}
  defp function_output, do: %{"type" => "function_call_output", "call_id" => "call_mailbox", "output" => "synthetic output"}
  defp mailbox_item, do: %{"type" => "agent_message", "author" => "/root/worker", "recipient" => "/root", "content" => [%{"type" => "input_text", "text" => "synthetic task update"}]}
  defp reasoning_item, do: %{"type" => "reasoning", "id" => "rs_mailbox", "summary" => [], "status" => "completed"}

  defp reasoning_frames do
    [
      %{"type" => "response.created", "response" => %{"id" => "resp_mailbox_predecessor", "status" => "in_progress", "output" => []}},
      %{"type" => "response.output_item.added", "output_index" => 0, "item" => Map.put(reasoning_item(), "status", "in_progress")},
      %{"type" => "response.output_item.done", "output_index" => 0, "item" => reasoning_item()}
    ]
  end

  defp completed_frame(id), do: %{"type" => "response.completed", "response" => %{"id" => id, "status" => "completed", "output" => [], "usage" => %{"input_tokens" => 3, "output_tokens" => 2, "total_tokens" => 5}}}
  defp encode(map), do: CodexPooler.JSON.encode!(map)

  defp receive_types!(conn, websocket, ref, count) do
    Enum.reduce(1..count, {conn, websocket, []}, fn _index, {conn, websocket, types} ->
      {conn, websocket, text} = public_websocket_receive_text!(conn, websocket, ref)
      {conn, websocket, types ++ [CodexPooler.JSON.decode!(text)["type"]]}
    end)
  end
end
