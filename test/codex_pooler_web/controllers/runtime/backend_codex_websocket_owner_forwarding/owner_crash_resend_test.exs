defmodule CodexPoolerWeb.Runtime.BackendCodexWebsocketOwnerForwarding.OwnerCrashResendTest do
  # A session's owner ends without its `terminate/2` while its turn has
  # shown output. The socket's cleanup settles HTTP 499; its response task
  # settles HTTP 502. Both now leave a canonically interrupted turn, while
  # persisted rows from older versions can retain the response task's failed
  # turn. The released client closes on 1011 and resends the turn whole on a
  # new socket. Each disposition needs the exact executor's durable terminal
  # proof before it can admit that resend once. Lease takeover leaves the old
  # socket's crash cleanup stale, so that path must keep the response task's
  # settlement and let the stale cleanup stand down at info.
  #
  # One node: the real public listener, owner forwarding on, the session's
  # owner on this node, the Pool's serving mode forced to Full, FakeUpstream
  # holding the turn after its first output, the released client's native
  # frames, a one-second owner call budget. The execution proof publisher
  # runs as in production (the test configuration turns it off).
  use CodexPoolerWeb.ConnCase, async: false

  import Ecto.Query
  import CodexPoolerWeb.Runtime.BackendCodexTestSupport

  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport,
    only: [await_socket_connection_state!: 2, completed_response_frames: 4, receive_frames_until_close!: 3, receive_native_terminal!: 3, released_client_frame: 2, socket_connection_state!: 1, with_info_log: 1]

  alias CodexPooler.Accounting.{Attempt, LedgerEntry, Request, RequestClientRetryLink}
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway.OperationalSettings
  alias CodexPooler.Gateway.Persistence.{BridgeOwnerLease, CodexTurn}
  alias CodexPooler.Gateway.Transports.Websocket.{OwnerDefaults, WebsocketOwnerSession}
  alias CodexPooler.Platform.ExecutionTerminalProofs
  alias CodexPooler.Pools.ModelServingOverride
  alias CodexPooler.Repo
  alias CodexPoolerWeb.Runtime.OwnerLossScenario, as: Scenario
  alias CodexPoolerWeb.Runtime.WebsocketCleanupFence

  @moduletag capture_log: true

  @owner_call_budget_ms 1_000
  @detection_timeout_ms 15_000
  @crashed_close {:close, 1011, "websocket owner crashed"}

  setup do
    CodexPooler.TestAppEnv.restore_on_exit(:websocket_owner_forwarding_enabled)
    Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, true)
    original_env = CodexPooler.TestAppEnv.restore_on_exit(OwnerDefaults)
    Application.put_env(:codex_pooler, OwnerDefaults, Keyword.merge(original_env, owner_call_timeout_ms: @owner_call_budget_ms))
    :ok
  end

  # The live socket is held until the new socket's init has finished, so the
  # takeover always comes before its crash cleanup, as it does unheld.
  @tag slow: "kills an owner blocked inside a callback past two one-second budgets while it runs a turn that showed output"
  test "a turn in flight on an owner killed by a new socket's reuse check closes, and the client's resend is served once" do
    _publisher = CodexPooler.ExecutionProofSupport.start_publisher!()
    {live, release_ref} = turn_in_flight!()
    :ok = age_last_renewal!(live)
    owner_monitor = Process.monitor(live.owner)
    busy = hold_owner_busy!(live.owner, @detection_timeout_ms)
    :ok = hold_socket!(live.client.socket)

    {frames, log} =
      with_info_log(fn ->
        second = Scenario.connect!(live.port, live.setup, Scenario.native_route(), live.window)
        assert_receive {:DOWN, ^owner_monitor, :process, _owner, :killed}, @detection_timeout_ms
        _initialized = await_socket_connection_state!(second.socket, &(is_map(&1) and is_pid(Map.get(&1, :websocket_owner_pid))))
        :sys.resume(live.client.socket)
        {conn, _websocket, frames} = receive_frames_until_close!(live.conn, live.websocket, live.client.ref)
        Mint.HTTP.close(conn)
        :ok = WebsocketCleanupFence.await_listener_socket_cleanup!(live.client.socket, @detection_timeout_ms)
        Scenario.close!(second)
        frames
      end)

    assert frames == [@crashed_close]
    assert Task.await(busy, @detection_timeout_ms) == :owner_gone
    killed = await_request!(live.turn_request_id, &match?(%Request{status: "failed", response_status_code: 502, last_error_code: "owner_crashed"}, &1))
    assert %CodexTurn{status: "interrupted", error_code: "owner_crashed"} = Repo.get_by!(CodexTurn, request_id: killed.id)

    # The takeover released the killed owner's lease, once, and the crash
    # cleanup that found it released stood down at info.
    assert [%BridgeOwnerLease{status: "released", metadata: %{"release_reason" => "owner_unavailable_takeover"}}, %BridgeOwnerLease{status: "active"}] = session_leases(live)
    assert log =~ "websocket owner lifecycle recovery superseded codex_session_id=#{live.session_id} recovery_reason=owner_crashed reason_code=lease_taken_over"
    refute log =~ "stale_owner_cleanup"

    assert_resend_served_once!(live, killed, release_ref)
  end

  # The live socket is held while its owner crashes, so its response task
  # settles the turn first, as it does in about four plain crashes out of nine.
  @tag slow: "crashes an owner while it runs a turn that showed output, with the socket held so its task settles the turn"
  test "a turn in flight on an owner that crashed, settled by its socket's response task, is resent and served once" do
    _publisher = CodexPooler.ExecutionProofSupport.start_publisher!()
    {live, release_ref} = turn_in_flight!()
    crashed = crash_with_socket_held!(live)
    assert :ok = await_executor_proof!(crashed)
    assert_resend_served_once!(live, crashed, release_ref)
  end

  # Without the proof of the executor's end the settled shape admits nothing:
  # its executor might still settle the attempt.
  @tag slow: "crashes an owner while it runs a turn that showed output, with no execution proof published"
  test "a turn settled by the response task after its owner crashed is not resent without the proof of its executor's end" do
    {live, _release_ref} = turn_in_flight!()
    _crashed = crash_with_socket_held!(live)

    assert %{"type" => "error", "status" => 409, "error" => %{"code" => "duplicate_turn"}} = served_socket!(live, live.frame)
    assert [_one, %Request{status: "failed"}] = pool_requests(live)
    assert Repo.all(from(link in RequestClientRetryLink, where: link.predecessor_request_id == ^live.turn_request_id)) == []
  end

  # The session's owner on this node, started by the socket's first turn, a
  # second turn held by the provider after its created event and a delta the
  # client read, and the frame of that turn.
  defp turn_in_flight! do
    release_ref = make_ref()

    upstream =
      start_upstream(
        # provenance: synthetic_adversarial (an owner that ends without its terminate while a turn it runs has shown output)
        FakeUpstream.repeat_last([
          completed_response_frames("resp_owner_crash_one", [], 3, 2),
          FakeUpstream.barrier_websocket_frames(turn_frames(), notify: self(), release_ref: release_ref),
          completed_response_frames("resp_owner_crash_resend", [], 3, 2)
        ])
      )

    setup = gateway_setup(upstream)
    Repo.insert!(%ModelServingOverride{pool_id: setup.pool.id, exposed_model_id: setup.model.exposed_model_id, mode: "full"})
    {_server, port} = start_public_endpoint_with_server!()
    window = Scenario.window()
    client = Scenario.connect!(port, setup, Scenario.native_route(), window)
    {client, one} = Scenario.turn!(client, setup, "turn one")
    assert Scenario.terminal(one) == {"response.completed", "resp_owner_crash_one"}
    state = socket_connection_state!(client.socket)
    assert {:ok, owner} = WebsocketOwnerSession.lookup(state.codex_session.id)

    frame = released_client_frame(setup, window.thread).(native_text_input("the turn in flight"), Ecto.UUID.generate(), %{})
    {conn, websocket} = public_websocket_send_text!(client.conn, client.websocket, client.ref, frame)

    for ordinal <- 0..1 do
      assert_receive {:fake_upstream_frame_barrier, ^ordinal, _handler, ^release_ref}, @detection_timeout_ms
      :ok = FakeUpstream.release_frame(upstream, release_ref)
    end

    assert_receive {:fake_upstream_frame_barrier, 2, _handler, ^release_ref}, @detection_timeout_ms
    {conn, websocket, _created} = receive_text!(conn, websocket, client.ref, "response.created")
    {conn, websocket, _delta} = receive_text!(conn, websocket, client.ref, "response.output_text.delta")
    turn_request_id = Repo.one!(from(r in Request, where: r.pool_id == ^setup.pool.id and r.status == "in_progress", select: r.id))

    live = %{
      setup: setup,
      port: port,
      window: window,
      client: client,
      conn: conn,
      websocket: websocket,
      owner: owner,
      upstream: upstream,
      session_id: state.codex_session.id,
      frame: frame,
      turn_request_id: turn_request_id
    }

    {live, release_ref}
  end

  # Kills the owner while its socket is held, so the socket's response task
  # settles the turn before the socket's crash cleanup runs; returns the
  # turn's settled request.
  defp crash_with_socket_held!(live) do
    :ok = hold_socket!(live.client.socket)
    owner_monitor = Process.monitor(live.owner)
    Process.exit(live.owner, :kill)
    assert_receive {:DOWN, ^owner_monitor, :process, _owner, :killed}, @detection_timeout_ms
    crashed = await_request!(live.turn_request_id, &match?(%Request{status: "failed", response_status_code: 502, last_error_code: "owner_crashed"}, &1))
    assert %CodexTurn{status: "interrupted", error_code: "owner_crashed"} = Repo.get_by!(CodexTurn, request_id: crashed.id)

    :sys.resume(live.client.socket)
    {conn, _websocket, frames} = receive_frames_until_close!(live.conn, live.websocket, live.client.ref)
    assert frames == [@crashed_close]
    Mint.HTTP.close(conn)
    :ok = WebsocketCleanupFence.await_listener_socket_cleanup!(live.client.socket, @detection_timeout_ms)
    crashed
  end

  # The client's resend of the whole turn on a new socket is served once: one
  # successor linked to the killed turn, charged for its own usage, while the
  # killed turn's request stays at no charge.
  defp assert_resend_served_once!(live, predecessor, release_ref) do
    assert %{"type" => "response.completed", "response" => %{"id" => "resp_owner_crash_resend"}} = served_socket!(live, live.frame)
    assert [_one, _predecessor, successor] = await_settled!(live, 3)
    assert successor.status == "succeeded"
    assert [%RequestClientRetryLink{successor_request_id: successor_id}] = Repo.all(from(link in RequestClientRetryLink, where: link.predecessor_request_id == ^predecessor.id))
    assert successor_id == successor.id
    assert settled_cost(predecessor.id) == Decimal.new(0)
    assert Decimal.gt?(settled_cost(successor.id), 0)
    # The killed turn's provider connection went with its owner.
    refute_received {:fake_upstream_frame_barrier, 3, _handler, ^release_ref}
  end

  defp served_socket!(live, frame) do
    client = Scenario.connect!(live.port, live.setup, Scenario.native_route(), live.window)
    {conn, websocket} = public_websocket_send_text!(client.conn, client.websocket, client.ref, frame)
    {conn, websocket, terminal} = receive_native_terminal!(conn, websocket, client.ref)
    Scenario.close!(%{client | conn: conn, websocket: websocket})
    terminal
  end

  defp settled_cost(request_id) do
    Repo.one!(from(entry in LedgerEntry, where: entry.request_id == ^request_id and entry.entry_kind == "settlement" and entry.amount_status == "recorded", select: entry.settled_cost_micros))
    |> Decimal.normalize()
  end

  defp session_leases(live), do: Repo.all(from(lease in BridgeOwnerLease, where: lease.codex_session_id == ^live.session_id, order_by: [asc: lease.acquired_at]))

  defp pool_requests(live), do: Repo.all(from(r in Request, where: r.pool_id == ^live.setup.pool.id, order_by: [asc: r.admitted_at]))

  defp await_executor_proof!(request) do
    attempt = Repo.one!(from(a in Attempt, where: a.request_id == ^request.id))
    await!(fn -> ExecutionTerminalProofs.terminal?(attempt) end, "the executor's proof was never published")
  end

  defp await_request!(request_id, predicate) do
    :ok = await!(fn -> predicate.(Repo.get!(Request, request_id)) end, "the turn's request never settled as expected")
    Repo.get!(Request, request_id)
  end

  defp await_settled!(live, count) do
    :ok = await!(fn -> match?(requests when length(requests) == count, pool_requests(live)) and Enum.all?(pool_requests(live), &(&1.status not in ["accepted", "in_progress"])) end, "the Pool's requests never settled")
    pool_requests(live)
  end

  defp await!(fun, message) do
    deadline = System.monotonic_time(:millisecond) + @detection_timeout_ms
    await_until!(fun, message, deadline)
  end

  defp await_until!(fun, message, deadline) do
    cond do
      fun.() ->
        :ok

      System.monotonic_time(:millisecond) >= deadline ->
        flunk(message)

      true ->
        Process.sleep(20)
        await_until!(fun, message, deadline)
    end
  end

  # Blocks the owner inside a callback for `hold_ms`: the owner runs the
  # function itself and handles nothing else meanwhile.
  defp hold_owner_busy!(owner, hold_ms) do
    test = self()
    on_exit(fn -> if Process.alive?(owner), do: Process.exit(owner, :kill) end)

    hold = fn state ->
      send(test, {:owner_busy, owner})
      Process.sleep(hold_ms)
      state
    end

    busy =
      Task.async(fn ->
        try do
          _state = :sys.replace_state(owner, hold, hold_ms + @detection_timeout_ms)
          :released
        catch
          :exit, _owner_gone -> :owner_gone
        end
      end)

    assert_receive {:owner_busy, ^owner}, @detection_timeout_ms
    busy
  end

  # Suspends a socket's listener connection process, so it handles nothing
  # (its owner's exit included) until the test resumes it.
  defp hold_socket!(socket) do
    :ok = :sys.suspend(socket)

    on_exit(fn ->
      try do
        :sys.resume(socket)
      catch
        :exit, _socket_gone -> :ok
      end
    end)

    :ok
  end

  # Ages the owner's record of its last lease renewal, as if it had started
  # none for longer than the lease TTL. The update runs inside the owner,
  # because only the process a registry value names can write it.
  defp age_last_renewal!(live) do
    aged_by_ms = OperationalSettings.current().bridge_owner_lease_ttl_seconds * 1_000 + 1_000
    session_id = live.session_id

    :sys.replace_state(live.owner, fn state ->
      {{:ready, _digest, _aged}, {:ready, _same_digest, _renewed}} =
        Registry.update_value(WebsocketOwnerSession.Registry, session_id, fn {:ready, digest, last_renewal_monotonic_ms} -> {:ready, digest, last_renewal_monotonic_ms - aged_by_ms} end)

      state
    end)

    :ok
  end

  defp receive_text!(conn, websocket, ref, type) do
    {conn, websocket, text} = public_websocket_receive_text!(conn, websocket, ref)

    case CodexPooler.JSON.decode!(text) do
      %{"type" => ^type} = event -> {conn, websocket, event}
      %{"type" => "codex.response.metadata"} -> receive_text!(conn, websocket, ref, type)
    end
  end

  defp turn_frames do
    created = %{"type" => "response.created", "response" => %{"id" => "resp_owner_crash_turn", "status" => "in_progress"}}
    deltas = for i <- 1..10, do: %{"type" => "response.output_text.delta", "delta" => "synthetic #{i} "}
    completed = %{"type" => "response.completed", "response" => %{"id" => "resp_owner_crash_turn", "status" => "completed", "output" => [], "usage" => %{"input_tokens" => 5, "output_tokens" => 10, "total_tokens" => 15}}}
    Enum.map([created | deltas] ++ [completed], &CodexPooler.JSON.encode!/1)
  end
end
