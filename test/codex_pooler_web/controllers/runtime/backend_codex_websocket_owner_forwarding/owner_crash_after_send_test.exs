defmodule CodexPoolerWeb.Runtime.BackendCodexWebsocketOwnerForwarding.OwnerCrashAfterSendTest do
  # A session's owner ends without its `terminate/2` while it runs a turn:
  # killed (the reuse check kills an owner blocked in a callback) or crashed.
  # The turn's submission catches the owner's exit and used to hand the turn
  # to a replacement owner whenever no frame of it had arrived, so a turn whose
  # payload the provider had already received went to the provider a second
  # time on a new connection, with one succeeded attempt in the request log
  # (findings#327). The upstream session now tells the submission before the
  # payload can leave, and the submission resubmits only a turn whose payload
  # never started to leave; any other turn settles `owner_crashed` (findings#325
  # row 325-6: settle after commit, never resend). A client socket's turn is
  # never handed over at all: the socket closes `1011` on its owner's crash
  # before a replacement owner can answer, so the takeover reached nobody and
  # the client's resend sent the turn to the provider a second time, charged
  # twice (findings#328). The HTTP bridge's relay takes a replacement's frames,
  # and its turn still is.
  #
  # One node: the real public listener, owner forwarding on, the session's
  # owner and its upstream session on this node, the Pool's serving mode
  # forced to Full or Lite, FakeUpstream over a real websocket, the released
  # client's native frames or a public `/v1` SDK request. The socket and its
  # response task meet the owner's exit in the order the schedulers give, or
  # the socket is held so the task meets it first, or the task then also holds
  # a database connection while the socket handles the crash
  # (`OwnerCrashAfterSendScenario`); the peer family is
  # `remote_owner_crash_after_send_test.exs`. The `/v1` arms and that third
  # order give every process its own connection, as production does
  # (`per_process_connections?/1`).
  use CodexPoolerWeb.ConnCase, async: false

  import Ecto.Query
  import CodexPoolerWeb.Runtime.BackendCodexTestSupport
  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport, only: [receive_native_terminal!: 3, socket_connection_state!: 1, with_info_log: 1]

  alias CodexPooler.Accounting.{Attempt, LedgerEntry, RequestClientRetryLink}
  alias CodexPooler.Gateway.Persistence.CodexSession
  alias CodexPooler.Gateway.Transports.Websocket.WebsocketOwnerSession
  alias CodexPooler.Platform.ExecutionTerminalProofs
  alias CodexPooler.Repo
  alias CodexPoolerWeb.Runtime.OwnerCrashAfterSendScenario, as: Crash
  alias CodexPoolerWeb.Runtime.OwnerLossScenario, as: Scenario

  @moduletag capture_log: true

  @crashed_close {:close, 1011, "websocket owner crashed"}
  @detection_timeout_ms 15_000

  setup ctx do
    CodexPooler.TestAppEnv.restore_on_exit(:websocket_owner_forwarding_enabled)
    Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, true)
    if per_process_connections?(ctx), do: Crash.per_process_connections!()
    :ok
  end

  for route <- [Scenario.native_route(), Scenario.public_route()], mode <- ["full", "lite"] do
    @tag route: route, mode: mode
    test "#{route} #{mode}: a turn the provider received settles owner_crashed when its owner is killed, and is never sent again", ctx do
      release_ref = make_ref()
      upstream = Crash.upstream!(:after_receipt, "resp_owner_sent", release_ref)
      %{setup: setup, client: client} = connect!(upstream, ctx)
      client = Crash.send_turn!(client, setup, "the turn the provider holds")

      :ok = Crash.await_kill_point!(:after_receipt, upstream, "resp_owner_sent", release_ref)
      assert Crash.generations(upstream) == 1
      %{owner: owner, session_id: session_id, request_id: request_id} = turn!(client, setup)

      :ok = Crash.hold_socket!(client.socket)

      {:ok, log} =
        with_info_log(fn ->
          :ok = Crash.kill_owner!(owner)
          assert Crash.await_outcome!(upstream, request_id, 1) == :settled
          :ok
        end)

      assert Crash.generations(upstream) == 1
      :ok = Crash.assert_settled_on_crash!(request_id, session_id)
      :ok = Crash.assert_fence_decision!(log, "settled", "payload_started", Crash.turn_correlators(request_id, session_id))
      assert Crash.write_refusal_lines(log) == []
      assert List.last(Crash.close_client!(client)) == @crashed_close
    end

    for order <- [:natural, :task_first, :task_holds_connection] do
      @tag route: route, mode: mode, order: order
      test "#{route} #{mode} (#{order}): a turn whose payload never left settles owner_crashed when its owner is killed, and the client's resend is its one send", ctx do
        publisher = Crash.start_proof_publisher!(if per_process_connections?(ctx), do: :committed, else: :sandboxed)
        release_ref = make_ref()
        upstream = Crash.upstream!(:before_write, "resp_owner_unsent", release_ref)
        %{setup: setup, client: client, port: port, window: window} = connect!(upstream, ctx)
        frame = Crash.frame(setup, client, "the turn whose connection is still opening")
        client = Crash.send_frame!(client, frame)

        :ok = Crash.await_kill_point!(:before_write, upstream, "resp_owner_unsent", release_ref)
        assert Crash.generations(upstream) == 0
        %{owner: owner, session_id: session_id, request_id: request_id} = turn!(client, setup)
        :ok = Crash.stop_session_owner_on_exit(node(), session_id)

        {frames, log} = with_info_log(fn -> kill_and_close!(ctx, client, owner, upstream, request_id, session_id) end)
        assert List.last(frames) == @crashed_close
        assert Process.alive?(publisher)
        :ok = assert_socket_turn_fence!(log, ctx, Crash.turn_correlators(request_id, session_id))
        {served, _answers} = Crash.resend_like_released_client!(port, setup, ctx.route, window, frame)
        assert %{"type" => "response.completed", "response" => %{"id" => "resp_owner_unsent_first_send"}} = served
        :ok = Crash.assert_resend_served_once!(setup, upstream, ctx.route, request_id)
      end
    end
  end

  # The HTTP bridge's relay is no socket: it takes the replacement owner's
  # frames, so a bridged turn whose payload never left is still handed over and
  # answered once (findings#328).
  for mode <- ["full", "lite"] do
    @tag mode: mode
    test "HTTP bridge #{mode}: a turn whose payload never left is answered once by a replacement owner when its owner is killed", ctx do
      release_ref = make_ref()
      upstream = Crash.upstream!(:before_write, "resp_bridge_unsent", release_ref)
      setup = gateway_setup(upstream)
      :ok = Crash.serve!(setup, ctx.mode)
      session_header = "owner-crash-bridge-#{System.unique_integer([:positive])}"
      payload = %{"model" => setup.model.exposed_model_id, "input" => "the bridged turn whose connection is still opening", "stream" => true}
      bridged = Task.async(fn -> post_bridged!(setup, session_header, payload) end)

      :ok = Crash.await_kill_point!(:before_write, upstream, "resp_bridge_unsent", release_ref)
      session = Repo.one!(from(s in CodexSession, where: s.pool_id == ^setup.pool.id))
      assert {:ok, owner} = WebsocketOwnerSession.lookup(session.id)
      :ok = Crash.stop_session_owner_on_exit(node(), session.id)
      request_id = Crash.turn_request_id!(setup)

      {response, log} =
        with_info_log(fn ->
          :ok = Crash.kill_owner!(owner)
          Task.await(bridged, @detection_timeout_ms)
        end)

      :ok = Crash.assert_fence_decision!(log, "takeover", "payload_unsent", Crash.turn_correlators(request_id, session.id))
      assert response.status == 200
      assert response.resp_body =~ ~s("id":"resp_bridge_unsent_first_send")
      assert response.resp_body =~ "response.completed"
      assert [_one] = Crash.await_settled!(setup, 1)
      assert Crash.generations(upstream) == 1
      :ok = Crash.assert_recovered!(request_id, session.id)
    end
  end

  # The forwarder's payload write observer has run (the turn counts as
  # started) and the payload is not written. Linked, the upstream session goes
  # with its killed owner and never writes; unlinked, it stands for a session
  # that handles its owner's exit only after its write and writes once
  # released. Either way the turn settles `owner_crashed` and nothing sends it
  # again.
  for link <- [:linked, :unlinked] do
    @tag link: link
    test "native full: an owner killed between the payload write observer and the write (#{link}) settles the turn without sending it again", ctx do
      release_ref = make_ref()
      hold_ref = make_ref()
      upstream = Crash.upstream!(:after_receipt, "resp_owner_marked", release_ref)
      setup = gateway_setup(upstream)
      :ok = Crash.serve!(setup, "full")
      window = Scenario.window()
      held = Crash.start_held_owner!(setup, window.id, node(), hold_ref, ctx.link)
      {_server, port} = start_public_endpoint_with_server!()
      client = Scenario.connect!(port, setup, Scenario.native_route(), window)
      client = Crash.send_turn!(client, setup, "the turn held at its write")

      assert_receive {:payload_write_held, session_pid, ^hold_ref, :ok}, @detection_timeout_ms
      assert Crash.generations(upstream) == 0
      request_id = Crash.turn_request_id!(setup)
      session_monitor = Process.monitor(session_pid)

      :ok = Crash.hold_socket!(client.socket)

      {:ok, log} =
        with_info_log(fn ->
          :ok = Crash.kill_owner!(held.owner_pid)
          assert Crash.await_outcome!(upstream, request_id, 0) == :settled

          case ctx.link do
            :linked ->
              assert_receive {:DOWN, ^session_monitor, :process, ^session_pid, :killed}, @detection_timeout_ms
              assert Crash.generations(upstream) == 0

            :unlinked ->
              :ok = Crash.release_held_write(session_pid, hold_ref)
              :ok = Crash.await_kill_point!(:after_receipt, upstream, "resp_owner_marked", release_ref)
              assert Crash.generations(upstream) == 1
              Process.exit(session_pid, :kill)
              assert_receive {:DOWN, ^session_monitor, :process, ^session_pid, :killed}, @detection_timeout_ms
          end

          :ok
        end)

      :ok = Crash.assert_settled_on_crash!(request_id, held.session.id)
      :ok = Crash.assert_fence_decision!(log, "settled", "payload_started", Crash.turn_correlators(request_id, held.session.id))
      assert Crash.write_refusal_lines(log) == []
      assert List.last(Crash.close_client!(client)) == @crashed_close
      assert Crash.generations(upstream) == if(ctx.link == :linked, do: 0, else: 1)
    end
  end

  # The owner is killed while its upstream session waits right before the
  # payload write observer: nothing of the turn left, the submission claims it
  # and, the turn being a socket's, settles it `owner_crashed`. Linked, the
  # session goes with its owner; unlinked, it reaches the observer once
  # released, finds the turn claimed and ends the request unsent, with one
  # `upstream websocket payload write refused` line (findings#329 row J).
  for link <- [:linked, :unlinked] do
    @tag link: link
    test "native full: a claimed turn's late write is refused (#{link}), and the provider never receives it", ctx do
      release_ref = make_ref()
      hold_ref = make_ref()
      upstream = Crash.upstream!(:after_receipt, "resp_owner_claimed", release_ref)
      setup = gateway_setup(upstream)
      :ok = Crash.serve!(setup, "full")
      window = Scenario.window()
      held = Crash.start_held_owner!(setup, window.id, node(), hold_ref, ctx.link, :before_mark)
      {_server, port} = start_public_endpoint_with_server!()
      client = Scenario.connect!(port, setup, Scenario.native_route(), window)
      client = Crash.send_turn!(client, setup, "the turn held before its write")

      assert_receive {:payload_write_held, session_pid, ^hold_ref, :unmarked}, @detection_timeout_ms
      request_id = Crash.turn_request_id!(setup)
      session_monitor = Process.monitor(session_pid)
      :ok = Crash.hold_socket!(client.socket)

      {:ok, log} =
        with_info_log(fn ->
          :ok = Crash.kill_owner!(held.owner_pid)
          assert Crash.await_outcome!(upstream, request_id, 0) == :settled

          if ctx.link == :unlinked do
            :ok = Crash.release_held_write(session_pid, hold_ref)
            # The session serves one call at a time: once it answers, the
            # released request has met its refusal. It outlives its owner, so
            # it is stopped here.
            _served = :sys.get_state(session_pid)
            Process.exit(session_pid, :kill)
          end

          assert_receive {:DOWN, ^session_monitor, :process, ^session_pid, :killed}, @detection_timeout_ms
          :ok
        end)

      assert Crash.generations(upstream) == 0
      :ok = Crash.assert_settled_on_crash!(request_id, held.session.id)
      turn = Crash.turn_correlators(request_id, held.session.id)
      :ok = Crash.assert_fence_decision!(log, "settled", "socket_turn", turn)

      case ctx.link do
        :linked -> assert Crash.write_refusal_lines(log) == []
        :unlinked -> :ok = Crash.assert_write_refused!(log, turn)
      end

      assert List.last(Crash.close_client!(client)) == @crashed_close
    end
  end

  # An owner stopped as the reuse check stops a stale one, `{:shutdown,
  # :stale_owner}` (`stop_stale_owner/2`), while its turn's handshake is held:
  # an exit the submission does not recover, so it settles the turn without
  # a takeover, and says so.
  test "native full: an owner stopped stale under a turn settles it with the exit it does not recover" do
    release_ref = make_ref()
    upstream = Crash.upstream!(:before_write, "resp_owner_stale", release_ref)
    %{setup: setup, client: client} = connect!(upstream, %{route: Scenario.native_route(), mode: "full"})
    client = Crash.send_turn!(client, setup, "the turn whose owner goes stale")
    :ok = Crash.await_kill_point!(:before_write, upstream, "resp_owner_stale", release_ref)
    %{owner: owner, session_id: session_id, request_id: request_id} = turn!(client, setup)

    {:ok, log} =
      with_info_log(fn ->
        owner_monitor = Process.monitor(owner)
        _ordered = :sys.get_state(owner)
        :ok = GenServer.stop(owner, {:shutdown, :stale_owner}, @detection_timeout_ms)
        assert_receive {:DOWN, ^owner_monitor, :process, ^owner, {:shutdown, :stale_owner}}, @detection_timeout_ms
        assert Crash.await_outcome!(upstream, request_id, 0) == :settled
        :ok
      end)

    assert Crash.generations(upstream) == 0
    turn = Crash.turn_correlators(request_id, session_id)
    :ok = Crash.assert_fence_decision!(log, "settled", "exit_not_recoverable", turn, %{"owner_exit" => "shutdown"})
    Mint.HTTP.close(client.conn)
  end

  # The released client closes on the socket's 1011 and resends the turn whole
  # on a new socket. Once the proof of the attempt's executor's end exists the
  # duplicate-turn fence serves it once, linked to the settled turn, which stays
  # at no charge: the provider has the turn twice, once for each request.
  for mode <- ["full", "lite"] do
    test "native #{mode}: the client's resend of a turn settled owner_crashed after its payload left is served once" do
      _publisher = CodexPooler.ExecutionProofSupport.start_publisher!()
      release_ref = make_ref()
      upstream = Crash.upstream!(:after_receipt, "resp_owner_sent", release_ref)
      %{setup: setup, client: client, port: port, window: window} = connect!(upstream, %{route: Scenario.native_route(), mode: unquote(mode)})
      frame = Crash.frame(setup, client, "the turn the provider holds")
      client = Crash.send_frame!(client, frame)

      :ok = Crash.await_kill_point!(:after_receipt, upstream, "resp_owner_sent", release_ref)
      %{owner: owner, session_id: session_id, request_id: request_id} = turn!(client, setup)
      :ok = Crash.hold_socket!(client.socket)
      :ok = Crash.kill_owner!(owner)
      assert Crash.await_outcome!(upstream, request_id, 1) == :settled
      :ok = Crash.assert_settled_on_crash!(request_id, session_id)
      assert List.last(Crash.close_client!(client)) == @crashed_close
      :ok = await_executor_proof!(request_id)

      retry = Scenario.connect!(port, setup, Scenario.native_route(), window)
      retry = Crash.send_frame!(retry, frame)
      {conn, websocket, served} = receive_native_terminal!(retry.conn, retry.websocket, retry.ref)
      Scenario.close!(%{retry | conn: conn, websocket: websocket})

      assert %{"type" => "response.completed", "response" => %{"id" => "resp_owner_sent_written_again"}} = served
      assert [%RequestClientRetryLink{successor_request_id: successor_id}] = Repo.all(from(link in RequestClientRetryLink, where: link.predecessor_request_id == ^request_id))
      assert Scenario.settled_statuses!(setup, 2) == ["failed", "succeeded"]
      assert Crash.generations(upstream) == 2
      assert settled_cost(request_id) == Decimal.new(0)
      assert Decimal.gt?(settled_cost(successor_id), 0)
    end
  end

  # Kills the owner, in the order the schedulers give, with the socket held so
  # its response task meets the exit first, or also with the task then
  # holding a database connection while the socket handles the crash (the
  # interleaving of Drone 1865), and returns the frames the client read until
  # its socket's Close.
  defp kill_and_close!(%{order: :natural}, client, owner, _upstream, _request_id, _session_id) do
    :ok = Crash.kill_owner!(owner)
    Crash.await_close!(client)
  end

  defp kill_and_close!(%{order: :task_first}, client, owner, upstream, request_id, session_id) do
    :ok = Crash.hold_socket!(client.socket)
    :ok = Crash.kill_owner!(owner)
    assert Crash.await_outcome!(upstream, request_id, 0) == :settled
    :ok = Crash.assert_settled_on_crash!(request_id, session_id)
    Crash.close_client!(client)
  end

  defp kill_and_close!(%{order: :task_holds_connection, route: route}, client, owner, _upstream, request_id, session_id),
    do: Crash.kill_with_task_holding_connection!(client, owner, route, request_id, session_id)

  # A public `/v1` socket cancels its turn's response task when it handles its
  # owner's crash, while the task may still be settling the turn or publishing
  # what it settled, and the third order holds the task on the database while
  # the socket handles it. On the one connection the sandbox shares, the
  # cancellation took every other process's connection down (Drone 1865) and
  # the hold blocks the socket's own cleanup, so those arms give every process
  # its own connection, as production does. A native socket waits for its task
  # and its other arms keep the shared connection.
  defp per_process_connections?(%{route: "/v1/responses"}), do: true
  defp per_process_connections?(%{order: :task_holds_connection}), do: true
  defp per_process_connections?(_ctx), do: false

  # A public `/v1` socket cancels its turn's response task when it handles its
  # owner's crash first (`maybe_abort_public_owner_turn/2`), so in the natural
  # order the submission may be stopped before it decides anything: then there
  # is no fence line, and the socket's cleanup settles the turn. A native
  # socket waits for its task, and a held socket lets the task decide first.
  defp assert_socket_turn_fence!(log, %{route: "/v1/responses", order: :natural}, turn) do
    case Crash.fence_lines(log) do
      [] -> :ok
      _decided -> Crash.assert_fence_decision!(log, "settled", "socket_turn", turn)
    end
  end

  defp assert_socket_turn_fence!(log, _ctx, turn), do: Crash.assert_fence_decision!(log, "settled", "socket_turn", turn)

  defp post_bridged!(setup, session_header, payload) do
    Phoenix.ConnTest.build_conn()
    |> Plug.Conn.put_req_header("authorization", setup.authorization)
    |> Plug.Conn.put_req_header("x-session-id", session_header)
    |> Phoenix.ConnTest.dispatch(CodexPoolerWeb.Endpoint, :post, "/v1/responses", payload)
  end

  defp connect!(upstream, ctx) do
    setup = gateway_setup(upstream)
    if per_process_connections?(ctx), do: register_unboxed_pool_cleanup!(setup)
    :ok = Crash.serve!(setup, ctx.mode)
    {_server, port} = start_public_endpoint_with_server!()
    window = Scenario.window()
    %{setup: setup, client: Scenario.connect!(port, setup, ctx.route, window), port: port, window: window}
  end

  defp await_executor_proof!(request_id) do
    attempt = Repo.one!(from(a in Attempt, where: a.request_id == ^request_id))
    deadline = System.monotonic_time(:millisecond) + @detection_timeout_ms
    await_executor_proof!(attempt, deadline)
  end

  defp await_executor_proof!(attempt, deadline) do
    cond do
      ExecutionTerminalProofs.terminal?(attempt) ->
        :ok

      System.monotonic_time(:millisecond) >= deadline ->
        flunk("the executor's proof was never published")

      true ->
        receive do
        after
          20 -> await_executor_proof!(attempt, deadline)
        end
    end
  end

  defp settled_cost(request_id) do
    Repo.one!(from(entry in LedgerEntry, where: entry.request_id == ^request_id and entry.entry_kind == "settlement" and entry.amount_status == "recorded", select: entry.settled_cost_micros))
    |> Decimal.normalize()
  end

  defp turn!(client, setup) do
    state = socket_connection_state!(client.socket)
    assert {:ok, owner} = WebsocketOwnerSession.lookup(state.codex_session.id)
    %{owner: owner, session_id: state.codex_session.id, request_id: Crash.turn_request_id!(setup)}
  end
end
