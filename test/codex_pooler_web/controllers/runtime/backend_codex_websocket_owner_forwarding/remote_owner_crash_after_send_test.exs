defmodule CodexPoolerWeb.Runtime.BackendCodexWebsocketOwnerForwarding.RemoteOwnerCrashAfterSendTest do
  # `owner_crash_after_send_test.exs` with the session's owner on another VM,
  # as production runs whenever a turn lands on the web pod that does not own
  # its session (findings#327). The submission that catches the owner's exit
  # runs on the owner's node (`remote_submit_request_v8/3`, called over erpc),
  # beside the owner's upstream session, so the payload write observer moves
  # the same per-request flag there that the frame observer moves, and nothing
  # of it crosses the nodes: a turn the provider received settles
  # `owner_crashed`. A socket's turn whose payload never left settles
  # `owner_crashed` as well: the takeover a replacement owner on the owner's
  # node used to make reached nobody, the socket on this node having closed
  # `1011` on its remote owner's crash, and the client's resend sent the turn
  # to the provider a second time (findings#328). On `/v1` that first send
  # went unrecorded, its request settled `499` at no charge while the provider
  # ran the turn.
  #
  # Two BEAM nodes: this node runs the public listener and the sockets, a
  # second VM sharing the committed database runs the session's owner and its
  # provider connection (the module boots it once). Owner forwarding on, the
  # Pool's serving mode forced to Full or Lite, FakeUpstream on this node, the
  # released client's native frames and a public `/v1` SDK request. The socket
  # and its response task meet the owner's exit in the order the schedulers
  # give, or the socket is held so the task meets it first, or the task also
  # holds a database connection after its settlement while the socket handles
  # the crash.
  use CodexPoolerWeb.ConnCase, async: false

  import Ecto.Query
  import CodexPoolerWeb.Runtime.BackendCodexTestSupport
  import CodexPoolerWeb.Runtime.BackendCodexWebsocketOwnerForwardingSupport, only: [enter_peer_owner_topology!: 0, start_shared_bridge_peer!: 0, start_shared_peer_window_owner!: 3]

  alias CodexPooler.Accounting.{LedgerEntry, RequestClientRetryLink}
  alias CodexPooler.Gateway.Transports.Websocket.WebsocketOwnerSession
  alias CodexPooler.Repo
  alias CodexPoolerWeb.Runtime.OwnerCrashAfterSendScenario, as: Crash
  alias CodexPoolerWeb.Runtime.OwnerLossScenario, as: Scenario
  alias CodexPoolerWeb.Runtime.PeerLogRelay

  @moduletag capture_log: true

  @crashed_close {:close, 1011, "websocket owner crashed"}
  @detection_timeout_ms 15_000

  setup_all do
    %{peer_node: start_shared_bridge_peer!()}
  end

  setup do
    CodexPooler.TestAppEnv.restore_on_exit(:websocket_owner_forwarding_enabled)
    Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, true)
    enter_peer_owner_topology!()
    :ok
  end

  for route <- [Scenario.native_route(), Scenario.public_route()], mode <- ["full", "lite"] do
    @tag route: route, mode: mode
    test "#{route} #{mode}: a turn the provider received settles owner_crashed when its remote owner is killed, and is never sent again", ctx do
      release_ref = make_ref()
      upstream = Crash.upstream!(:after_receipt, "resp_remote_owner_sent", release_ref)
      %{setup: setup, client: client, peer_owner: peer_owner} = connect!(upstream, ctx)
      client = Crash.send_turn!(client, setup, "the turn the provider holds")

      :ok = Crash.await_kill_point!(:after_receipt, upstream, "resp_remote_owner_sent", release_ref)
      assert Crash.generations(upstream) == 1
      request_id = Crash.turn_request_id!(setup)

      :ok = Crash.hold_socket!(client.socket)
      :ok = PeerLogRelay.attach!(ctx.peer_node, Crash.fence_line_prefixes())
      :ok = Crash.kill_owner!(peer_owner.owner_pid)

      assert Crash.await_outcome!(upstream, request_id, 1) == :settled
      assert Crash.generations(upstream) == 1
      :ok = Crash.assert_settled_on_crash!(request_id, peer_owner.session.id)
      log = Crash.await_peer_lines!(ctx.peer_node, 1)
      :ok = Crash.assert_fence_decision!(log, "settled", "payload_started", Crash.turn_correlators(request_id, peer_owner.session.id))
      assert {:error, :owner_unavailable} = :erpc.call(ctx.peer_node, WebsocketOwnerSession, :lookup, [peer_owner.session.id], @detection_timeout_ms)
      assert List.last(Crash.close_client!(client)) == @crashed_close
    end

    for order <- [:natural, :task_first, :task_holds_connection] do
      @tag route: route, mode: mode, order: order
      test "#{route} #{mode} (#{order}): a turn whose payload never left settles owner_crashed when its remote owner is killed, and the client's resend is its one send", ctx do
        publisher = Crash.start_proof_publisher!(:committed)
        release_ref = make_ref()
        upstream = Crash.upstream!(:before_write, "resp_remote_owner_unsent", release_ref)
        %{setup: setup, client: client, peer_owner: peer_owner, port: port, window: window} = connect!(upstream, ctx)
        :ok = Crash.stop_session_owner_on_exit(ctx.peer_node, peer_owner.session.id)
        frame = Crash.frame(setup, client, "the turn whose connection is still opening")
        client = Crash.send_frame!(client, frame)

        :ok = Crash.await_kill_point!(:before_write, upstream, "resp_remote_owner_unsent", release_ref)
        assert Crash.generations(upstream) == 0
        request_id = Crash.turn_request_id!(setup)

        :ok = PeerLogRelay.attach!(ctx.peer_node, Crash.fence_line_prefixes())
        assert List.last(kill_and_close!(ctx, client, peer_owner, upstream, request_id)) == @crashed_close
        assert Process.alive?(publisher)
        # The submission runs on the owner's node, in the erpc worker of the
        # proxy's call, so it decides even when the proxy cancels its task.
        log = Crash.await_peer_lines!(ctx.peer_node, 1)
        :ok = Crash.assert_fence_decision!(log, "settled", "socket_turn", Crash.turn_correlators(request_id, peer_owner.session.id))
        assert {:error, :owner_unavailable} = :erpc.call(ctx.peer_node, WebsocketOwnerSession, :lookup, [peer_owner.session.id], @detection_timeout_ms)
        {served, _answers} = Crash.resend_like_released_client!(port, setup, ctx.route, window, frame)
        assert %{"type" => "response.completed", "response" => %{"id" => "resp_remote_owner_unsent_first_send"}} = served
        :ok = Crash.assert_resend_served_once!(setup, upstream, ctx.route, request_id)
      end
    end
  end

  for mode <- ["full", "lite"] do
    @tag mode: mode, route: Scenario.native_route()
    test "native #{mode}: a sent remote owner's turn admits one proven resend with one measured charge", ctx do
      publisher = Crash.start_proof_publisher!(:committed)
      release_ref = make_ref()
      upstream = Crash.upstream!(:after_receipt, "resp_remote_sent_resend", release_ref)
      %{setup: setup, client: client, peer_owner: peer_owner, port: port, window: window} = connect!(upstream, ctx)
      :ok = Crash.stop_session_owner_on_exit(ctx.peer_node, peer_owner.session.id)
      frame = Crash.frame(setup, client, "the held turn")
      client = Crash.send_frame!(client, frame)
      :ok = Crash.await_kill_point!(:after_receipt, upstream, "resp_remote_sent_resend", release_ref)
      assert Crash.generations(upstream) == 1
      request_id = Crash.turn_request_id!(setup)
      :ok = Crash.hold_socket!(client.socket)
      :ok = Crash.kill_owner!(peer_owner.owner_pid)
      assert Crash.await_outcome!(upstream, request_id, 1) == :settled
      :ok = Crash.assert_settled_on_crash!(request_id, peer_owner.session.id)
      assert List.last(Crash.close_client!(client)) == @crashed_close
      assert Process.alive?(publisher)

      {served, _answers} = Crash.resend_like_released_client!(port, setup, ctx.route, window, frame)
      assert %{"type" => "response.completed", "response" => %{"id" => "resp_remote_sent_resend_written_again"}} = served
      assert [predecessor, successor] = Crash.await_settled!(setup, 2)
      assert predecessor.id == request_id
      assert {predecessor.status, successor.status} == {"failed", "succeeded"}
      assert Crash.generations(upstream) == 2
      assert [%RequestClientRetryLink{successor_request_id: successor_id}] = Repo.all(from(l in RequestClientRetryLink, where: l.predecessor_request_id == ^request_id))
      assert successor_id == successor.id
      assert [zero] = Repo.all(from(e in LedgerEntry, where: e.request_id == ^request_id and e.entry_kind == "settlement" and e.amount_status == "recorded", select: e.settled_cost_micros))
      assert Decimal.equal?(zero, 0)
      assert [%LedgerEntry{total_tokens: 5, settled_cost_micros: cost}] = Repo.all(from(e in LedgerEntry, where: e.request_id == ^successor_id and e.entry_kind == "settlement" and e.amount_status == "recorded"))
      assert Decimal.gt?(cost, 0)
    end
  end

  # The forwarder's payload write observer has run on the owner's node and the
  # payload is not written: the kill comes right after the notification and
  # before the write. Linked, the upstream session goes with its killed owner
  # and never writes; unlinked, it stands for a session that handles its
  # owner's exit only after its write and writes once released. Either way the
  # turn settles `owner_crashed` and nothing sends it again.
  for route <- [Scenario.native_route(), Scenario.public_route()], link <- [:linked, :unlinked] do
    @tag route: route, link: link
    test "#{route} full: a remote owner killed between the payload write observer and the write (#{link}) settles the turn without sending it again", ctx do
      release_ref = make_ref()
      hold_ref = make_ref()
      upstream = Crash.upstream!(:after_receipt, "resp_remote_owner_marked", release_ref)
      setup = gateway_setup(upstream)
      register_unboxed_pool_cleanup!(setup)
      :ok = Crash.serve!(setup, "full")
      window = Scenario.window()
      held = Crash.start_held_owner!(setup, window.id, ctx.peer_node, hold_ref, ctx.link)
      {_server, port} = start_public_endpoint_with_server!()
      client = Scenario.connect!(port, setup, ctx.route, window)
      client = Crash.send_turn!(client, setup, "the turn held at its write")

      assert_receive {:payload_write_held, session_pid, ^hold_ref, :ok}, @detection_timeout_ms
      assert node(session_pid) == ctx.peer_node
      assert Crash.generations(upstream) == 0
      request_id = Crash.turn_request_id!(setup)
      session_monitor = Process.monitor(session_pid)

      :ok = Crash.hold_socket!(client.socket)
      :ok = PeerLogRelay.attach!(ctx.peer_node, Crash.fence_line_prefixes())
      :ok = Crash.kill_owner!(held.owner_pid)
      assert Crash.await_outcome!(upstream, request_id, 0) == :settled

      case ctx.link do
        :linked ->
          assert_receive {:DOWN, ^session_monitor, :process, ^session_pid, :killed}, @detection_timeout_ms
          assert Crash.generations(upstream) == 0

        :unlinked ->
          :ok = Crash.release_held_write(session_pid, hold_ref)
          :ok = Crash.await_kill_point!(:after_receipt, upstream, "resp_remote_owner_marked", release_ref)
          assert Crash.generations(upstream) == 1
          Process.exit(session_pid, :kill)
          assert_receive {:DOWN, ^session_monitor, :process, ^session_pid, :killed}, @detection_timeout_ms
      end

      :ok = Crash.assert_settled_on_crash!(request_id, held.session.id)
      log = Crash.await_peer_lines!(ctx.peer_node, 1)
      :ok = Crash.assert_fence_decision!(log, "settled", "payload_started", Crash.turn_correlators(request_id, held.session.id))
      refute_received {:peer_log, _peer, "upstream websocket payload write refused " <> _fields}
      assert List.last(Crash.close_client!(client)) == @crashed_close
      assert Crash.generations(upstream) == if(ctx.link == :linked, do: 0, else: 1)
    end
  end

  # The remote owner is killed while its upstream session on the owner's node
  # waits right before the payload write observer: the submission there claims
  # the turn and, the turn being a socket's, settles it `owner_crashed`.
  # Linked, the session goes with its owner; unlinked, it reaches the observer
  # once released, finds the turn claimed and ends the request unsent, and the
  # owner's node logs the refused write (findings#329 row J).
  for route <- [Scenario.native_route(), Scenario.public_route()], link <- [:linked, :unlinked] do
    @tag route: route, link: link
    test "#{route} full: a claimed turn's late write on the owner's node is refused (#{link}), and the provider never receives it", ctx do
      release_ref = make_ref()
      hold_ref = make_ref()
      upstream = Crash.upstream!(:after_receipt, "resp_remote_owner_claimed", release_ref)
      setup = gateway_setup(upstream)
      register_unboxed_pool_cleanup!(setup)
      :ok = Crash.serve!(setup, "full")
      window = Scenario.window()
      held = Crash.start_held_owner!(setup, window.id, ctx.peer_node, hold_ref, ctx.link, :before_mark)
      {_server, port} = start_public_endpoint_with_server!()
      client = Scenario.connect!(port, setup, ctx.route, window)
      client = Crash.send_turn!(client, setup, "the turn held before its write")

      assert_receive {:payload_write_held, session_pid, ^hold_ref, :unmarked}, @detection_timeout_ms
      assert node(session_pid) == ctx.peer_node
      request_id = Crash.turn_request_id!(setup)
      session_monitor = Process.monitor(session_pid)
      :ok = Crash.hold_socket!(client.socket)
      :ok = PeerLogRelay.attach!(ctx.peer_node, Crash.fence_line_prefixes())
      :ok = Crash.kill_owner!(held.owner_pid)
      assert Crash.await_outcome!(upstream, request_id, 0) == :settled
      decision = Crash.await_peer_lines!(ctx.peer_node, 1)

      case ctx.link do
        :linked ->
          assert_receive {:DOWN, ^session_monitor, :process, ^session_pid, :killed}, @detection_timeout_ms

        :unlinked ->
          :ok = Crash.release_held_write(session_pid, hold_ref)
          refusal = Crash.await_peer_lines!(ctx.peer_node, 1)
          :ok = Crash.assert_write_refused!(refusal, Crash.turn_correlators(request_id, held.session.id))
          Process.exit(session_pid, :kill)
          assert_receive {:DOWN, ^session_monitor, :process, ^session_pid, :killed}, @detection_timeout_ms
      end

      assert Crash.generations(upstream) == 0
      :ok = Crash.assert_settled_on_crash!(request_id, held.session.id)
      :ok = Crash.assert_fence_decision!(decision, "settled", "socket_turn", Crash.turn_correlators(request_id, held.session.id))
      assert List.last(Crash.close_client!(client)) == @crashed_close
    end
  end

  # Kills the remote owner, in the order the schedulers give, with the socket
  # held so its response task meets the exit first, or also with the task
  # holding a database connection after its settlement while the socket
  # handles the crash, and returns the frames the client read until its
  # socket's Close.
  defp kill_and_close!(%{order: :natural}, client, peer_owner, _upstream, _request_id) do
    :ok = Crash.kill_owner!(peer_owner.owner_pid)
    Crash.await_close!(client)
  end

  defp kill_and_close!(%{order: :task_first}, client, peer_owner, upstream, request_id) do
    :ok = Crash.hold_socket!(client.socket)
    :ok = Crash.kill_owner!(peer_owner.owner_pid)
    assert Crash.await_outcome!(upstream, request_id, 0) == :settled
    :ok = Crash.assert_settled_on_crash!(request_id, peer_owner.session.id)
    Crash.close_client!(client)
  end

  defp kill_and_close!(%{order: :task_holds_connection, route: route}, client, peer_owner, _upstream, request_id),
    do: Crash.kill_with_task_holding_connection!(client, peer_owner.owner_pid, route, request_id, peer_owner.session.id)

  defp connect!(upstream, ctx) do
    setup = gateway_setup(upstream)
    :ok = Crash.serve!(setup, ctx.mode)
    window = Scenario.window()
    peer_owner = start_shared_peer_window_owner!(setup, window.id, ctx.peer_node)
    {_server, port} = start_public_endpoint_with_server!()
    %{setup: setup, client: Scenario.connect!(port, setup, ctx.route, window), peer_owner: peer_owner, port: port, window: window}
  end
end
