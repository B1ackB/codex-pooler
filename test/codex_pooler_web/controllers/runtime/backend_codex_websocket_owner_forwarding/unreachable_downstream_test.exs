defmodule CodexPoolerWeb.Runtime.BackendCodexWebsocketOwnerForwarding.UnreachableDownstreamTest do
  # A websocket owner whose downstream's node becomes unreachable while the
  # turn it asked for is still generating (findings#286). The turn's executor
  # ran on that node, so nobody can receive its output or settle it:
  #
  #   * on a partition, that node's socket closes 1011, interrupts the turn
  #     `owner_crashed` and releases the owner's lease, and the client's resend
  #     is served by a new owner there once the proxy executor's end is
  #     durable;
  #   * when that node dies, nobody interrupts or releases anything, and the
  #     resend of a turn that showed output meets `409 duplicate_turn` until
  #     recovery settles the turn.
  #
  # The owner used to keep generating a turn that showed output to the end: a
  # second generation of the same turn that nobody received or recorded. On
  # DOWN `:noconnection` of its downstream it now cancels such a turn at once:
  # the upstream request's caller exits and the upstream session closes the
  # request (`request_caller_down`). A turn that showed nothing stays `:lost`
  # for the resend that can still rejoin it, and a replay under way keeps its
  # turn, as for any other downstream loss. The owner then checks its lease a
  # write budget after the DOWN and once more after another: after a
  # partition the socket's node has released it, and the owner stops with
  # the `:lost` turn; after a node death both checks renew it. A `:lost` turn
  # that shows output with nobody reattached can no longer be rejoined, and
  # the owner cancels it there (findings#290).
  #
  # Owner forwarding on, native route, the Pool's default serving mode, the
  # released client's frames. FakeUpstream on this node holds the turn at a
  # frame barrier and, once the node is cut off, releases one frame every 50 ms,
  # the provider going on generating.
  #   * Partition: the owner on a peer VM with a TCP control connection, the
  #     socket on this node; cookie swap and disconnect, healed at the end.
  #   * Node death: the socket on a peer VM running the whole application with
  #     its public listener, the owner on this node; that VM is halted, and the
  #     client's resend comes through this node's listener.
  use CodexPoolerWeb.ConnCase, async: false

  import Ecto.Query
  import CodexPoolerWeb.Runtime.BackendCodexTestSupport
  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport, only: [completed_response_frames: 4, receive_frames_until_close!: 3, receive_native_terminal!: 3, released_client_frame: 2, socket_connection_state!: 1, with_info_log: 1]
  import CodexPoolerWeb.Runtime.BackendCodexWebsocketOwnerForwardingSupport, only: [ensure_test_distribution_started!: 0, start_shared_peer_window_owner!: 3]
  import CodexPoolerWeb.Runtime.UnreachableNodeSupport

  alias CodexPooler.Accounting
  alias CodexPooler.Accounting.{Attempt, LedgerEntry, Request, RequestClientRetryLink, RequestLifecycle}
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway.Persistence.{BridgeOwnerLease, CodexSession, CodexTurn}
  alias CodexPooler.Gateway.Transports.Websocket.UpstreamWebsocketSession
  alias CodexPooler.Gateway.Transports.Websocket.WebsocketOwnerSession
  alias CodexPooler.Platform.{ExecutionTerminalProof, ExecutionTerminalProofs, ForwardedGenerationEnd, InstanceHeartbeat, InstancePresence}
  alias CodexPooler.Repo
  alias CodexPooler.UnboxedFixture
  alias CodexPoolerWeb.Runtime.OwnerLossScenario, as: Scenario
  alias CodexPoolerWeb.Runtime.UnreachableNodeSupport
  alias Ecto.Adapters.SQL.Sandbox

  @moduletag capture_log: true

  @deltas 10
  # A turn that outlasts both lease checks without showing anything: lifecycle
  # events only, as a long pre-output phase, before its output.
  @long_preamble 80
  @pace_ms 50
  @detection_timeout_ms 15_000
  # Frames the provider may still have sent before the owner heard of the cut
  # (at most one pacing interval and the one released at the cut).
  @frames_before_cancel 3
  @liveness_window_s InstancePresence.liveness_window_seconds()

  setup_all do
    ensure_test_distribution_started!()
    {owner_peer, owner_node} = boot_tcp_owner_peer!()
    %{owner_peer: owner_peer, owner_node: owner_node, app_peers: %{previsible: boot_app_peer!(), visible: boot_app_peer!(), late: boot_app_peer!(), reachable: boot_app_peer!(), resend_recovery: boot_app_peer!(), cleanup_recovery: boot_app_peer!()}}
  end

  setup do
    CodexPooler.TestAppEnv.restore_on_exit(:websocket_owner_forwarding_enabled)
    Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, true)
    assert :ok = Sandbox.mode(Repo, :auto)
    on_exit(fn -> Sandbox.mode(Repo, :manual) end)
    :ok
  end

  describe "a partition cuts the socket's node off from the owner's" do
    # Once the cut reaches this node, two finalizations race to settle the
    # turn (findings#270 row 270-349): the socket's crash cleanup interrupts it
    # (499 `owner_crashed`, turn interrupted), or the socket's response task,
    # whose forward to the cut owner failed, fails it with that answer (503
    # `owner_unavailable`, turn failed). A loaded run once landed in the second
    # shape where the arm expected the first. Each order is now forced: the
    # response task is held until the crash cleanup settled the turn, or the
    # socket is held until the task did. The released client closes on the
    # socket's 1011 and resends the turn whole; once the proof of the
    # attempt's executor's end exists the resend is served once in both
    # shapes. Against the task's shape it met `409 duplicate_turn`.
    for order <- [:socket_first, :task_first] do
      @tag shown: :visible, order: order
      @tag slow: "cuts a peer VM's owner off mid-turn, forces which finalization settles the turn and resends it"
      test "#{order}: the owner cancels a turn that showed output at once, and the client's resend of the settled turn is served once", ctx do
        proofs_before = Repo.all(from(proof in ExecutionTerminalProof, select: proof.execution_id))
        :ok = register_proof_cleanup!(proofs_before)
        _publisher = CodexPooler.ExecutionProofSupport.start_publisher!()
        turn = start_turn!(ctx, :partition, successor: true)
        %{tasks: tasks} = socket_connection_state!(turn.client.socket)
        [task] = MapSet.to_list(tasks)
        held = hold_finalizer!(turn.client.socket, task, ctx.order)

        on_exit(fn -> heal!(ctx.owner_node) end)
        partition!(ctx.owner_node)

        # The owner cancels the turn at its downstream's DOWN, and its
        # upstream session closes the turn's connection; the provider's next
        # frame finds it closed. An owner that kept the turn goes on at the
        # provider's pace.
        handled = await_peer_state!(ctx.owner_peer, turn.owner, &downstream_handled?/1, "the owner never handled its downstream's DOWN")

        if match?(%{active_turn: nil}, handled) do
          :ok = await_peer_upstream_connection_closed!(ctx.owner_peer, handled.upstream_pid)
          :ok = step!(turn.pacer)
        end

        :ok = pace!(turn.pacer, @pace_ms)
        assert_turn_cancelled_at_once!(turn.pacer)
        owner_state = peer_owner_state(ctx.owner_peer, turn.owner)
        assert owner_state == :stopped or match?(%{active_turn: nil}, owner_state)

        settled = await_state!(fn -> request_outcome(turn.request_id) end, &match?({"failed", _, _}, &1), "the turn was never settled", System.monotonic_time(:millisecond) + @detection_timeout_ms)
        :ok = release_finalizer!(held)
        {conn, _websocket, frames} = receive_frames_until_close!(turn.client.conn, turn.client.websocket, turn.client.ref)
        Mint.HTTP.close(conn)
        assert List.last(frames) == {:close, 1011, "websocket owner crashed"}

        # The finalization that came first settled the turn; the other found it
        # settled. The socket's node released the cut owner's lease either way.
        {request_shape, turn_shape} = settled_shape(ctx.order)
        assert settled == request_shape
        assert request_outcome(turn.request_id) == request_shape
        assert turn_shape == Repo.get_by!(CodexTurn, request_id: turn.request_id) |> Map.take([:status, :error_code])
        assert [%BridgeOwnerLease{status: "released", owner_instance_id: cut_owner_instance}] = leases(turn.session_id)
        assert cut_owner_instance == Atom.to_string(ctx.owner_node)
        assert FakeUpstream.count(turn.upstream) == 2

        # The cut owner recorded the end of the generation it served.
        assert %{reason: "unreachable_downstream_cancelled", owner_instance_id: ending_owner} = await_generation_end!(turn.request_id)
        assert ending_owner == Atom.to_string(ctx.owner_node)

        # Once the proof of the attempt's executor's end exists, the client's
        # resend is served once, linked to the settled turn, which stays at no
        # charge while the successor pays its usage.
        :ok = await_executor_proof!(turn.request_id)
        retry = Scenario.connect!(turn.port, turn.setup, Scenario.native_route(), turn.window)
        {conn, websocket} = public_websocket_send_text!(retry.conn, retry.websocket, retry.ref, turn.frame)
        {conn, websocket, served} = receive_native_terminal!(conn, websocket, retry.ref)
        assert %{"type" => "response.completed", "response" => %{"id" => "resp_unreachable_successor"}} = served
        Scenario.close!(%{retry | conn: conn, websocket: websocket})
        assert [%RequestClientRetryLink{successor_request_id: successor_id}] = Repo.all(from(link in RequestClientRetryLink, where: link.predecessor_request_id == ^turn.request_id))
        # The served resend's response task settles it after its terminal frame reached the client.
        settled_successor = await_state!(fn -> request_outcome(successor_id) end, &(elem(&1, 0) not in ["accepted", "in_progress"]), "the served resend was never settled", System.monotonic_time(:millisecond) + @detection_timeout_ms)
        assert {"succeeded", 200, nil} == settled_successor
        assert settled_cost(turn.request_id) == Decimal.new(0)
        assert Decimal.gt?(settled_cost(successor_id), 0)
        assert FakeUpstream.count(turn.upstream) == 3
      end
    end

    # Without the proof of the executor's end the task's shape admits nothing:
    # the executor might still settle the attempt.
    @tag shown: :visible
    @tag slow: "cuts a peer VM's owner off mid-turn, lets the response task settle the turn and resends it with no execution proof published"
    test "task_first: the resend of a turn the response task settled is refused without the proof of its executor's end", ctx do
      turn = start_turn!(ctx, :partition, successor: true)
      %{tasks: tasks} = socket_connection_state!(turn.client.socket)
      [task] = MapSet.to_list(tasks)
      held = hold_finalizer!(turn.client.socket, task, :task_first)

      on_exit(fn -> heal!(ctx.owner_node) end)
      partition!(ctx.owner_node)
      settled = await_state!(fn -> request_outcome(turn.request_id) end, &match?({"failed", _, _}, &1), "the turn was never settled", System.monotonic_time(:millisecond) + @detection_timeout_ms)
      :ok = release_finalizer!(held)
      {conn, _websocket, frames} = receive_frames_until_close!(turn.client.conn, turn.client.websocket, turn.client.ref)
      Mint.HTTP.close(conn)
      assert List.last(frames) == {:close, 1011, "websocket owner crashed"}
      assert settled == {"failed", 503, "owner_unavailable"}
      refute ExecutionTerminalProofs.terminal?(attempt(turn.request_id))

      retry = Scenario.connect!(turn.port, turn.setup, Scenario.native_route(), turn.window)
      {conn, websocket} = public_websocket_send_text!(retry.conn, retry.websocket, retry.ref, turn.frame)
      {conn, websocket, refused} = receive_native_terminal!(conn, websocket, retry.ref)
      assert %{"type" => "error", "status" => 409, "error" => %{"code" => "duplicate_turn"}} = refused
      Scenario.close!(%{retry | conn: conn, websocket: websocket})
      assert Repo.all(from(link in RequestClientRetryLink, where: link.predecessor_request_id == ^turn.request_id)) == []
      assert FakeUpstream.count(turn.upstream) == 2
    end

    @tag shown: :previsible
    @tag slow: "cuts a peer VM's owner off mid-turn and waits for its lease check to stop it"
    test "the owner keeps a turn that showed nothing until its lease check finds the lease released", ctx do
      turn = start_turn!(ctx, :partition, deltas: 2, preamble: @long_preamble)
      probe = start_lease_check_probe!(ctx.owner_peer, turn.owner)

      on_exit(fn -> heal!(ctx.owner_node) end)
      partition!(ctx.owner_node)
      _lost = await_lease_check_probe!(ctx.owner_peer, probe, & &1.lost?, "the owner did not keep the turn for a resend")

      {conn, _websocket, frames} = receive_frames_until_close!(turn.client.conn, turn.client.websocket, turn.client.ref)
      Mint.HTTP.close(conn)
      assert List.last(frames) == {:close, 1011, "websocket owner crashed"}
      :ok = await_lease_released!(turn.session_id)

      # Hold the provider before output so lease checks, not a paced first delta,
      # decide this turn. The first actual early check must stop the owner.
      :ok = await_peer_owner_stopped!(ctx.owner_peer, turn.owner, System.monotonic_time(:millisecond) + @detection_timeout_ms)
      assert %{started: [1], completed: []} = peer_lease_check_probe(ctx.owner_peer, probe)
      # A held FakeUpstream handler reads no socket closure until its barrier is
      # released. Pace only after owner termination, so provider output cannot
      # be the cause of cancellation; its next push observes the closed socket.
      :ok = pace!(turn.pacer, @pace_ms)
      %{frames: consumed} = await_connection_down!(turn.pacer)
      assert consumed <= @frames_before_cancel
    end

    @tag shown: :previsible
    @tag slow: "cuts a peer VM's owner off mid-turn, delays the socket node's release past the first lease check and waits for the second"
    test "a lease the socket's node releases after the first check is found by the second", ctx do
      turn = start_turn!(ctx, :partition, deltas: 2, preamble: @long_preamble)
      [lease] = leases(turn.session_id)
      probe = start_lease_check_probe!(ctx.owner_peer, turn.owner, hold_first: true)

      # The socket's node is slow to release: its socket takes the owner's
      # DOWN only after the owner's first check.
      :ok = :sys.suspend(turn.client.socket)
      on_exit(fn -> resume_if_alive(turn.client.socket) end)
      on_exit(fn -> heal!(ctx.owner_node) end)
      partition!(ctx.owner_node)
      _held = await_lease_check_probe!(ctx.owner_peer, probe, & &1.held?, "the owner's first lease check never completed")

      # The post-handler barrier observes the real first renewal and holds the
      # owner there. A database expiry read alone can race its next check or a
      # different lease write, and a state call can meet termination mid-cleanup.
      assert %{started: [1], completed: [1], held?: true, lost?: true} = peer_lease_check_probe(ctx.owner_peer, probe)
      current = Repo.get!(BridgeOwnerLease, lease.id)
      assert %{status: "active", lease_token: token} = current
      assert token == lease.lease_token
      assert DateTime.compare(current.expires_at, lease.expires_at) == :gt

      :ok = :sys.resume(turn.client.socket)
      :ok = await_lease_released!(turn.session_id)
      :ok = :peer.call(ctx.owner_peer, UnreachableNodeSupport, :release_lease_check_probe, [probe], @detection_timeout_ms)
      :ok = await_peer_owner_stopped!(ctx.owner_peer, turn.owner, System.monotonic_time(:millisecond) + @detection_timeout_ms)
      assert %{started: [1, 0], completed: [1]} = peer_lease_check_probe(ctx.owner_peer, probe)
      :ok = pace!(turn.pacer, @pace_ms)
      %{frames: consumed} = await_connection_down!(turn.pacer)
      assert consumed <= @frames_before_cancel
      Mint.HTTP.close(turn.client.conn)
    end

    @tag slow: "arms a replay, starts it on a second socket, then cuts the peer VM's owner off while the provider paces"
    test "a replay under way keeps its turn", ctx do
      retry_ref = make_ref()
      retry_pacer = start_pacer!(retry_ref)
      replay_ref = make_ref()
      pacer = start_pacer!(replay_ref)

      upstream =
        start_upstream(
          # provenance: synthetic_adversarial (a client-retry turn armed for replay when its socket closed, replayed on a socket whose node is then cut off)
          FakeUpstream.repeat_last([
            completed_response_frames("resp_unreachable_one", [], 3, 2),
            FakeUpstream.websocket_terminal_failure("server_error"),
            FakeUpstream.barrier_websocket_frames(turn_frames(), notify: retry_pacer, release_ref: retry_ref),
            FakeUpstream.barrier_websocket_frames(turn_frames(), notify: pacer, release_ref: replay_ref)
          ])
        )

      :ok = pace_upstream!(retry_pacer, upstream)
      :ok = pace_upstream!(pacer, upstream)
      setup = gateway_setup(upstream)
      window = Scenario.window()
      owner = start_shared_peer_window_owner!(setup, window.id, ctx.owner_node).owner_pid
      {_server, port} = start_public_endpoint_with_server!()
      client = Scenario.connect!(port, setup, Scenario.native_route(), window)
      {client, _one} = Scenario.turn!(client, setup, "turn one")
      frame = released_client_frame(setup, window.thread).(native_text_input("the turn the provider failed"), Ecto.UUID.generate(), %{})
      {conn, websocket} = public_websocket_send_text!(client.conn, client.websocket, client.ref, frame)
      {conn, websocket, failed} = receive_native_terminal!(conn, websocket, client.ref)
      assert %{"type" => "response.failed"} = failed

      # The client's retry, closed before it showed anything: its replay is armed.
      {conn, _websocket} = public_websocket_send_text!(conn, websocket, client.ref, frame)
      :ok = await_frame_barrier!(retry_pacer, 0)
      Scenario.close!(%{client | conn: conn})
      _armed = await_peer_state!(ctx.owner_peer, owner, &match?(%{active_turn: nil, suspended_replay: %{provisional_status: :armed}}, &1), "the replay was never armed")

      # The client's resend on another socket starts the replay, which shows output.
      replay = Scenario.connect!(port, setup, Scenario.native_route(), window)
      {conn, websocket} = public_websocket_send_text!(replay.conn, replay.websocket, replay.ref, frame)
      :ok = await_frame_barrier!(pacer, 0)
      :ok = release_frames!(pacer, 2)
      {conn, websocket, _created} = receive_text!(conn, websocket, replay.ref, "response.created")
      {conn, _websocket, _delta} = receive_text!(conn, websocket, replay.ref, "response.output_text.delta")
      assert %{active_turn: %{visible_output?: true}, suspended_replay: %{provisional_status: :started}} = :peer.call(ctx.owner_peer, :sys, :get_state, [owner])

      on_exit(fn -> heal!(ctx.owner_node) end)
      partition!(ctx.owner_node)
      :ok = pace!(pacer, @pace_ms)
      detached = await_peer_state!(ctx.owner_peer, owner, &is_nil(&1.downstream), "the owner kept its cut-off downstream")

      # The replay's turn goes on as before, until the owner's lease check.
      assert %{active_turn: %{}, suspended_replay: %{provisional_status: :started}} = detached
      assert %{frames: frames} = await_frames!(pacer, @frames_before_cancel + 2)
      assert frames >= @frames_before_cancel + 2
      Mint.HTTP.close(conn)
    end
  end

  describe "the socket's node dies" do
    @tag shown: :visible
    @tag slow: "halts the peer VM running the socket mid-turn and paces the provider until the turn's connection closes"
    test "the owner cancels a turn that showed output at once and keeps its lease", ctx do
      turn = start_turn!(ctx, :death)
      lease_token = Repo.get!(CodexSession, turn.session_id).owner_lease_token

      {_cancelled, log} =
        with_info_log(fn ->
          :ok = halt!(turn.app_peer.peer, turn.app_peer.node)
          :ok = pace!(turn.pacer, @pace_ms)
          assert_turn_cancelled_at_once!(turn.pacer)
        end)

      assert log =~ "websocket owner cancelled the turn of an unreachable downstream"
      assert %{active_turn: nil} = :sys.get_state(turn.owner)
      assert WebsocketOwnerSession.lookup(turn.session_id) == {:ok, turn.owner}

      # Nobody on the dead node interrupted or released anything: the turn
      # waits for recovery, under the owner's lease.
      assert {"in_progress", nil, nil} == request_outcome(turn.request_id)
      assert %CodexTurn{status: "in_progress"} = Repo.get_by!(CodexTurn, request_id: turn.request_id)
      assert [%BridgeOwnerLease{status: "active", lease_token: ^lease_token}] = leases(turn.session_id)
      assert FakeUpstream.count(turn.upstream) == 2
      assert %{reason: "unreachable_downstream_cancelled", owner_instance_id: ending_owner} = await_generation_end!(turn.request_id)
      assert ending_owner == Atom.to_string(node())
    end

    @tag shown: :previsible
    @tag slow: "halts the peer VM running the socket mid-turn, waits through both lease checks and streams the whole paced turn to the resend"
    test "the owner keeps a turn that showed nothing, and the client's resend reattaches to it", ctx do
      turn = start_turn!(ctx, :death)
      [lease] = leases(turn.session_id)
      probe = start_lease_check_probe!(nil, turn.owner)

      :ok = halt!(turn.app_peer.peer, turn.app_peer.node)
      _lost = await_owner_state!(turn.owner, &lost_turn?/1, "the owner did not keep the turn for a resend")

      # Nobody released the lease: observe both actual handlers after the DOWN,
      # rather than charging their database and scheduler latency to the cut.
      _checks = await_lease_check_probe!(nil, probe, &(&1.completed == [1, 0]), "the owner's two early lease checks did not complete")
      current = Repo.get!(BridgeOwnerLease, lease.id)
      assert %{status: "active", lease_token: token} = current
      assert token == lease.lease_token
      assert DateTime.compare(current.expires_at, lease.expires_at) == :gt
      assert %{active_turn: %{descriptor: %{downstream_status: :lost}}} = :sys.get_state(turn.owner)

      # The released client's resend reaches this node before the turn's first
      # output, and rejoins the one generation.
      retry = Scenario.connect!(turn.port, turn.setup, Scenario.native_route(), turn.window)
      {conn, websocket} = public_websocket_send_text!(retry.conn, retry.websocket, retry.ref, turn.frame)
      _reattached = await_owner_state!(turn.owner, &match?(%{active_turn: %{descriptor: %{downstream_status: :attached}}}, &1), "the resend never reattached")
      :ok = pace!(turn.pacer, 10)
      {conn, websocket, events} = receive_turn!(conn, websocket, retry.ref, [])
      assert events == [{"response.created", "resp_unreachable_turn"} | List.duplicate({"response.output_text.delta", nil}, @deltas)] ++ [{"response.completed", "resp_unreachable_turn"}]
      assert FakeUpstream.count(turn.upstream) == 2
      # The terminal went to the resend, never to the attempt's executor.
      assert %{reason: "terminal_delivered_to_reattached"} = await_generation_end!(turn.request_id)
      Scenario.close!(%{retry | conn: conn, websocket: websocket})
    end

    @tag shown: :previsible, app_peer: :late
    @tag slow: "halts the peer VM running the socket mid-turn and paces the provider to the turn's first output"
    test "the owner stops a turn that showed nothing once it shows output with nobody reattached", ctx do
      turn = start_turn!(ctx, :death)
      lease_token = Repo.get!(CodexSession, turn.session_id).owner_lease_token

      {_cancelled, log} =
        with_info_log(fn ->
          :ok = halt!(turn.app_peer.peer, turn.app_peer.node)
          _lost = await_owner_state!(turn.owner, &lost_turn?/1, "the owner did not keep the turn for a resend")

          # No resend comes before the turn's output (created, then its first
          # delta). The provider sends the two, then waits for the owner to
          # handle that first output: it went on at a fixed pace while the
          # owner committed the turn's visibility, and a loaded run released a
          # fourth frame first (findings#270 row 270-343). Once the owner
          # cancelled the turn and its upstream session closed the connection,
          # the provider's next frame finds the connection closed; a turn the
          # owner kept goes on at the provider's pace.
          :ok = step!(turn.pacer)
          :ok = step!(turn.pacer)
          handled = await_owner_state!(turn.owner, &first_output_handled?/1, "the owner never handled the turn's first output")

          if is_nil(handled.active_turn) do
            :ok = await_upstream_connection_closed!(handled.upstream_pid)
            :ok = step!(turn.pacer)
          end

          :ok = pace!(turn.pacer, @pace_ms)
          assert_turn_cancelled_at_once!(turn.pacer)
        end)

      assert log =~ "websocket owner cancelled a lost turn of an unreachable downstream at its first output"
      assert %{active_turn: nil} = :sys.get_state(turn.owner)
      assert %{reason: "lost_turn_cancelled_at_output"} = await_generation_end!(turn.request_id)

      # The turn showed output, so it waits for recovery under the owner's
      # lease, and the client's late resend meets the refusal it met before.
      assert {"in_progress", nil, nil} == request_outcome(turn.request_id)
      assert %CodexTurn{status: "in_progress", first_visible_output_at: %DateTime{}} = Repo.get_by!(CodexTurn, request_id: turn.request_id)
      assert [%BridgeOwnerLease{status: "active", lease_token: ^lease_token}] = leases(turn.session_id)
      retry = Scenario.connect!(turn.port, turn.setup, Scenario.native_route(), turn.window)
      {conn, websocket} = public_websocket_send_text!(retry.conn, retry.websocket, retry.ref, turn.frame)
      {conn, websocket, refused} = receive_native_terminal!(conn, websocket, retry.ref)
      assert %{"type" => "error", "status" => 409} = refused
      assert FakeUpstream.count(turn.upstream) == 2
      Scenario.close!(%{retry | conn: conn, websocket: websocket})
    end
  end

  describe "the socket's node is replaced by one under another name" do
    # The dead node's presence row passes the liveness window: 121 s are
    # taken off it (and off the attempt, which a recovery pass also ages),
    # instead of waiting. Nothing comes back under its node name or slot, so
    # the owner's record of the generation's end is the only death evidence.
    @tag shown: :visible, app_peer: :resend_recovery
    @tag slow: "halts the peer VM running the socket mid-turn and resends once its presence passed the liveness window"
    test "the client's resend is admitted once the dead node's presence passed the liveness window", ctx do
      turn = replaced_socket_node_turn!(ctx)

      retry = Scenario.connect!(turn.port, turn.setup, Scenario.native_route(), turn.window)
      {conn, websocket} = public_websocket_send_text!(retry.conn, retry.websocket, retry.ref, turn.frame)
      {conn, websocket, served} = receive_native_terminal!(conn, websocket, retry.ref)
      assert %{"type" => "response.completed", "response" => %{"id" => "resp_unreachable_successor"}} = served
      Scenario.close!(%{retry | conn: conn, websocket: websocket})
      assert_recovered!(turn)
    end

    @tag shown: :visible, app_peer: :cleanup_recovery
    @tag slow: "halts the peer VM running the socket mid-turn and runs absent-instance recovery once its presence passed the liveness window"
    test "absent-instance recovery settles the turn once the dead node's presence passed the liveness window", ctx do
      turn = replaced_socket_node_turn!(ctx)
      age_attempt!(turn.request_id, @liveness_window_s + 1)

      assert {:ok, %{absent_instance_attempts_recovered: 1}} = Accounting.recover_absent_instance_attempts(InstancePresence.database_now())
      assert_recovered!(turn)
    end
  end

  describe "the socket exits on a node the owner still reaches" do
    @tag shown: :previsible, app_peer: :reachable
    @tag slow: "kills the socket on a peer VM mid-turn and paces the provider past the turn's first output"
    test "the owner keeps generating a turn that showed nothing past its first output", ctx do
      assert_reachable_downstream_turn!(ctx, false)
    end

    @tag shown: :previsible, app_peer: :reachable
    @tag slow: "holds the owned upstream consumer while the provider advances after the reachable socket dies"
    test "provider progress can precede the reachable owner's visible output", ctx do
      assert_reachable_downstream_turn!(ctx, true)
    end
  end

  defp assert_reachable_downstream_turn!(ctx, hold_upstream?) do
    turn = start_turn!(ctx, :death)
    %{downstream: %{pid: socket}} = :sys.get_state(turn.owner)
    assert node(socket) == turn.app_peer.node

    Process.exit(socket, :kill)
    _lost = await_owner_state!(turn.owner, &lost_turn?/1, "the owner did not keep the turn for a resend")
    upstream = if hold_upstream?, do: hold_upstream_consumer!(turn.owner)
    :ok = pace!(turn.pacer, @pace_ms)

    # That node's task can still settle the turn: the owner goes on as
    # before, output and all.
    assert %{frames: frames, connection_down_at: nil} = await_frames!(turn.pacer, @frames_before_cancel + 3)
    assert frames >= @frames_before_cancel + 3

    if hold_upstream? do
      assert match?(%{active_turn: %{visible_output?: false}}, :sys.get_state(turn.owner))
      :ok = resume_task(upstream)
    end

    visible = await_owner_state!(turn.owner, &match?(%{active_turn: %{visible_output?: true}}, &1), "the owner did not observe the provider's visible output")
    assert %{active_turn: %{visible_output?: true}} = visible
    refute Repo.get(ForwardedGenerationEnd, attempt(turn.request_id).id)

    # And it does, once the provider ends the turn and nobody received it.
    # Awaited here, before the Pool's rows go: the test used to end with the
    # turn still `in_progress`, and that task's settlement then raised
    # `Ecto.NoResultsError` in `lock_finalization_rows/2` (findings#270 row
    # 270-288).
    assert {{"failed", 503, "owner_unavailable"}, {"interrupted", "owner_unavailable"}} == await_turn_settled!(turn.request_id)

    # The owner received the provider's `response.completed` and its usage
    # but had nobody to deliver it to. Its answer to that node's task keeps
    # the terminal, so the settlement records the usage the provider
    # reported (5 in, 10 out) instead of `usage_unknown` at the
    # reservation's estimate (findings#270 row 270-293).
    assert %Request{usage_status: "usage_known"} = Repo.get!(Request, turn.request_id)
    assert settled_usage(turn.request_id) == {"usage_known", 5, 10, 15}
  end

  defp hold_upstream_consumer!(owner) do
    upstream = :sys.get_state(owner).upstream_pid
    assert node(upstream) == node()
    on_exit(fn -> resume_task(upstream) end)
    true = :erlang.suspend_process(upstream)
    upstream
  end

  # A socket whose turn the owner runs and FakeUpstream holds before any
  # frame (with `:visible`, after the created event and one delta the client
  # received). `:partition`: the owner on the TCP-controlled peer, the socket
  # here. `:death`: the owner here, the socket on the application peer (the
  # test's `app_peer` tag, or its `shown` one). `deltas:` sets the turn's
  # output, `preamble:` the lifecycle events before it, and `successor: true`
  # serves any later request at once.
  defp start_turn!(ctx, topology, opts \\ []) do
    release_ref = make_ref()
    pacer = start_pacer!(release_ref)
    frames = turn_frames(Keyword.get(opts, :deltas, @deltas), Keyword.get(opts, :preamble, 0))
    successor = if Keyword.get(opts, :successor, false), do: [completed_response_frames("resp_unreachable_successor", [], 3, 2)], else: []

    upstream =
      start_upstream(
        # provenance: synthetic_adversarial (a turn whose downstream's node is cut off or dies while the provider still generates it)
        FakeUpstream.repeat_last(
          [
            completed_response_frames("resp_unreachable_one", [], 3, 2),
            FakeUpstream.barrier_websocket_frames(frames, notify: pacer, release_ref: release_ref)
          ] ++ successor
        )
      )

    :ok = pace_upstream!(pacer, upstream)
    setup = gateway_setup(upstream)
    window = Scenario.window()
    {_server, port} = start_public_endpoint_with_server!()

    {client, owner, app_peer} =
      case topology do
        :partition ->
          # Registers the Pool's cleanup too.
          peer_owner = start_shared_peer_window_owner!(setup, window.id, ctx.owner_node)
          client = Scenario.connect!(port, setup, Scenario.native_route(), window)
          {client, _one} = Scenario.turn!(client, setup, "turn one")
          {client, peer_owner.owner_pid, nil}

        :death ->
          register_unboxed_pool_cleanup!(setup)
          first = Scenario.connect!(port, setup, Scenario.native_route(), window)
          {first, _one} = Scenario.turn!(first, setup, "turn one")
          owner = socket_connection_state!(first.socket).websocket_owner_pid
          Scenario.close!(first)
          app_peer = Map.fetch!(ctx.app_peers, Map.get(ctx, :app_peer, ctx.shown))
          {conn, websocket, ref, _headers} = public_websocket_connect_with_request_headers!(app_peer.port, setup, Ecto.UUID.generate(), Scenario.native_route(), [{"x-codex-window-id", window.id}])
          {%{conn: conn, websocket: websocket, ref: ref}, owner, app_peer}
      end

    frame = released_client_frame(setup, window.thread).(native_text_input("the turn whose downstream's node goes"), Ecto.UUID.generate(), %{})
    {conn, websocket} = public_websocket_send_text!(client.conn, client.websocket, client.ref, frame)
    :ok = await_frame_barrier!(pacer, 0)

    {conn, websocket} =
      if ctx.shown == :visible do
        :ok = release_frames!(pacer, 2)
        {conn, websocket, _created} = receive_text!(conn, websocket, client.ref, "response.created")
        {conn, websocket, _delta} = receive_text!(conn, websocket, client.ref, "response.output_text.delta")
        {conn, websocket}
      else
        {conn, websocket}
      end

    session_id = Repo.one!(from(s in CodexSession, where: s.pool_id == ^setup.pool.id, select: s.id))
    request_id = Repo.one!(from(r in Request, where: r.pool_id == ^setup.pool.id and r.status == "in_progress", select: r.id))

    %{
      pacer: pacer,
      upstream: upstream,
      setup: setup,
      window: window,
      port: port,
      client: %{client | conn: conn, websocket: websocket},
      owner: owner,
      app_peer: app_peer,
      frame: frame,
      session_id: session_id,
      request_id: request_id
    }
  end

  # The owner stopped the provider's generation right after its DOWN: the held
  # connection consumed at most the frames already on their way, and closed.
  defp assert_turn_cancelled_at_once!(pacer) do
    deadline = System.monotonic_time(:millisecond) + @detection_timeout_ms
    %{frames: frames, connection_down_at: down_at} = await_pacer!(pacer, &(is_integer(&1.connection_down_at) or &1.frames > @frames_before_cancel), deadline)
    assert is_integer(down_at)
    assert frames <= @frames_before_cancel, "the owner went on generating: #{frames} frames after the cut"
  end

  defp await_connection_down!(pacer) do
    deadline = System.monotonic_time(:millisecond) + @detection_timeout_ms
    report = await_pacer!(pacer, &is_integer(&1.connection_down_at), deadline)
    assert is_integer(report.connection_down_at), "the provider's connection stayed open"
    report
  end

  defp await_frames!(pacer, count) do
    deadline = System.monotonic_time(:millisecond) + @detection_timeout_ms
    report = await_pacer!(pacer, &(&1.frames >= count or not is_nil(&1.connection_down_at)), deadline)
    assert report.frames >= count or not is_nil(report.connection_down_at)
    report
  end

  defp await_pacer!(pacer, done?, deadline) do
    report = consumed_after_pace(pacer)

    if done?.(report) or System.monotonic_time(:millisecond) >= deadline do
      report
    else
      Process.sleep(10)
      await_pacer!(pacer, done?, deadline)
    end
  end

  defp await_owner_state!(owner, predicate, message) do
    deadline = System.monotonic_time(:millisecond) + @detection_timeout_ms
    await_state!(fn -> :sys.get_state(owner) end, predicate, message, deadline)
  end

  defp await_peer_state!(peer, owner, predicate, message) do
    deadline = System.monotonic_time(:millisecond) + @detection_timeout_ms
    await_state!(fn -> :peer.call(peer, :sys, :get_state, [owner]) end, predicate, message, deadline)
  end

  # The first state that satisfies `predicate`.
  defp await_state!(get_state, predicate, message, deadline) do
    state = get_state.()

    cond do
      predicate.(state) ->
        state

      System.monotonic_time(:millisecond) >= deadline ->
        flunk(message)

      true ->
        Process.sleep(10)
        await_state!(get_state, predicate, message, deadline)
    end
  end

  defp lost_turn?(state), do: match?(%{downstream: nil, active_turn: %{descriptor: %{downstream_status: :lost}}}, state)

  # The owner handled its downstream's DOWN: it cancelled the turn, or it let
  # the downstream go and kept the turn.
  defp downstream_handled?(state), do: match?(%{active_turn: nil}, state) or match?(%{downstream: nil}, state)

  # Holds the finalization that must come second: the socket's response task
  # for `:socket_first` (suspended, so its failed forward waits), the socket
  # for `:task_first` (its owner's exit waits).
  defp hold_finalizer!(_socket, task, :socket_first) do
    true = :erlang.suspend_process(task)
    on_exit(fn -> resume_task(task) end)
    {:task, task}
  end

  defp hold_finalizer!(socket, _task, :task_first) do
    :ok = :sys.suspend(socket)
    on_exit(fn -> resume_if_alive(socket) end)
    {:socket, socket}
  end

  defp release_finalizer!({:task, task}), do: resume_task(task)
  defp release_finalizer!({:socket, socket}), do: :sys.resume(socket)

  defp resume_task(task) do
    if Process.alive?(task), do: :erlang.resume_process(task)
    :ok
  catch
    :error, :badarg -> :ok
  end

  defp settled_shape(:socket_first), do: {{"failed", 499, "owner_crashed"}, %{status: "interrupted", error_code: "owner_crashed"}}
  defp settled_shape(:task_first), do: {{"failed", 503, "owner_unavailable"}, %{status: "interrupted", error_code: "owner_unavailable"}}

  # The owner's upstream session, on the cut peer, closed the turn's
  # connection.
  defp await_peer_upstream_connection_closed!(peer, upstream) do
    deadline = System.monotonic_time(:millisecond) + @detection_timeout_ms
    _closed = await_state!(fn -> :peer.call(peer, UpstreamWebsocketSession, :live_connection, [upstream]) end, &match?({:ok, %{generation: nil}}, &1), "the owner's upstream session kept the turn's connection open", deadline)
    :ok
  end

  defp await_executor_proof!(request_id) do
    attempt = attempt(request_id)
    _proven = await_state!(fn -> ExecutionTerminalProofs.terminal?(attempt) end, & &1, "the executor's proof was never published", System.monotonic_time(:millisecond) + @detection_timeout_ms)
    :ok
  end

  # The publisher commits its proofs, of this test's executions and of any
  # the node's execution registry still holds from earlier ones: every proof
  # the test found absent goes.
  defp register_proof_cleanup!(proofs_before) do
    UnboxedFixture.register_unboxed_cleanup!(fn -> Repo.delete_all(from(proof in ExecutionTerminalProof, where: proof.execution_id not in ^proofs_before)) end)
  end

  defp settled_cost(request_id) do
    Repo.one!(from(entry in LedgerEntry, where: entry.request_id == ^request_id and entry.entry_kind == "settlement" and entry.amount_status == "recorded", select: entry.settled_cost_micros))
    |> Decimal.normalize()
  end

  # The owner handled a turn's first output: it cancelled the turn, or it
  # committed the turn's visibility and went on.
  defp first_output_handled?(state), do: match?(%{active_turn: nil}, state) or match?(%{active_turn: %{visible_output?: true}}, state)

  # The upstream session closed the turn's provider connection: a cancelled
  # request's caller exit closes it (`request_caller_down`). The session
  # answers only once the request it serves has ended.
  defp await_upstream_connection_closed!(upstream) do
    deadline = System.monotonic_time(:millisecond) + @detection_timeout_ms
    _closed = await_state!(fn -> UpstreamWebsocketSession.live_connection(upstream) end, &match?({:ok, %{generation: nil}}, &1), "the owner's upstream session kept the turn's connection open", deadline)
    :ok
  end

  # The owner's state, or `:stopped` once its lease check stopped it.
  defp peer_owner_state(peer, owner) do
    if :peer.call(peer, Process, :alive?, [owner]), do: :peer.call(peer, :sys, :get_state, [owner]), else: :stopped
  catch
    :exit, _reason -> :stopped
  end

  defp peer_lease_check_probe(nil, probe), do: UnreachableNodeSupport.lease_check_probe_state(probe)
  defp peer_lease_check_probe(peer, probe), do: :peer.call(peer, UnreachableNodeSupport, :lease_check_probe_state, [probe], @detection_timeout_ms)

  defp await_lease_check_probe!(peer, probe, predicate, message) do
    await_state!(fn -> peer_lease_check_probe(peer, probe) end, predicate, message, System.monotonic_time(:millisecond) + @detection_timeout_ms)
  end

  defp await_peer_owner_stopped!(peer, owner, deadline) do
    cond do
      not :peer.call(peer, Process, :alive?, [owner]) ->
        :ok

      System.monotonic_time(:millisecond) >= deadline ->
        flunk("the owner went on past its lease checks")

      true ->
        Process.sleep(5)
        await_peer_owner_stopped!(peer, owner, deadline)
    end
  end

  defp await_lease_released!(session_id) do
    deadline = System.monotonic_time(:millisecond) + @detection_timeout_ms
    _released = await_state!(fn -> leases(session_id) end, &match?([%BridgeOwnerLease{status: "released"}], &1), "the socket's node never released the owner's lease", deadline)
    :ok
  end

  defp resume_if_alive(socket) do
    if Process.alive?(socket), do: :sys.resume(socket)
  catch
    :exit, _reason -> :ok
  end

  defp receive_text!(conn, websocket, ref, type) do
    {conn, websocket, text} = public_websocket_receive_text!(conn, websocket, ref)

    case CodexPooler.JSON.decode!(text) do
      %{"type" => ^type} = event -> {conn, websocket, event}
      %{"type" => "codex.response.metadata"} -> receive_text!(conn, websocket, ref, type)
    end
  end

  # The turn's events up to its terminal, as {type, response id}.
  defp receive_turn!(conn, websocket, ref, events) do
    {conn, websocket, text} = public_websocket_receive_text!(conn, websocket, ref)

    case CodexPooler.JSON.decode!(text) do
      %{"type" => "codex.response.metadata"} ->
        receive_turn!(conn, websocket, ref, events)

      %{"type" => type} = event when type in ["response.completed", "response.failed", "error"] ->
        {conn, websocket, Enum.reverse([{type, get_in(event, ["response", "id"])} | events])}

      %{"type" => type} = event ->
        receive_turn!(conn, websocket, ref, [{type, get_in(event, ["response", "id"])} | events])
    end
  end

  defp request_outcome(request_id) do
    request = Repo.get!(Request, request_id)
    {request.status, request.response_status_code, request.last_error_code}
  end

  defp leases(session_id), do: Repo.all(from(l in BridgeOwnerLease, where: l.codex_session_id == ^session_id, order_by: [asc: l.created_at]))

  # A visible turn on the application peer, which publishes presence (so does
  # this node, the observer recovery requires). The peer is halted, its turn
  # cancelled by the owner, and its presence row made 121 s stale.
  defp replaced_socket_node_turn!(ctx) do
    _observer = start_supervised!({InstanceHeartbeat, enabled: true, name: :"unreachable_node_observer_#{System.unique_integer([:positive])}"})
    app_node = Map.fetch!(ctx.app_peers, ctx.app_peer).node
    heartbeat = %{id: :unreachable_node_heartbeat, start: {InstanceHeartbeat, :start_link, [[enabled: true, name: :unreachable_node_heartbeat]]}}
    {:ok, _heartbeat} = :erpc.call(app_node, Supervisor, :start_child, [CodexPooler.Supervisor, heartbeat])
    CodexPooler.UnboxedFixture.register_unboxed_cleanup!(fn -> Repo.delete_all(from(i in InstancePresence.Instance, where: i.node_name in ^[Atom.to_string(app_node), Atom.to_string(node())])) end)
    _published = await_state!(fn -> presence_rows(app_node) end, &(&1 != []), "the application peer never published presence", System.monotonic_time(:millisecond) + @detection_timeout_ms)

    turn = start_turn!(ctx, :death, successor: true)
    :ok = halt!(turn.app_peer.peer, turn.app_peer.node)
    %{reason: "unreachable_downstream_cancelled"} = await_generation_end!(turn.request_id)
    refute RequestLifecycle.execution_recovery_authority(attempt(turn.request_id))

    Repo.query!("UPDATE instance_presences SET last_seen_at = last_seen_at - ($2 * interval '1 second') WHERE node_name = $1", [Atom.to_string(app_node), @liveness_window_s + 1])
    turn
  end

  defp assert_recovered!(turn) do
    assert {"failed", 499, "absent_instance_recovered"} == request_outcome(turn.request_id)
    assert %CodexTurn{status: "interrupted", error_code: "absent_instance_recovered"} = Repo.get_by!(CodexTurn, request_id: turn.request_id)
  end

  defp presence_rows(node), do: Repo.all(from(i in InstancePresence.Instance, where: i.node_name == ^Atom.to_string(node), select: i.boot_id))

  # The request's outcome and its turn's once the request, every attempt and
  # the turn are terminal.
  defp await_turn_settled!(request_id) do
    deadline = System.monotonic_time(:millisecond) + @detection_timeout_ms

    await_state!(
      fn -> {request_outcome(request_id), Repo.get_by!(CodexTurn, request_id: request_id), Repo.exists?(from(a in Attempt, where: a.request_id == ^request_id and is_nil(a.completed_at)))} end,
      fn {{status, _code, _error}, turn, open_attempt?} -> status not in ["accepted", "in_progress"] and turn.status != "in_progress" and not open_attempt? end,
      "the turn never settled",
      deadline
    )
    |> then(fn {outcome, turn, _open_attempt?} -> {outcome, {turn.status, turn.error_code}} end)
  end

  defp settled_usage(request_id) do
    Repo.one!(
      from(entry in CodexPooler.Accounting.LedgerEntry,
        where: entry.request_id == ^request_id and entry.entry_kind == "settlement" and entry.amount_status == "recorded",
        select: {entry.usage_status, entry.input_tokens, entry.output_tokens, entry.total_tokens}
      )
    )
  end

  defp attempt(request_id), do: Repo.one!(from(a in Attempt, where: a.request_id == ^request_id))

  defp age_attempt!(request_id, seconds) do
    Repo.query!("UPDATE attempts SET started_at = started_at - ($2 * interval '1 second') WHERE request_id = $1", [Ecto.UUID.dump!(request_id), seconds])
  end

  # The owner's record of the generation's end for the turn's attempt, once written.
  defp await_generation_end!(request_id) do
    attempt_id = attempt(request_id).id
    deadline = System.monotonic_time(:millisecond) + @detection_timeout_ms
    await_state!(fn -> Repo.get(ForwardedGenerationEnd, attempt_id) end, &(not is_nil(&1)), "the owner recorded no end of the generation", deadline)
  end

  defp turn_frames(count \\ @deltas, preamble \\ 0) do
    created = %{"type" => "response.created", "response" => %{"id" => "resp_unreachable_turn", "status" => "in_progress"}}
    in_progress = List.duplicate(%{"type" => "response.in_progress", "response" => %{"id" => "resp_unreachable_turn", "status" => "in_progress"}}, preamble)
    deltas = in_progress ++ for i <- 1..count, do: %{"type" => "response.output_text.delta", "delta" => "synthetic #{i} "}
    completed = %{"type" => "response.completed", "response" => %{"id" => "resp_unreachable_turn", "status" => "completed", "output" => [], "usage" => %{"input_tokens" => 5, "output_tokens" => count, "total_tokens" => 5 + count}}}
    Enum.map([created | deltas] ++ [completed], &CodexPooler.JSON.encode!/1)
  end
end
