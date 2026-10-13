defmodule CodexPooler.Gateway.Runtime.NativeResponseSteeringTest do
  use CodexPooler.DataCase, async: false

  import ExUnit.CaptureLog
  import CodexPooler.PoolerFixtures
  import CodexPoolerWeb.Runtime.BackendCodexTestSupport

  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport,
    only: [await_socket_connection_state!: 2, socket_connection_state!: 1, model_serving_scope: 0, set_model_serving_mode!: 3]

  alias CodexPooler.Accounting.{Attempt, LedgerEntry, Request, RequestOutcome}
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway.Persistence.CodexTurn
  alias CodexPooler.Gateway.Runtime.NativeResponseSteering
  alias CodexPooler.Gateway.Runtime.NativeResponseSteeringTest.ActorDebugHook
  alias CodexPooler.Gateway.Transports.Websocket.{NativeCompactionAdmission, UpstreamWebsocketSession, WebsocketOwnerSession}
  alias CodexPooler.Platform.ExecutionRegistry
  alias CodexPooler.Platform.InstancePresence.Identity, as: InstanceIdentity
  alias CodexPooler.Repo
  alias CodexPoolerWeb.Runtime.BackendCodexWebsocketOwnerForwardingSupport
  alias CodexPoolerWeb.Runtime.DownstreamKeepaliveScenario
  alias CodexPoolerWeb.Runtime.WebsocketCleanupFence

  @moduletag capture_log: true
  @detection_timeout_ms 15_000
  @path "/backend-api/codex/responses"
  @original_usage %{"input_tokens" => 41, "output_tokens" => 23, "total_tokens" => 64}
  @successor_usage %{"input_tokens" => 110, "output_tokens" => 7, "total_tokens" => 117}
  @independent_usage %{"input_tokens" => 19, "output_tokens" => 3, "total_tokens" => 22}

  for mode <- ["full", "lite"], forwarding <- [:off, :on] do
    test "ordinary turns retain bounded steering state with mode=#{mode} forwarding=#{forwarding}" do
      assert_bounded_ordinary_context!(unquote(mode), unquote(forwarding))
    end
  end

  defp assert_bounded_ordinary_context!(mode, forwarding) do
    fixture = start_fixture!(forwarding, :coalesced, mode: mode, catalog_size: 20, independent?: true)
    assert_bounded_lane!(fixture.lane)
    assert :ok = FakeUpstream.release_steerable(fixture.upstream, fixture.hold)
    {client, frames} = receive_texts!(fixture.client, 1)
    assert event_types(frames) == ["response.completed"]
    [original] = await_rows!(fixture.setup, 1, &Enum.all?(&1, fn row -> row.status == "succeeded" end))
    settled = await_lane!(fixture.lane, &(map_size(&1.prepared) == 0 and not is_nil(&1.original_result)))
    assert settled.context.reserved.request.id == original.id
    assert is_nil(settled.active)
    assert_bounded_lane!(fixture.lane)
    assert_usage!(original, @original_usage)
    assert_complete_ledger!(original)

    client = send_text!(client, create(fixture.setup, fixture.thread, Ecto.UUID.generate(), "synthetic independent input"))
    {client, frames} = receive_texts!(client, 2)
    assert event_types(frames) == ["response.created", "response.completed"]
    settled_rows = await_rows!(fixture.setup, 2, &Enum.all?(&1, fn row -> row.status == "succeeded" end))
    independent = Enum.find(settled_rows, &(&1.id != original.id))
    assert await_lane!(fixture.lane, &(map_size(&1.prepared) == 0 and &1.context.reserved.request.id == independent.id))
    assert_bounded_lane!(fixture.lane)
    assert_usage!(independent, @independent_usage)
    Enum.each(settled_rows, &assert_complete_ledger!/1)
    assert :ok = FakeUpstream.verify!(fixture.upstream)
    close_client!(client)
  end

  defp assert_bounded_lane!(lane) do
    state = :sys.get_state(lane)
    # flat_size counts the copied terms, independent of GC and heap slack.
    assert :erts_debug.flat_size(state) * :erlang.system_info(:wordsize) < 1_048_576
    assert map_size(state.context.route_state.visible_model_context) == 0
    assert state.context.route_state.visible_models == []
    assert is_map(state.context.route_state.reservation_snapshot_inputs)
    assert state.context.route_state.candidates != []

    Enum.each(state.prepared, fn {_key, entry} ->
      assert map_size(entry.context.route_state.visible_model_context) == 0
      assert entry.context.route_state.visible_models == []
    end)
  end

  for row_locked? <- [false, true] do
    @tag slow: "the real downstream read timeout closes an untracked direct steering successor"
    test "the server idle timeout attributes only the active direct steering successor with row_locked=#{row_locked?}" do
      assert_steering_idle_timeout!(unquote(row_locked?))
    end
  end

  defp assert_steering_idle_timeout!(row_locked?) do
    if row_locked?, do: BackendCodexWebsocketOwnerForwardingSupport.enter_peer_owner_topology!()
    DownstreamKeepaliveScenario.put_settings!([websocket_idle_timeout_ms: 1_000], false)
    fixture = start_fixture!(:off, :separate)
    if row_locked?, do: register_unboxed_pool_cleanup!(fixture.setup)
    client = send_text!(fixture.client, steer(fixture.original_id))
    {client, _initial} = release_frames!(fixture, client, 0..2)
    state = await_socket_connection_state!(client.socket, &is_map(Map.get(&1, :native_response_steering_active)))
    identity = state.native_response_steering_active.identity
    # The provider holds the successor terminal while the real client keeps answering pings.
    assert_receive {:fake_upstream_frame_barrier, 3, _handler, _hold}, @detection_timeout_ms
    lane_monitor = Process.monitor(fixture.lane)
    lock = if row_locked?, do: DownstreamKeepaliveScenario.hold_session_row!(state.codex_session.id)
    on_exit(fn -> if lock, do: send(lock.pid, :release) end)
    turn = DownstreamKeepaliveScenario.read_turn!(Map.put(client, :sent_at, System.monotonic_time(:millisecond)), answer_pings?: true)
    assert turn.end in [{:close, 1002}, :socket_closed]

    if lock do
      assert Process.alive?(lock.pid)
      send(lock.pid, :release)
      assert Task.await(lock, @detection_timeout_ms) == :ok
    end

    close_client!(turn.client)
    assert_receive {:DOWN, ^lane_monitor, :process, _lane, _reason}, @detection_timeout_ms
    settled = await_rows!(fixture.setup, 2, &Enum.all?(&1, fn row -> row.status in ["succeeded", "failed"] end))
    successor = Enum.find(settled, &(&1.id == identity.request_id))
    original = Enum.find(settled, &(&1.id != identity.request_id))
    assert original.status == "succeeded"
    refute Map.has_key?(original.request_metadata, "downstream_interruption")
    assert successor.status == "failed"
    assert successor.last_error_code == "client_disconnected"
    assert successor.request_metadata["downstream_interruption"]["cause"] == "idle_timeout"
    refute RequestOutcome.client_cancelled?(successor)
    assert Repo.get!(Attempt, identity.attempt_id).response_metadata["downstream_interruption"] == successor.request_metadata["downstream_interruption"]
    Enum.each(settled, &assert_complete_ledger!/1)
  end

  test "a queued independent prepare does not replace the producing request before grouped provider successor frames" do
    assert_queued_context!(:coalesced)
  end

  test "a queued independent prepare retains the producing identity for separately arrived successor frames" do
    assert_queued_context!(:separate)
  end

  test "a real socket can prepare an independent request while its lane awaits a successor receipt" do
    fixture = start_fixture!(:off, :separate, independent?: true)
    client = send_text!(fixture.client, steer(fixture.original_id))
    {client, _initial} = release_frames!(fixture, client, 0..2)
    state = await_socket_connection_state!(client.socket, &is_map(Map.get(&1, :native_response_steering_active)))
    identity = state.native_response_steering_active.identity
    probe = install_probe!(fixture.lane, %{kind: :hold_terminal_until_prepare, notify: self(), ref: make_ref(), socket: client.socket, request_id: identity.request_id})
    ref = probe.ref
    lane = fixture.lane
    release_frame_without_read!(fixture, 3)
    assert_receive {^ref, :terminal_held, ^lane}, @detection_timeout_ms
    client = send_text!(client, create(fixture.setup, fixture.thread, Ecto.UUID.generate(), "synthetic independent input"))
    assert_receive {^ref, :prepare_queued, socket}, @detection_timeout_ms
    assert socket == client.socket
    # The genuine socket call is queued behind the terminal. No process stays
    # held after this release: any preparation/receipt cycle is product code.
    release_probe!(probe)
    finish_frame_barrier!(fixture, 4)
    {client, frames} = receive_texts!(client, 3)
    assert event_types(frames) == ["response.completed", "response.created", "response.completed"]
    assert get_in(CodexPooler.JSON.decode!(hd(frames)), ["response", "id"]) == fixture.successor_id
    assert get_in(CodexPooler.JSON.decode!(List.last(frames)), ["response", "id"]) == fixture.independent_id
    settled = await_rows!(fixture.setup, 3, &Enum.all?(&1, fn row -> row.status == "succeeded" end))
    successor = Enum.find(settled, &(&1.id == identity.request_id))
    original = Enum.find(settled, &(&1.id == successor.request_metadata["native_websocket_response_steering"]["predecessor_request_id"]))
    independent = Enum.find(settled, &(&1.id not in [original.id, successor.id]))
    assert_usage!(original, @original_usage)
    assert_usage!(successor, @successor_usage)
    assert_usage!(independent, @independent_usage)
    Enum.each(settled, &assert_complete_ledger!/1)
    assert await_lane!(lane, &(map_size(&1.prepared) == 0)).context.reserved.request.id == independent.id
    assert :ok = FakeUpstream.verify!(fixture.upstream)
    close_client!(client)
  end

  test "a real original settlement rollback returns its accounting failure and cannot arm or reserve a successor" do
    fixture = start_fixture!(:off, :coalesced)
    state = socket_connection_state!(fixture.client.socket)
    lane = state.native_response_steering
    context = :sys.get_state(lane).context
    close_monitor = Process.monitor(lane)
    fallback = active_upstream_assignment_fixture(fixture.setup.pool)
    assert Repo.get!(Attempt, context.attempt.id).status == "in_progress"
    original_attempt = Repo.get!(Attempt, context.attempt.id)
    on_exit(fn -> restore_attempt_identity(original_attempt) end)
    original_attempt |> Ecto.Changeset.change(upstream_identity_id: fallback.identity.id) |> Repo.update!()

    close_hold = install_probe!(fixture.client.socket, %{kind: :hold_close, notify: self(), ref: make_ref()})

    {{client, frames}, logs} =
      with_log(fn ->
        client = send_text!(fixture.client, steer(fixture.original_id))
        {client, frames} = receive_texts!(client, 2)
        assert_receive {ref, :close_held, socket}, @detection_timeout_ms
        assert ref == close_hold.ref
        assert socket == fixture.client.socket
        assert {:error, %{code: "gateway_accounting_failed"}} = :sys.get_state(lane).original_result
        assert :sys.get_state(lane).admission_revoked?
        assert {:error, :owner_unavailable} = NativeResponseSteering.open_successor(lane, fixture.successor_id)
        assert [original] = rows(fixture.setup)
        assert original.id == context.reserved.request.id
        assert original.status == "in_progress"
        assert Repo.get!(Attempt, context.attempt.id).status == "in_progress"
        assert Enum.frequencies_by(ledger(original), & &1.entry_kind) == %{"reservation" => 1}
        assert is_nil(:sys.get_state(lane).active)
        assert {:error, _reason} = UpstreamWebsocketSession.compaction_reservation_snapshot(state.upstream_websocket_session)
        Repo.get!(Attempt, context.attempt.id) |> Ecto.Changeset.change(upstream_identity_id: fixture.setup.identity.id) |> Repo.update!()
        release_probe!(close_hold)
        {client, _more, 1011} = receive_close!(client)
        assert_receive {:DOWN, ^close_monitor, :process, ^lane, _reason}, @detection_timeout_ms
        {client, frames}
      end)

    assert event_types(frames) == ["response.steer.accepted", "response.completed"]
    assert logs =~ "reason=upstream_reference_mismatch"
    assert logs =~ "operation=open_successor"
    assert FakeUpstream.physical_counts(fixture.upstream).websocket_generation == 1
    assert FakeUpstream.physical_counts(fixture.upstream).http_generation == 0
    close_client!(client)
    assert :ok = FakeUpstream.verify!(fixture.upstream)
  end

  test "an actual owner successor settlement failure is delivered as an error, never an acknowledgement-only success" do
    fixture = start_fixture!(:on, :separate)
    client = send_text!(fixture.client, steer(fixture.original_id))
    {client, _initial} = release_frames!(fixture, client, 0..2)
    state = await_socket_connection_state!(client.socket, &is_map(Map.get(&1, :native_response_steering_active)))
    identity = state.native_response_steering_active.identity
    lane = state.native_response_steering
    assert {:ok, owner} = WebsocketOwnerSession.lookup(state.codex_session.id)
    fallback = active_upstream_assignment_fixture(fixture.setup.pool)
    original_attempt = Repo.get!(Attempt, identity.attempt_id)
    on_exit(fn -> restore_attempt_identity(original_attempt) end)
    original_attempt |> Ecto.Changeset.change(upstream_identity_id: fallback.identity.id) |> Repo.update!()
    completion_hold = install_probe!(client.socket, %{kind: :hold_failed_completion, notify: self(), ref: make_ref(), identity: identity})

    {{client, [terminal]}, logs} =
      with_log(fn ->
        {client, [terminal]} = release_frames!(fixture, client, 3..3)
        assert_receive {ref, :done, {:error, %{code: "gateway_accounting_failed"} = error}}, @detection_timeout_ms
        assert ref == completion_hold.ref
        refute Map.has_key?(error, :native_response_steering_acknowledgement)
        assert_receive {ref, :close_held, socket}, @detection_timeout_ms
        assert ref == completion_hold.ref
        assert socket == client.socket
        assert {:error, %{code: "gateway_accounting_failed"}} = :sys.get_state(lane).original_result
        assert {:error, :owner_unavailable} = NativeResponseSteering.open_successor(lane, "resp_synthetic_forbidden_after_failed_settlement")
        assert length(rows(fixture.setup)) == 2
        assert :sys.get_state(owner).ordinary_success_result == nil
        successor_digest = :crypto.hash(:sha256, fixture.successor_id)
        refute match?(%NativeCompactionAdmission{binding: %{previous_response_digest: ^successor_digest}}, :sys.get_state(owner).native_compaction_admission)
        Repo.get!(Attempt, identity.attempt_id) |> Ecto.Changeset.change(upstream_identity_id: fixture.setup.identity.id) |> Repo.update!()
        finish_frame_barrier!(fixture, 4)
        release_probe!(completion_hold)
        {client, _more, 1011} = receive_close!(client)
        {client, [terminal]}
      end)

    assert event_types([terminal]) == ["response.completed"]
    assert logs =~ "reason=upstream_reference_mismatch"
    close_client!(client)
    assert :ok = FakeUpstream.verify!(fixture.upstream)
  end

  test "socket death while the actual successor receipt is awaited retires the unlinked lane and its execution" do
    fixture = start_fixture!(:off, :separate)
    client = send_text!(fixture.client, steer(fixture.original_id))
    {client, _initial} = release_frames!(fixture, client, 0..2)
    state = await_socket_connection_state!(client.socket, &is_map(Map.get(&1, :native_response_steering_active)))
    lane = state.native_response_steering
    identity = state.native_response_steering_active.identity
    successor_attempt = Repo.get!(Attempt, identity.attempt_id)
    assert ExecutionRegistry.status(successor_attempt.owner_execution_id, lane) == :alive
    lane_monitor = Process.monitor(lane)
    upstream_session = state.upstream_websocket_session
    producer_monitor = Process.monitor(upstream_session)
    socket_monitor = Process.monitor(client.socket)
    done_hold = install_probe!(client.socket, %{kind: :hold_done, notify: self(), ref: make_ref(), identity: identity})
    {client, [terminal]} = release_frames!(fixture, client, 3..3)
    assert event_types([terminal]) == ["response.completed"]
    assert_receive {ref, :done_held, socket}, @detection_timeout_ms
    assert ref == done_hold.ref
    assert socket == client.socket
    assert Repo.get!(Attempt, identity.attempt_id).status == "succeeded"
    finish_frame_barrier!(fixture, 4)

    # The callback is parked outside database work and before the receipt ACK.
    # This is a real listener-process death, not a fabricated delivery outcome.
    Process.exit(client.socket, :kill)
    assert_receive {:DOWN, ^socket_monitor, :process, socket, :killed}, @detection_timeout_ms
    assert socket == client.socket
    assert_receive {:DOWN, ^lane_monitor, :process, ^lane, :normal}, @detection_timeout_ms
    assert :ok = UpstreamWebsocketSession.close(upstream_session)
    assert_receive {:DOWN, ^producer_monitor, :process, ^upstream_session, _reason}, @detection_timeout_ms
    assert ExecutionRegistry.status(successor_attempt.owner_execution_id, lane) == :dead
    assert_complete_ledger!(Repo.get!(Request, identity.request_id))
    assert :sys.get_state(ExecutionRegistry).entries[successor_attempt.owner_execution_id] == {lane, :dead}
    Mint.HTTP.close(client.conn)
    assert :ok = FakeUpstream.verify!(fixture.upstream)
  end

  test "owner death after successor reservation commits compensates the request, turn and attempt before Close" do
    fixture = start_fixture!(:on, :separate)
    owner_hold = install_owner_acquisition_probe!(fixture.client)
    owner = owner_hold.server
    owner_monitor = Process.monitor(owner)
    lane = fixture.lane
    socket = fixture.client.socket
    owner_hold_ref = owner_hold.ref
    client = send_text!(fixture.client, steer(fixture.original_id))
    acquisition_probe = install_probe!(fixture.lane, %{kind: :observe_open_reply, notify: self(), ref: make_ref(), pending?: false})
    {client, _original} = release_frames!(fixture, client, 0..1)
    assert :sys.get_state(lane).owner == owner
    assert_receive {:fake_upstream_frame_barrier, 2, handler, hold}, @detection_timeout_ms
    assert handler == fixture.handler
    assert hold == fixture.hold
    assert :ok = FakeUpstream.release_frame(fixture.upstream, fixture.hold)
    assert_receive {^owner_hold_ref, :owner_acquisition_held, ^owner, ^lane, ^socket, identity, turn_id}, @detection_timeout_ms
    assert [original, successor] = rows(fixture.setup)
    assert original.status == "succeeded"
    assert successor.status == "in_progress"
    assert [attempt] = attempts(successor)
    assert attempt.status == "in_progress"
    assert identity == %{request_id: successor.id, attempt_id: attempt.id, replay_generation: attempt.replay_generation}
    assert {:ok, _execution_id} = Ecto.UUID.cast(attempt.owner_execution_id)
    assert attempt.owner_process_id == List.to_string(:erlang.pid_to_list(lane))
    assert attempt.owner_instance_id == Atom.to_string(node())
    assert attempt.owner_instance_boot_id == InstanceIdentity.boot_id()
    assert ExecutionRegistry.status(attempt.owner_execution_id, lane) == :alive
    assert %CodexTurn{id: ^turn_id, status: "in_progress"} = Repo.get_by!(CodexTurn, request_id: successor.id)
    assert [%LedgerEntry{entry_kind: "reservation"}] = ledger(successor)
    # The owner is parked before acquisition, so these real provider bytes
    # cannot finish the successor. Release its entire tail before killing the
    # owner, while the handler can still acknowledge every required barrier.
    release_frame_without_read!(fixture, 3)
    finish_frame_barrier!(fixture, 4)
    on_exit(fn -> resume_process(client.socket) end)
    assert :ok = :sys.suspend(client.socket)

    logs =
      capture_log(fn ->
        # The acquisition hook is before the owner's callback/database work.
        # Killing this exact owner makes the in-flight real GenServer.call exit.
        Process.exit(owner, :kill)
        assert_receive {:DOWN, ^owner_monitor, :process, ^owner, :killed}, @detection_timeout_ms
        # The database commit precedes execution retirement. The lane's reply
        # acknowledges both, while the suspended socket still cannot Close.
        assert_receive {ref, :open_reply, {:error, :owner_unavailable}}, @detection_timeout_ms
        assert ref == acquisition_probe.ref
        [_original, compensated] = await_rows!(fixture.setup, 2, &Enum.all?(&1, fn row -> row.completed_at != nil end))
        assert compensated.id == successor.id
        assert compensated.status == "failed"
        assert [settled_attempt] = attempts(compensated)
        assert settled_attempt.id == attempt.id
        assert settled_attempt.status == "failed"
        assert settled_attempt.completed_at != nil
        assert Repo.get_by!(CodexTurn, request_id: successor.id).status not in ["accepted", "in_progress"]
        assert_complete_ledger!(compensated)
        assert ExecutionRegistry.status(settled_attempt.owner_execution_id, fixture.lane) == :dead
        assert is_nil(:sys.get_state(fixture.lane).active)
        assert :ok = :sys.resume(client.socket)
      end)

    assert logs =~ "phase=owner_successor"
    assert logs =~ "reason_code=owner_unavailable"
    {client, _frames, _code} = receive_close!(client)
    close_client!(client)
    assert :ok = FakeUpstream.verify!(fixture.upstream)
  end

  test "a cancelled producing generation cannot be reactivated or reserve new work" do
    fixture = start_fixture!(:off, :coalesced, successor?: false)
    lane = fixture.lane
    assert :ok = NativeResponseSteering.cancel(lane, :client_disconnected)
    assert :sys.get_state(lane).admission_revoked?
    assert {:error, :owner_unavailable} = NativeResponseSteering.activate(lane, fixture.upstream_session, nil)
    logs = capture_log(fn -> assert {:error, :owner_unavailable} = NativeResponseSteering.open_successor(lane, fixture.successor_id) end)
    assert logs =~ "phase=lane_acquisition"
    assert length(rows(fixture.setup)) == 1
    close_client!(fixture.client)
  end

  defp assert_queued_context!(batches) do
    fixture = start_fixture!(:off, batches, independent?: true, before_create: &install_next_request_probe!/1)
    next_hold = fixture.before_create
    next_hold_ref = next_hold.ref
    producer = fixture.upstream_session
    state = :sys.get_state(fixture.lane)
    original_context = state.context
    client = send_text!(fixture.client, create(fixture.setup, fixture.thread, Ecto.UUID.generate(), "synthetic independent input"))
    queued = await_lane!(fixture.lane, &(map_size(&1.prepared) == 2))
    assert queued.context.reserved.request.id == original_context.reserved.request.id
    assert queued.context.attempt.id == original_context.attempt.id
    assert [independent_key] = Enum.reject(Map.keys(queued.prepared), &(&1 == {original_context.reserved.request.id, original_context.attempt.id}))
    assert length(FakeUpstream.requests(fixture.upstream)) == 1
    arrival = producer_socket_snapshot!(fixture)
    client = send_text!(client, steer(fixture.original_id))

    {client, frames} =
      case batches do
        :coalesced ->
          assert_receive {^next_hold_ref, :next_request_held, ^producer, ^independent_key}, @detection_timeout_ms
          # A grouped provider push need not reach Mint as one decoded batch.
          # Observe the entire real TCP tail before releasing the queued request,
          # whether that tail was decoded before the hook or is still unread.
          await_transport_arrival!(next_hold, arrival, fixture.steering_frames)
          assert :sys.get_state(fixture.lane).context.reserved.request.id != elem(independent_key, 0)
          release_probe!(next_hold)
          {client, frames} = receive_texts!(client, 4)
          {client, frames}

        :separate ->
          {client, first} = release_frames!(fixture, client, 0..1)
          assert_receive {^next_hold_ref, :next_request_held, ^producer, ^independent_key}, @detection_timeout_ms
          Enum.each(2..3, &release_frame_without_read!(fixture, &1))
          # The next actual request stays parked until all four real provider
          # frames have reached its TCP socket, not merely the fake's send hook.
          await_transport_arrival!(next_hold, arrival, fixture.steering_frames)
          parked = :sys.get_state(fixture.lane)
          assert parked.context.reserved.request.id == original_context.reserved.request.id
          assert parked.context.attempt.id == original_context.attempt.id
          assert is_nil(parked.active)
          release_probe!(next_hold)
          {client, successor} = receive_texts!(client, 2)
          finish_frame_barrier!(fixture, 4)
          {client, first ++ successor}
      end

    {client, independent} = receive_texts!(client, 2)
    assert event_types(frames) == ["response.steer.accepted", "response.completed", "response.created", "response.completed"]
    assert event_types(independent) == ["response.created", "response.completed"]
    assert get_in(CodexPooler.JSON.decode!(List.last(independent)), ["response", "id"]) == fixture.independent_id
    settled = await_rows!(fixture.setup, 3, &Enum.all?(&1, fn row -> row.status == "succeeded" end))
    original = Enum.find(settled, &(&1.id == original_context.reserved.request.id))
    successor = Enum.find(settled, &is_map(&1.request_metadata["native_websocket_response_steering"]))
    independent_row = Enum.find(settled, &(&1.id not in [original.id, successor.id]))
    assert successor.request_metadata["native_websocket_response_steering"]["predecessor_request_id"] == original.id
    assert_usage!(original, @original_usage)
    assert_usage!(successor, @successor_usage)
    assert_usage!(independent_row, @independent_usage)
    assert Enum.all?(settled, &(length(attempts(&1)) == 1))
    Enum.each(settled, &assert_complete_ledger!/1)
    assert [%{websocket_connection_id: connection}, %{websocket_connection_id: connection}] = FakeUpstream.requests(fixture.upstream)
    assert connection == 1
    assert length(FakeUpstream.websocket_steers(fixture.upstream)) == 1
    assert FakeUpstream.physical_counts(fixture.upstream).http_generation == 0
    assert await_lane!(fixture.lane, &(map_size(&1.prepared) == 0)).context.reserved.request.id == independent_row.id
    assert :ok = FakeUpstream.verify!(fixture.upstream)
    close_client!(client)
  end

  defp producer_socket_snapshot!(fixture) do
    producer = fixture.upstream_session
    port = URI.parse(fixture.upstream.url).port

    sockets =
      Enum.filter(:erlang.ports(), fn socket ->
        :erlang.port_info(socket, :connected) == {:connected, producer} and
          :inet.peername(socket) == {:ok, {{127, 0, 0, 1}, port}}
      end)

    assert [socket] = sockets
    assert {:ok, [recv_oct: received]} = :inet.getstat(socket, [:recv_oct])
    %{socket: socket, received: received}
  end

  defp await_transport_arrival!(probe, arrival, frames) do
    required = Enum.reduce(frames, 0, &(byte_size(&1) + websocket_header_bytes(&1) + &2))
    ref = probe.ref
    producer = probe.server
    send(producer, {ref, :await_transport_bytes, arrival.socket, arrival.received, required})
    assert_receive {^ref, :transport_arrived, ^producer, received}, @detection_timeout_ms
    assert received >= required
  end

  defp start_fixture!(forwarding, batches, opts \\ []) do
    CodexPooler.TestAppEnv.restore_on_exit(:websocket_owner_forwarding_enabled)
    Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, forwarding == :on)
    suffix = System.unique_integer([:positive])
    original_id = "resp_synthetic_lifecycle_original_#{suffix}"
    successor_id = "resp_synthetic_lifecycle_successor_#{suffix}"
    independent_id = "resp_synthetic_lifecycle_independent_#{suffix}"
    hold = make_ref()
    steer_id = "steer_synthetic_lifecycle_#{suffix}"
    successor_frames = if Keyword.get(opts, :successor?, true), do: [created(successor_id), completed(successor_id, @successor_usage)], else: []
    opening = [created(original_id), encode(%{"type" => "response.output_text.delta", "item_id" => "msg_synthetic_lifecycle", "output_index" => 0, "content_index" => 0, "delta" => "synthetic visible output"})]
    response = FakeUpstream.websocket_steerable(opening, notify: self(), ref: hold, response_id: original_id, steer_id: steer_id, terminal_frames: [completed(original_id, @original_usage)], successor_frames: successor_frames, batches: batches)
    steering_frames = [encode(%{"type" => "response.steer.accepted", "sequence_number" => 1, "steer" => %{"id" => steer_id, "previous_response_id" => original_id}}), completed(original_id, @original_usage) | successor_frames]
    expectations = [expected_request(response)]
    expectations = if Keyword.get(opts, :independent?, false), do: expectations ++ [expected_request(FakeUpstream.websocket_text_frames([created(independent_id), completed(independent_id, @independent_usage)]))], else: expectations
    upstream = start_upstream(FakeUpstream.strict_sequence(expectations))
    setup = gateway_setup(upstream)
    set_model_serving_mode!(model_serving_scope(), setup, Keyword.get(opts, :mode, "full"))
    seed_unselected_catalog!(setup, Keyword.get(opts, :catalog_size, 0))
    {_server, port} = start_public_endpoint_with_server!()
    # The listener's preinstalled fence owns teardown, including partial
    # connection setup. Keep release ahead of listener shutdown and never
    # query the test process's fence or close a stale Mint state from on_exit.
    on_exit(fn ->
      try do
        FakeUpstream.release_steerable(upstream, hold)
        FakeUpstream.release_remaining_frames(upstream, hold)
      catch
        :exit, _reason -> :ok
      end
    end)

    thread = Ecto.UUID.generate()
    before = WebsocketCleanupFence.listener_sockets()
    {conn, websocket, ref, _headers} = public_websocket_connect_with_request_headers!(port, setup, thread, @path, [{"session-id", thread}, {"thread-id", thread}, {"x-codex-window-id", "#{thread}:0"}])
    socket = WebsocketCleanupFence.await_new_listener_socket!(before)
    client = %{conn: conn, websocket: websocket, ref: ref, socket: socket, frames: []}
    before_create = if callback = Keyword.get(opts, :before_create), do: callback.(client)
    client = send_text!(client, create(setup, thread, Ecto.UUID.generate(), "synthetic original input"))

    assert_receive {:fake_upstream_steerable_open, handler, ^hold}, @detection_timeout_ms
    {client, frames} = receive_texts!(client, length(opening))
    assert event_types(frames) == ["response.created", "response.output_text.delta"]
    state = socket_connection_state!(client.socket)
    assert is_pid(state.native_response_steering)
    lane_state = :sys.get_state(state.native_response_steering)
    assert lane_state.socket == client.socket

    upstream_session =
      if forwarding == :on do
        assert {:ok, owner} = WebsocketOwnerSession.lookup(state.codex_session.id)
        owner_state = :sys.get_state(owner)
        assert owner_state.native_response_steering == state.native_response_steering
        assert lane_state.owner == owner
        assert lane_state.upstream == owner_state.upstream_pid
        owner_state.upstream_pid
      else
        assert is_nil(lane_state.owner)
        assert lane_state.upstream == state.upstream_websocket_session
        state.upstream_websocket_session
      end

    %{setup: setup, upstream: upstream, hold: hold, handler: handler, client: client, thread: thread, lane: state.native_response_steering, upstream_session: upstream_session, before_create: before_create, original_id: original_id, successor_id: successor_id, independent_id: independent_id, successor_frames: successor_frames, steering_frames: steering_frames}
  end

  defp seed_unselected_catalog!(_setup, 0), do: :ok

  defp seed_unselected_catalog!(setup, size) do
    metadata = Map.put(setup.model.metadata, "synthetic_catalog_details", Enum.map(1..512, &%{"index" => &1, "label" => "synthetic model metadata"}))

    Enum.each(1..size, fn index ->
      model_fixture(setup.pool, %{exposed_model_id: "synthetic-unselected-#{index}", metadata: metadata})
    end)
  end

  defp install_next_request_probe!(client) do
    state = socket_connection_state!(client.socket)
    install_probe!(state.upstream_websocket_session, %{kind: :hold_next_request, notify: self(), ref: make_ref(), seen?: false, original_from: nil})
  end

  defp install_owner_acquisition_probe!(client) do
    state = socket_connection_state!(client.socket)
    lane = state.native_response_steering
    assert {:ok, owner} = WebsocketOwnerSession.lookup(state.codex_session.id)
    assert :sys.get_state(lane).owner == owner
    assert :sys.get_state(lane).socket == client.socket
    install_probe!(owner, %{kind: :hold_owner_acquisition, notify: self(), ref: make_ref(), lane: lane, socket: client.socket})
  end

  defp install_probe!(server, probe) do
    on_exit(fn ->
      send(server, {probe.ref, :release})

      try do
        :sys.remove(server, probe.ref, @detection_timeout_ms)
      catch
        :exit, _reason -> :ok
      end
    end)

    assert :ok = :sys.install(server, {probe.ref, &ActorDebugHook.hook/3, probe}, @detection_timeout_ms)
    Map.put(probe, :server, server)
  end

  defp release_probe!(probe), do: send(probe.server, {probe.ref, :release})

  defp resume_process(pid) do
    :sys.resume(pid)
  catch
    :exit, _reason -> :ok
  end

  defp restore_attempt_identity(attempt) do
    if current = Repo.get(Attempt, attempt.id), do: current |> Ecto.Changeset.change(upstream_identity_id: attempt.upstream_identity_id) |> Repo.update!()
  end

  defp expected_request(response), do: FakeUpstream.expect_request(method: "WEBSOCKET", path: @path, websocket_connection_ordinal: 1, json: [valid: true, equals: %{"type" => "response.create"}], respond: response)
  defp encode(frame), do: CodexPooler.JSON.encode!(frame)
  defp created(id), do: encode(%{"type" => "response.created", "response" => %{"id" => id, "status" => "in_progress", "output" => []}})
  defp completed(id, usage), do: encode(%{"type" => "response.completed", "response" => %{"id" => id, "status" => "completed", "output" => [], "usage" => usage}})
  defp steer(id), do: encode(%{"type" => "response.steer", "previous_response_id" => id, "input" => [%{"type" => "message", "role" => "user", "content" => [%{"type" => "input_text", "text" => "synthetic steering input"}]}]})
  defp websocket_header_bytes(frame), do: if(byte_size(frame) < 126, do: 2, else: 4)

  defp create(setup, thread, turn, text) do
    metadata = %{"session_id" => thread, "thread_id" => thread, "turn_id" => turn, "request_kind" => "turn"}
    encode(%{"type" => "response.create", "model" => setup.model.exposed_model_id, "instructions" => "synthetic lifecycle instructions", "tools" => [], "input" => [%{"type" => "message", "role" => "user", "content" => [%{"type" => "input_text", "text" => text}]}], "stream" => true, "store" => false, "client_metadata" => %{"session_id" => thread, "thread_id" => thread, "turn_id" => turn, "x-codex-window-id" => "#{thread}:0", "x-codex-turn-metadata" => encode(metadata)}})
  end

  defp send_text!(client, frame) do
    {conn, websocket} = public_websocket_send_text!(client.conn, client.websocket, client.ref, frame)
    %{client | conn: conn, websocket: websocket}
  end

  # Separate steering releases accepted (0), original terminal (1), successor
  # created (2), successor terminal (3), and finally the empty tail barrier (4).
  defp release_frames!(fixture, client, ordinals) do
    Enum.reduce(ordinals, {client, []}, fn ordinal, {client, frames} ->
      release_frame_without_read!(fixture, ordinal)
      {client, [frame]} = receive_texts!(client, 1)
      {client, frames ++ [frame]}
    end)
  end

  defp release_frame_without_read!(fixture, ordinal) do
    assert_receive {:fake_upstream_frame_barrier, ^ordinal, handler, hold}, @detection_timeout_ms
    assert handler == fixture.handler
    assert hold == fixture.hold
    assert :ok = FakeUpstream.release_frame(fixture.upstream, fixture.hold)
  end

  defp finish_frame_barrier!(fixture, ordinal), do: release_frame_without_read!(fixture, ordinal)
  defp receive_texts!(client, 0), do: {client, []}

  defp receive_texts!(client, count) do
    {client, frame} = receive_frame!(client)
    assert {:text, text} = frame
    {client, rest} = receive_texts!(client, count - 1)
    {client, [text | rest]}
  end

  defp receive_close!(client, frames \\ []) do
    {client, frame} = receive_frame!(client)

    case frame do
      {:close, code, _reason} -> {client, Enum.reverse(frames), code}
      {:text, raw} -> receive_close!(client, [raw | frames])
    end
  end

  defp receive_frame!(%{frames: [frame | rest]} = client), do: {%{client | frames: rest}, frame}

  defp receive_frame!(client) do
    message = receive_mint_socket_message!(client.conn, @detection_timeout_ms, "timed out waiting for native steering lifecycle")

    case Mint.WebSocket.stream(client.conn, message) do
      {:ok, conn, parts} ->
        {websocket, frames} = decode_lifecycle_parts(parts, client)

        receive_frame!(%{client | conn: conn, websocket: websocket, frames: frames})

      {:error, _conn, reason, _parts} ->
        flunk("native lifecycle transport failed: #{inspect(reason)}")

      :unknown ->
        receive_frame!(client)
    end
  end

  defp decode_lifecycle_parts(parts, client) do
    Enum.reduce(parts, {client.websocket, []}, fn
      {:data, ref, data}, {websocket, frames} when ref == client.ref ->
        assert {:ok, websocket, decoded} = Mint.WebSocket.decode(websocket, data)
        {websocket, frames ++ Enum.filter(decoded, &lifecycle_frame?/1)}

      _part, acc ->
        acc
    end)
  end

  defp lifecycle_frame?({:text, text}), do: not String.starts_with?(CodexPooler.JSON.decode!(text)["type"] || "", "codex.")
  defp lifecycle_frame?({:close, _code, _reason}), do: true
  defp lifecycle_frame?(_control), do: false

  defp close_client!(client) do
    Mint.HTTP.close(client.conn)
    WebsocketCleanupFence.await_listener_socket_cleanup!(client.socket)
  end

  defp event_types(frames), do: Enum.map(frames, &CodexPooler.JSON.decode!(&1)["type"])
  defp rows(setup), do: Repo.all(from(request in Request, where: request.pool_id == ^setup.pool.id, order_by: [asc: request.admitted_at]))
  defp attempts(request), do: Repo.all(from(attempt in Attempt, where: attempt.request_id == ^request.id))
  defp ledger(request), do: Repo.all(from(entry in LedgerEntry, where: entry.request_id == ^request.id))
  defp assert_complete_ledger!(request), do: assert(Enum.frequencies_by(ledger(request), & &1.entry_kind) == %{"reservation" => 1, "release" => 1, "settlement" => 1})

  defp assert_usage!(request, expected) do
    assert [entry] = Enum.filter(ledger(request), &(&1.entry_kind == "settlement"))
    assert {entry.input_tokens, entry.output_tokens, entry.total_tokens} == {expected["input_tokens"], expected["output_tokens"], expected["total_tokens"]}
  end

  defp await_rows!(setup, count, ready?, deadline \\ nil) do
    deadline = deadline || System.monotonic_time(:millisecond) + @detection_timeout_ms
    rows = rows(setup)

    cond do
      length(rows) == count and ready?.(rows) ->
        rows

      System.monotonic_time(:millisecond) >= deadline ->
        flunk("native requests did not finish their observed lifecycle; statuses=#{inspect(Enum.map(rows, & &1.status))}")

      true ->
        receive do
        after
          5 -> await_rows!(setup, count, ready?, deadline)
        end
    end
  end

  defp await_lane!(lane, ready?, deadline \\ nil) do
    deadline = deadline || System.monotonic_time(:millisecond) + @detection_timeout_ms
    state = :sys.get_state(lane)

    cond do
      ready?.(state) ->
        state

      System.monotonic_time(:millisecond) >= deadline ->
        flunk("native lane did not reach its causal dispatch phase")

      true ->
        receive do
        after
          5 -> await_lane!(lane, ready?, deadline)
        end
    end
  end

  defmodule ActorDebugHook do
    @moduledoc false
    import ExUnit.Assertions

    @spec hook(map(), term(), term()) :: map() | :done
    def hook(%{kind: :hold_next_request, seen?: false} = probe, {:in, {:"$gen_call", from, {:request, _request}}}, _name), do: %{probe | seen?: true, original_from: from}

    def hook(%{kind: :hold_next_request, original_from: from} = probe, {:out, _reply, from, %{conn: conn}}, _name), do: Map.put(probe, :socket, Mint.HTTP.get_socket(conn))

    def hook(%{kind: :hold_next_request, seen?: true, socket: socket} = probe, {:in, {:"$gen_call", _from, {:request, request}}}, _name) do
      {:ok, [active: active]} = :inet.getopts(socket, [:active])
      send(probe.notify, {probe.ref, :next_request_held, self(), {request.request_id, request.attempt_id}})

      try do
        await_release(probe)
      after
        restore_active(socket, active)
      end
    end

    def hook(%{kind: :hold_terminal_until_prepare, request_id: request_id} = probe, {:in, {:"$gen_call", _from, {:terminal, request_id, _finalization}}}, _name) do
      send(probe.notify, {probe.ref, :terminal_held, self()})
      socket = probe.socket

      receive do
        {:"$gen_call", {^socket, _tag}, {:prepare, _context, _callbacks, _dispatcher}} = pending ->
          send(self(), pending)
          send(probe.notify, {probe.ref, :prepare_queued, socket})
          await_release(probe)
      after
        15_000 -> flunk("the real socket preparation call never queued behind the held terminal")
      end
    end

    def hook(%{kind: :hold_owner_acquisition, lane: lane, socket: socket} = probe, {:in, {:"$gen_call", {lane, _tag}, {:begin_steering_successor, lane, socket, identity, turn_id}}}, _name) do
      send(probe.notify, {probe.ref, :owner_acquisition_held, self(), lane, socket, identity, turn_id})
      await_release(probe)
    end

    def hook(%{kind: :hold_done, identity: identity} = probe, {:in, {:native_response_steering_done, _lane, identity, _result}}, _name), do: hold(probe, :done_held)

    def hook(%{kind: :hold_failed_completion, identity: identity} = probe, {:in, {:native_response_steering_done, _lane, identity, result}}, _name) do
      send(probe.notify, {probe.ref, :done, result})
      probe
    end

    def hook(%{kind: :hold_failed_completion} = probe, {:in, {:native_response_steering_close, _lane, _code, _reason}}, _name), do: hold(probe, :close_held)
    def hook(%{kind: :hold_close} = probe, {:in, {:native_response_steering_close, _lane, _code, _reason}}, _name), do: hold(probe, :close_held)
    def hook(%{kind: :observe_open_reply} = probe, {:in, {:"$gen_call", from, {:open, _response_id}}}, _name), do: probe |> Map.put(:pending?, true) |> Map.put(:open_from, from)

    def hook(%{kind: :observe_open_reply, pending?: true, open_from: to} = probe, {:out, reply, to, _state}, _name) do
      send(probe.notify, {probe.ref, :open_reply, open_reply_identity(reply)})
      :done
    end

    def hook(%{kind: :observe_open_reply, pending?: true, open_from: to} = probe, {:out, reply, to}, _name) do
      send(probe.notify, {probe.ref, :open_reply, open_reply_identity(reply)})
      :done
    end

    def hook(probe, _event, _name), do: probe

    defp open_reply_identity({:ok, %{request_id: request_id, attempt_id: attempt_id}}), do: {:ok, %{request_id: request_id, attempt_id: attempt_id}}
    defp open_reply_identity({:error, reason}) when is_atom(reason), do: {:error, reason}
    defp open_reply_identity(_reply), do: :unclassified_reply

    defp hold(probe, phase) do
      send(probe.notify, {probe.ref, phase, self()})
      await_release(probe)
    end

    defp await_release(probe) do
      ref = probe.ref
      socket = Map.get(probe, :socket)

      receive do
        {^ref, :release} ->
          :done

        {^ref, :await_transport_bytes, ^socket, baseline, bytes} when not is_nil(socket) and is_integer(baseline) and is_integer(bytes) and bytes > 0 and bytes < 32_768 ->
          # A nonempty TCP message consumes at least one byte, so this finite
          # allowance covers every possible split without unbounded active mode.
          :ok = :inet.setopts(probe.socket, active: bytes)

          case await_transport_bytes(Map.put(probe, :received_baseline, baseline), bytes, System.monotonic_time(:millisecond) + 15_000) do
            {:arrived, received} ->
              send(probe.notify, {ref, :transport_arrived, self(), received})
              await_release(probe)

            :released ->
              :done
          end
      after
        15_000 -> flunk("the native lifecycle process barrier was not released")
      end
    end

    defp received_bytes!(socket) do
      {:ok, [recv_oct: bytes]} = :inet.getstat(socket, [:recv_oct])
      bytes
    end

    defp restore_active(socket, active) do
      case :inet.setopts(socket, active: active) do
        :ok -> :ok
        {:error, :closed} -> :ok
        {:error, :einval} -> :ok
      end
    end

    # The baseline predates steering, so already decoded frames and TCP messages
    # still queued behind Mint's active-once read both count as real arrivals.
    # The callback remains parked; no private mailbox or sensitive flag is read.
    defp await_transport_bytes(probe, required, deadline) do
      received = received_bytes!(probe.socket) - probe.received_baseline
      ref = probe.ref

      cond do
        received >= required ->
          {:arrived, received}

        System.monotonic_time(:millisecond) >= deadline ->
          flunk("the provider frames never reached the parked producing request handoff; received_bytes=#{received} required_bytes=#{required}")

        true ->
          receive do
            {^ref, :release} -> :released
          after
            5 -> await_transport_bytes(probe, required, deadline)
          end
      end
    end
  end
end
