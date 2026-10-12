defmodule CodexPoolerWeb.Runtime.BackendCodexWebsocketOwnerForwarding.RetainedUsageTest do
  use CodexPoolerWeb.ConnCase, async: false

  @moduletag capture_log: true
  import Ecto.Query
  import CodexPoolerWeb.Runtime.BackendCodexTestSupport
  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport, only: [model_serving_scope: 0, set_model_serving_mode!: 3]
  import CodexPoolerWeb.Runtime.BackendCodexWebsocketOwnerForwardingSupport

  alias CodexPooler.Access
  alias CodexPooler.Accounting.{Attempt, LedgerEntry, Request}
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway.Persistence.{BridgeOwnerLease, CodexSession, CodexTurn}
  alias CodexPooler.Gateway.Transports.Websocket.{ActivityRegistry, RolloutDrain, WebsocketOwnerSession}
  alias CodexPooler.Gateway.Transports.WebsocketRolloutDrainSupport
  alias CodexPooler.Gateway.Transports.WebsocketRolloutDrainSupport.VirtualDeadline
  alias CodexPooler.Gateway.Websocket, as: Gateway
  alias CodexPooler.Platform.InstancePresence.Identity
  alias CodexPooler.Repo
  alias CodexPoolerWeb.CodexResponsesSocket
  alias CodexPoolerWeb.Runtime.BackendCodexWebsocketOwnerForwardingSupport.LegacyUsageNodeClient
  alias CodexPoolerWeb.Runtime.{OwnerCrashAfterSendScenario, WebsocketCleanupFence}

  @timeout 15_000
  @window "019a0000-0000-7000-8000-00000000e364:0"

  setup do
    CodexPooler.TestAppEnv.restore_on_exit(:websocket_owner_forwarding_enabled, nil)
    Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, true)
    :ok
  end

  for topology <- [:local, :peer], mode <- ["full", "lite"], interruption <- [:detach, :submitter_down, :timeout, :retirement, :drain, :delivered] do
    @tag topology: topology, mode: mode, interruption: interruption
    if topology == :peer, do: @tag(slow: "real second BEAM with committed PostgreSQL")

    test "normalized held receipt remains measured (#{topology}, #{mode}, #{interruption})", context do
      run_scenario(context.topology, context.mode, context.interruption)
    end
  end

  for mode <- ["full", "lite"] do
    @tag mode: mode, topology: :peer, interruption: :legacy_caller
    test "admitted legacy caller drops additive usage while peer owner commits (#{mode})", %{mode: mode} do
      enter_peer_owner_topology!()
      hold = make_ref()
      upstream = start_upstream(FakeUpstream.websocket_text_frames([encode(completed())]))
      setup = gateway_setup(upstream)
      set_model_serving_mode!(model_serving_scope(), setup, mode)
      attrs = %{session_header: @window, session_header_source: "x-codex-window-id"}
      peer = start_peer_session_owner!(setup, attrs, upstream: held_terminal_writer_boundary(self(), hold), terminal_delivery_timeout_ms: 30_000)
      {:ok, auth} = Access.authenticate_authorization_header(setup.authorization)
      {:ok, socket} = owner_socket(auth, Ecto.UUID.generate(), Ecto.UUID.generate(), session_header: @window, session_header_source: "x-codex-window-id", websocket_owner_forwarder_opts: [node_client: LegacyUsageNodeClient])
      opts = Gateway.websocket_owner_response_options(socket.opts, socket.codex_session, socket.websocket_owner_lease_token, socket.websocket_owner_downstream)
      caller = Task.async(fn -> Gateway.run_websocket_response(auth, websocket_payload(setup, "synthetic legacy caller"), opts, fn _frame -> :ok end) end)
      caller_monitor = Process.monitor(caller.pid)
      on_exit(fn -> if Process.alive?(caller.pid), do: Process.exit(caller.pid, :kill) end)
      assert_receive {:held_terminal_writer, writer, ^hold}, @timeout
      writer_monitor = Process.monitor(writer)
      on_exit(fn -> send(writer, {:discard_terminal_writer, hold}) end)
      pending = await_pending!(peer.owner_pid, deadline())
      assert {:ok, %{response_usage: %{status: "usage_known", total_tokens: 120}}} = pending.active_turn.pending_result
      request_id = pending.active_turn.admission_request_id
      attempt_id = pending.active_turn.admission_attempt_id
      assert :ok = WebsocketOwnerSession.detach_downstream(peer.owner_pid, pending.downstream)
      assert_receive {:legacy_caller_dropped_usage, %{status: "usage_known", total_tokens: 120}}, @timeout
      _result = Task.await(caller, @timeout)
      assert_receive {:DOWN, ^caller_monitor, :process, _caller, :normal}, @timeout
      send(writer, {:discard_terminal_writer, hold})
      assert_receive {:DOWN, ^writer_monitor, :process, ^writer, :normal}, @timeout
      assert :ok = CodexResponsesSocket.terminate(:closed, socket)
      WebsocketCleanupFence.await_session_cleanups!()
      request = await_request!(request_id, "failed", deadline())
      settlement = await_settlement!(request_id, deadline())
      assert {settlement.attempt_id, settlement.total_tokens} == {attempt_id, 120}
      assert {request.last_error_code, request.response_status_code} == {"client_disconnected", 499}
      assert Repo.aggregate(from(e in LedgerEntry, where: e.request_id == ^request_id and e.entry_kind == "settlement" and e.amount_status == "recorded"), :count) == 1
      assert request.request_metadata["routing"]["model_serving_mode"] == mode
      assert FakeUpstream.count(upstream) == 1
      record_evidence(%{scenario: "admitted_legacy_caller_dropped_usage", fixture_port: URI.parse(FakeUpstream.url(upstream)).port, topology: :peer, mode: mode, owner_node: Atom.to_string(peer.node), caller_node: Atom.to_string(node()), request_id: request_id, attempt_id: attempt_id, replay_generation: Repo.get!(Attempt, attempt_id).replay_generation, settlement_id: settlement.id, total_tokens: 120, request_status: request.status, error_code: request.last_error_code, actual_additive_field_dropped: true, provider_generations: 1})
    end
  end

  for force_cut <- [false, true] do
    @tag retained_sql_drain: true
    test "known receipt registers before blocked interruption SQL (force_cut=#{force_cut})", do: run_sql_drain(unquote(force_cut))
  end

  defp run_sql_drain(force_cut) do
    OwnerCrashAfterSendScenario.per_process_connections!()
    assert_namespace!()
    harness = WebsocketRolloutDrainSupport.start_rollout_drain_harness(self())
    hold = make_ref()
    upstream = start_upstream(FakeUpstream.websocket_text_frames([encode(completed())]))
    setup = gateway_setup(upstream)
    register_unboxed_pool_cleanup!(setup)
    {:ok, auth} = Access.authenticate_authorization_header(setup.authorization)
    {session, owner} = start_remote_session_owner!(auth, %{session_header: @window, session_header_source: "x-codex-window-id"}, node(), :real, upstream: held_terminal_writer_boundary(self(), hold), receipt_registry: harness.activity_registry, terminal_delivery_timeout_ms: 30_000)
    port = start_public_endpoint!()
    {conn, ws, ref, _} = public_websocket_connect_with_request_headers!(port, setup, Ecto.UUID.generate(), "/backend-api/codex/responses", socket_headers("full"))
    {conn, _ws} = public_websocket_send_text!(conn, ws, ref, encode(%{"type" => "response.create", "model" => setup.model.exposed_model_id, "instructions" => "synthetic instructions", "input" => [%{"role" => "user", "content" => "synthetic question"}], "tools" => [], "stream" => true}))
    on_exit(fn -> Mint.HTTP.close(conn) end)
    assert_receive {:held_terminal_writer, writer, ^hold}, @timeout
    on_exit(fn -> send(writer, {:discard_terminal_writer, hold}) end)
    pending = await_pending!(owner, deadline())
    request_id = pending.active_turn.admission_request_id
    assert {:ok, %{response_usage: %{total_tokens: 120}}} = pending.active_turn.pending_result
    {:ok, lock} = Postgrex.start_link(Keyword.take(Repo.config(), [:hostname, :port, :username, :password, :database]))
    Process.unlink(lock)
    on_exit(fn -> if Process.alive?(lock), do: GenServer.stop(lock) end)
    Postgrex.query!(lock, "BEGIN", [])
    %{rows: [[backend]]} = Postgrex.query!(lock, "SELECT pg_backend_pid() FROM requests WHERE id=$1 FOR UPDATE", [Ecto.UUID.dump!(request_id)])
    drain = Task.async(fn -> RolloutDrain.start_drain([name: harness.name, timeout_ms: 5_000] ++ WebsocketRolloutDrainSupport.deadline_options(harness.deadline)) end)
    clock = harness.deadline
    assert_receive {:rollout_drain_deadline_wait, ^clock, _}, @timeout
    assert %{pending_producers: 1, entries: []} = ActivityRegistry.receipt_cohort(name: harness.activity_registry)
    VirtualDeadline.advance(clock, :sys.get_state(harness.name).deadline_ms)

    try do
      worker = await_sql_receipt_worker!(harness.activity_registry, System.monotonic_time(:millisecond) + 2_000)
      assert {:ok, %{active_turn?: true}} = WebsocketOwnerSession.owner_status(owner)
      assert worker != owner
      assert_sql_blocker!(backend, deadline())
      assert :ok = WebsocketOwnerSession.detach_downstream(owner, pending.downstream)
      assert [%{pid: ^worker, kind: :receipt_settlement}] = ActivityRegistry.activities(name: harness.activity_registry)
      state = :sys.get_state(owner)
      receipt = state.active_turn.receipt_settlement
      send(owner, {:websocket_owner_receipt_settled, make_ref(), receipt.ref, receipt.token, :inserted})
      WebsocketOwnerSession.detach_downstream(owner, pending.downstream)
      assert :sys.get_state(owner).active_turn.receipt_settlement == receipt

      if force_cut do
        worker_monitor = Process.monitor(worker)
        VirtualDeadline.advance(clock, 1_000)
        assert_receive {:DOWN, ^worker_monitor, :process, ^worker, :killed}, @timeout
        assert %{entries: [%{status: {:finished, :aborted}}]} = ActivityRegistry.receipt_cohort(name: harness.activity_registry)
      end
    after
      Postgrex.query!(lock, "ROLLBACK", [])
      GenServer.stop(lock)
    end

    if not force_cut do
      settlement = await_settlement!(request_id, deadline())
      assert settlement.total_tokens == 120
      assert {Repo.get!(Request, request_id).last_error_code, Repo.get!(Request, request_id).response_status_code} == {"owner_drained", 499}
      assert Repo.one!(from(t in CodexTurn, where: t.request_id == ^request_id)).status == "interrupted"
      assert Repo.aggregate(from(e in LedgerEntry, where: e.request_id == ^request_id and e.entry_kind == "settlement" and e.amount_status == "recorded"), :count) == 1
    end

    VirtualDeadline.advance(clock, 200)
    summary = Task.await(drain, @timeout)
    assert_sql_drain_outcome(summary, force_cut)
    assert %DateTime{} = Repo.get!(CodexSession, session.id).disconnected_at
    lease = Repo.get_by!(BridgeOwnerLease, codex_session_id: session.id, lease_token: session.owner_lease_token)
    assert lease.status == "released"
    assert lease.metadata["owner_exit_cause"] == "drain_cut"
    send(writer, {:discard_terminal_writer, hold})
    Mint.HTTP.close(conn)
    WebsocketCleanupFence.await_session_cleanups!()
    assert :ok = WebsocketRolloutDrainSupport.await_rollout_drain_harness(harness)
    record_evidence(%{scenario: "sql_held_actual_drain", force_cut: force_cut, request_id: request_id, blocker_backend: backend, total_tokens: 120, receipt_seen: summary.receipt_settlements_seen, receipt_completed: summary.receipt_settlements_completed, actual_sql_blocker_observed: true})
  end

  defp assert_sql_drain_outcome(summary, false) do
    assert %{result: :ok, receipt_settlements_seen: 1, receipt_settlements_completed: 1, receipt_settlements_aborted: 0, receipt_producers_failed?: false} = summary
  end

  defp assert_sql_drain_outcome(summary, true) do
    assert %{result: :error, receipt_settlements_seen: 1, receipt_settlements_completed: 0, receipt_settlements_aborted: 1, receipt_producers_failed?: false} = summary
  end

  defp await_sql_receipt_worker!(registry, limit) do
    case ActivityRegistry.activities(name: registry) do
      [%{kind: :receipt_settlement, pid: worker}] -> worker
      _ -> retry!(fn -> await_sql_receipt_worker!(registry, limit) end, limit, "held known receipt was not registered before blocked drain persistence")
    end
  end

  defp assert_sql_blocker!(backend, limit) do
    %{rows: [[count]]} = Repo.query!("SELECT count(*) FROM pg_stat_activity WHERE $1::integer = ANY(pg_blocking_pids(pid))", [backend])
    if count > 0, do: :ok, else: retry!(fn -> assert_sql_blocker!(backend, limit) end, limit, "no blocked SQL backend observed")
  end

  defp run_scenario(topology, mode, interruption) do
    if topology == :peer, do: enter_peer_owner_topology!(), else: OwnerCrashAfterSendScenario.per_process_connections!()
    assert_namespace!()
    hold = make_ref()
    upstream = start_upstream(FakeUpstream.websocket_text_frames([encode(completed())]))
    setup = gateway_setup(upstream)
    set_model_serving_mode!(model_serving_scope(), setup, mode)
    opts = [upstream: held_terminal_writer_boundary(self(), hold, "resp_held_receipt"), terminal_delivery_timeout_ms: 30_000]
    attrs = %{session_header: @window, session_header_source: "x-codex-window-id"}

    {session, owner} =
      if topology == :peer do
        peer = start_peer_session_owner!(setup, attrs, opts)
        {peer.session, peer.owner_pid}
      else
        register_unboxed_pool_cleanup!(setup)
        {:ok, auth} = Access.authenticate_authorization_header(setup.authorization)
        start_remote_session_owner!(auth, attrs, node(), :real, opts)
      end

    assert node(owner) != node() == (topology == :peer)
    owner_identity = :erpc.call(node(owner), Identity, :local, [])
    caller_identity = Identity.local()
    assert owner_identity.instance_id != caller_identity.instance_id == (topology == :peer)
    port = start_public_endpoint!()
    headers = socket_headers(mode)
    {conn, ws, ref, _headers} = public_websocket_connect_with_request_headers!(port, setup, Ecto.UUID.generate(), "/backend-api/codex/responses", headers)
    {conn, ws} = public_websocket_send_text!(conn, ws, ref, encode(%{"type" => "response.create", "model" => setup.model.exposed_model_id, "instructions" => "synthetic instructions", "input" => [%{"role" => "user", "content" => "synthetic question"}], "tools" => [], "stream" => true}))
    assert_receive {:held_terminal_writer, writer, ^hold}, @timeout
    writer_monitor = Process.monitor(writer)
    on_exit(fn -> send(writer, {:discard_terminal_writer, hold}) end)
    pending = await_pending!(owner, deadline())
    assert {:ok, %{response_usage: usage}} = pending.active_turn.pending_result
    assert {usage.status, usage.input_tokens, usage.output_tokens, usage.total_tokens} == {"usage_known", 100, 20, 120}
    refute pending.active_turn.terminal_forwarded?
    request_id = pending.active_turn.admission_request_id
    attempt_id = pending.active_turn.admission_attempt_id
    assert is_binary(request_id) and is_binary(attempt_id)
    admitted_identity = Repo.get!(Attempt, attempt_id) |> Map.take([:id, :request_id, :replay_generation, :owner_instance_id, :owner_instance_boot_id, :owner_process_id, :owner_execution_id])
    # A second state call proves the owner keeps serving messages while only
    # the terminal writer is parked; the real parser result is already held.
    assert :sys.get_state(owner).active_turn.ref == pending.active_turn.ref

    if interruption == :delivered do
      send(writer, {:release_terminal_writer, hold})
      {conn, _ws, text} = public_websocket_receive_text!(conn, ws, ref)
      assert CodexPooler.JSON.decode!(text)["type"] == "response.completed"
      await_request!(request_id, "succeeded", deadline())
      Mint.HTTP.close(conn)
    else
      interrupt!(interruption, owner, pending)
      await_request!(request_id, "failed", deadline())
      send(writer, {:discard_terminal_writer, hold})
      Mint.HTTP.close(conn)
    end

    assert_receive {:DOWN, ^writer_monitor, :process, ^writer, :normal}, @timeout
    WebsocketCleanupFence.await_session_cleanups!()
    status = if interruption == :delivered, do: "succeeded", else: "failed"
    request = await_request!(request_id, status, deadline())
    entry = await_settlement!(request_id, deadline())
    assert {entry.attempt_id, entry.usage_status, entry.input_tokens, entry.output_tokens, entry.total_tokens} == {attempt_id, "usage_known", 100, 20, 120}
    assert Repo.aggregate(from(e in LedgerEntry, where: e.request_id == ^request_id and e.entry_kind == "settlement" and e.amount_status == "recorded"), :count) == 1
    attempt = Repo.get!(Attempt, attempt_id)
    turn = Repo.one!(from(t in CodexTurn, where: t.request_id == ^request_id))
    assert Map.take(attempt, Map.keys(admitted_identity)) == admitted_identity
    assert attempt.status == status
    assert turn.status == expected_turn_status(topology, interruption)
    assert {request.last_error_code, request.response_status_code} == expected_failure(topology, interruption)
    assert turn.final_attempt_id == attempt_id
    assert request.request_metadata["routing"]["model_serving_mode"] == mode
    assert FakeUpstream.count(upstream) == 1

    # A separate physical PostgreSQL connection proves these are committed,
    # not reads inside the fixture's transaction or the receipt worker's one.
    {:ok, db} = Postgrex.start_link(Keyword.take(Repo.config(), [:hostname, :port, :username, :password, :database]))

    snapshot =
      try do
        %{columns: columns, rows: [row]} = Postgrex.query!(db, "SELECT pg_backend_pid() AS observer_backend, r.id::text AS request_id, r.status AS request_status, r.last_error_code, r.response_status_code, a.id::text AS attempt_id, a.status AS attempt_status, a.replay_generation, t.status AS turn_status, l.id::text AS settlement_id, l.usage_status, l.input_tokens, l.output_tokens, l.total_tokens, l.settled_cost_micros::text FROM requests r JOIN attempts a ON a.request_id=r.id JOIN codex_turns t ON t.request_id=r.id JOIN ledger_entries l ON l.request_id=r.id AND l.attempt_id=a.id WHERE r.id=$1 AND l.entry_kind='settlement' AND l.amount_status='recorded'", [Ecto.UUID.dump!(request_id)])
        snapshot = Map.new(Enum.zip(columns, row))
        assert {snapshot["request_status"], snapshot["attempt_status"], snapshot["attempt_id"], snapshot["total_tokens"], snapshot["usage_status"]} == {status, status, attempt_id, 120, "usage_known"}
        assert snapshot["turn_status"] == turn.status
        snapshot
      after
        GenServer.stop(db)
      end

    successor = assert_successor!(upstream, port, setup, headers, request_id, entry, attempt, turn)
    record_evidence(%{endpoint_port: port, fixture_port: URI.parse(FakeUpstream.url(upstream)).port, original_execution_identity: admitted_identity, owner_pid: inspect(owner), postgres_snapshot: snapshot, successor_request_id: successor.id, successor_tokens: 5, predecessor_unchanged_after_successor: true, topology: topology, mode: mode, interruption: interruption, request_id: request_id, attempt_id: attempt_id, replay_generation: attempt.replay_generation, settlement_id: entry.id, usage_status: entry.usage_status, total_tokens: entry.total_tokens, error_code: request.last_error_code, response_status_code: request.response_status_code, request_status: request.status, attempt_status: attempt.status, turn_status: turn.status, owner_node: Atom.to_string(node(owner)), caller_node: Atom.to_string(node()), owner_instance_id: session.owner_instance_id, owner_live_instance_id: owner_identity.instance_id, caller_live_instance_id: caller_identity.instance_id, database: Repo.config()[:database], committed_independent_connection: true, provider_generations: 2})
  end

  defp assert_successor!(upstream, port, setup, headers, request_id, entry, attempt, turn) do
    successor_frame = put_in(completed(), ["response", "id"], "resp_held_successor") |> put_in(["response", "usage"], %{"input_tokens" => 3, "output_tokens" => 2, "total_tokens" => 5})
    FakeUpstream.set_mode(upstream, FakeUpstream.websocket_text_frames([encode(successor_frame)]))
    {conn, ws, ref, _} = public_websocket_connect_with_request_headers!(port, setup, Ecto.UUID.generate(), "/backend-api/codex/responses", headers)
    {conn, ws} = public_websocket_send_text!(conn, ws, ref, encode(%{"type" => "response.create", "model" => setup.model.exposed_model_id, "instructions" => "synthetic instructions", "input" => [%{"role" => "user", "content" => "synthetic successor"}], "tools" => [], "stream" => true}))
    {conn, _ws, text} = public_websocket_receive_text!(conn, ws, ref)
    assert get_in(CodexPooler.JSON.decode!(text), ["response", "id"]) == "resp_held_successor"
    successor = Repo.one!(from(r in Request, where: r.pool_id == ^setup.pool.id and r.id != ^request_id))
    successor = await_request!(successor.id, "succeeded", deadline())
    assert await_settlement!(successor.id, deadline()).total_tokens == 5
    Mint.HTTP.close(conn)
    WebsocketCleanupFence.await_session_cleanups!()
    assert Repo.get!(LedgerEntry, entry.id) == entry
    assert Repo.get!(Attempt, attempt.id).status == attempt.status
    assert Repo.get!(CodexTurn, turn.id).status == turn.status
    assert FakeUpstream.count(upstream) == 2
    successor
  end

  defp assert_namespace! do
    if namespace = System.get_env("CODEX_POOLER_TEST_RUN_NAMESPACE"), do: assert(Repo.config()[:database] =~ namespace)
  end

  defp socket_headers("lite"), do: [{"x-openai-internal-codex-responses-lite", "true"}, {"x-codex-window-id", @window}]
  defp socket_headers(_mode), do: [{"x-codex-window-id", @window}]

  defp expected_failure(_topology, :delivered), do: {nil, 200}
  defp expected_failure(_topology, :timeout), do: {"upstream_stream_error", 502}
  defp expected_failure(:peer, :submitter_down), do: {"owner_crashed", 502}
  defp expected_failure(_topology, :retirement), do: {"owner_crashed", 499}
  defp expected_failure(_topology, :drain), do: {"owner_drained", 499}
  defp expected_failure(_topology, _interruption), do: {"client_disconnected", 499}

  # Timeout is a transport failure; a peer submitter is the ERPC call
  # process, whose death the live caller classifies as owner_crashed. Killing
  # the local submitter instead kills the socket's own response task.
  defp expected_turn_status(_topology, :delivered), do: "succeeded"
  defp expected_turn_status(_topology, :timeout), do: "failed"
  defp expected_turn_status(_topology, _interruption), do: "interrupted"

  defp interrupt!(:detach, owner, pending), do: assert(:ok = WebsocketOwnerSession.detach_downstream(owner, pending.downstream))

  defp interrupt!(:submitter_down, _owner, pending) do
    {submitter, _tag} = pending.active_turn.reply_to
    monitor = Process.monitor(submitter)
    Process.exit(submitter, :kill)
    assert_receive {:DOWN, ^monitor, :process, ^submitter, :killed}, @timeout
  end

  defp interrupt!(:timeout, owner, pending) do
    {turn_ref, timer_token} = pending.active_turn.terminal_delivery_timeout
    send(owner, {:websocket_owner_terminal_delivery_timeout, turn_ref, timer_token})
  end

  defp interrupt!(:drain, owner, _pending), do: assert(:ok = WebsocketOwnerSession.drain_owner(owner))

  defp interrupt!(:retirement, _owner, pending) do
    upstream = pending.upstream_pid
    monitor = Process.monitor(upstream)
    Process.exit(upstream, :kill)
    assert_receive {:DOWN, ^monitor, :process, ^upstream, :killed}, @timeout
  end

  defp await_pending!(owner, limit) do
    state = :sys.get_state(owner)
    if match?(%{active_turn: %{pending_result: {:ok, _}}}, state), do: state, else: retry!(fn -> await_pending!(owner, limit) end, limit, "normalized result never became pending")
  end

  defp await_request!(id, status, limit) do
    row = Repo.get!(Request, id)
    if row.status == status, do: row, else: retry!(fn -> await_request!(id, status, limit) end, limit, "request did not finalize as #{status}")
  end

  defp await_settlement!(id, limit) do
    row = Repo.one(from(e in LedgerEntry, where: e.request_id == ^id and e.entry_kind == "settlement" and e.amount_status == "recorded"))
    if row && row.usage_status == "usage_known", do: row, else: retry!(fn -> await_settlement!(id, limit) end, limit, "held usage did not commit")
  end

  defp retry!(fun, limit, message) do
    assert System.monotonic_time(:millisecond) < limit, message

    receive do
    after
      10 -> fun.()
    end
  end

  defp deadline, do: System.monotonic_time(:millisecond) + @timeout
  defp completed, do: %{"type" => "response.completed", "response" => %{"id" => "resp_held_receipt", "status" => "completed", "output" => [], "usage" => %{"input_tokens" => 100, "output_tokens" => 20, "total_tokens" => 120}}}
  defp encode(value), do: CodexPooler.JSON.encode!(value)

  defp record_evidence(receipt) do
    if path = System.get_env("CODEX_POOLER_HELD_USAGE_EVIDENCE"), do: File.write!(path, encode(receipt) <> "\n", [:append])
  end
end
