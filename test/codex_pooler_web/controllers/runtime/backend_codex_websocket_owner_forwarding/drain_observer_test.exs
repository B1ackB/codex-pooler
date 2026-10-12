defmodule CodexPoolerWeb.Runtime.BackendCodexWebsocketOwnerForwarding.DrainObserverTest do
  use CodexPoolerWeb.ConnCase, async: false

  @moduletag capture_log: true
  import CodexPoolerWeb.Runtime.BackendCodexTestSupport
  import CodexPoolerWeb.Runtime.BackendCodexWebsocketOwnerForwardingSupport
  import ExUnit.CaptureLog

  alias CodexPooler.Access
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway.Persistence.{BridgeOwnerLease, SessionContinuity}
  alias CodexPooler.Gateway.Transports.Websocket.{RolloutDrain, UpstreamWebsocketSession, WebsocketOwnerSession}
  alias CodexPooler.Gateway.Transports.WebsocketRolloutDrainSupport
  alias CodexPooler.Gateway.Websocket, as: Gateway
  alias CodexPooler.Repo
  alias CodexPoolerWeb.Runtime.OwnerCrashAfterSendScenario

  @timeout 15_000

  setup do
    CodexPooler.TestAppEnv.restore_on_exit(:websocket_owner_forwarding_enabled, nil)
    Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, true)
    :ok
  end

  for topology <- [:local, :peer] do
    @tag topology: topology
    if topology == :peer, do: @tag(slow: "actual second BEAM with committed lease release")
    test "exact drain completion crosses the real #{topology} owner boundary", do: successful_observation(unquote(topology))
  end

  defp successful_observation(topology) do
    {session, owner, _setup} = owner_fixture(topology)
    ref = observe(owner)
    monitor = Process.monitor(owner)
    assert :ok = WebsocketOwnerSession.drain_owner(owner)
    assert_receive {:websocket_owner_drain_finished, ^owner, ^ref, :ok}, @timeout
    assert_receive {:DOWN, ^monitor, :process, ^owner, :normal}, @timeout
    assert Repo.get_by!(BridgeOwnerLease, codex_session_id: session.id, lease_token: session.owner_lease_token).status == "released"
    record(%{scenario: "exact_owner_observer", topology: topology, owner_node: Atom.to_string(node(owner)), observer_node: Atom.to_string(node()), owner_exit: "normal", completion: "ok", original_lease_released: true})
  end

  test "malformed observer references do not enroll shutdown observers" do
    {_session, owner, _setup} = owner_fixture(:local)
    clock = :sys.get_state(RolloutDrain).drain_policy.now_ms

    for ref <- [nil, "invalid", %{}] do
      assert {:ok, _} = GenServer.call(owner, {:local_drain_status, %{at: clock.() + @timeout, now_ms: clock, observer_ref: ref}}, @timeout)
    end

    assert :sys.get_state(owner).drain_observers == %{}
  end

  test "replaced observer references and cancelled observation cannot receive completion" do
    {_session, owner, _setup} = owner_fixture(:local)
    old = observe(owner)
    current = observe(owner)
    assert map_size(:sys.get_state(owner).drain_observers) == 1
    GenServer.cast(owner, {:cancel_drain_observation, self(), old})
    assert :sys.get_state(owner).drain_observers[self()].ref == current
    GenServer.cast(owner, {:cancel_drain_observation, self(), current})
    assert :sys.get_state(owner).drain_observers == %{}
    monitor = Process.monitor(owner)
    assert :ok = WebsocketOwnerSession.drain_owner(owner)
    assert_receive {:DOWN, ^monitor, :process, ^owner, :normal}, @timeout
    refute_received {:websocket_owner_drain_finished, ^owner, ^old, _}
    refute_received {:websocket_owner_drain_finished, ^owner, ^current, _}
  end

  test "observer process death removes its reference while owner stays alive" do
    {_session, owner, _setup} = owner_fixture(:local)
    parent = self()

    task =
      Task.async(fn ->
        ref = observe(owner)
        send(parent, {:observer_registered, self(), ref})

        receive do
          :finish -> :ok
        end
      end)

    observer = task.pid
    assert_receive {:observer_registered, ^observer, _ref}, @timeout
    assert Map.has_key?(:sys.get_state(owner).drain_observers, observer)
    send(observer, :finish)
    Task.await(task, @timeout)
    await_no_observers(owner, System.monotonic_time(:millisecond) + @timeout)
    assert Process.alive?(owner)
  end

  for reason <- [:normal, :kill] do
    test "an arbitrary #{reason} owner exit is not graceful drain evidence", do: unrequested_exit(unquote(reason))
  end

  defp unrequested_exit(reason) do
    {_session, owner, _setup} = owner_fixture(:local)
    ref = observe(owner)
    upstream = :sys.get_state(owner).upstream_pid
    on_exit(fn -> if Process.alive?(upstream), do: UpstreamWebsocketSession.close(upstream) end)
    monitor = Process.monitor(owner)
    if reason == :normal, do: GenServer.stop(owner, :normal), else: Process.exit(owner, :kill)
    assert_receive {:DOWN, ^monitor, :process, ^owner, _}, @timeout
    refute_received {:websocket_owner_drain_finished, ^owner, ^ref, :ok}
  end

  test "actual SQL release failure stays failed even when recovery runs" do
    {_session, owner, _setup} = owner_fixture(:local)
    ref = observe(owner)

    inject_release_sql_failure(owner)

    monitor = Process.monitor(owner)

    log =
      capture_log(fn ->
        assert :ok = WebsocketOwnerSession.drain_owner(owner)
        assert_receive {:websocket_owner_drain_finished, ^owner, ^ref, {:error, :shutdown_incomplete}}, @timeout
        assert_receive {:DOWN, ^monitor, :process, ^owner, :normal}, @timeout
      end)

    assert log =~ "release_owner_lease"
    assert log =~ "reason_class=Postgrex.Error"
    assert log =~ "failure_reason=stale_owner_cleanup"
    assert_receive {:release_sql_failure, :division_by_zero}
    record(%{scenario: "checked_release_sql_failure", postgres_code: "division_by_zero", completion: "shutdown_incomplete", owner_exit: "normal"})
  end

  test "the actual coordinator rejects a successful drain call with a failed checked release" do
    {_session, owner, _setup} = owner_fixture(:local)
    harness = WebsocketRolloutDrainSupport.start_rollout_drain_harness(self())
    inject_release_sql_failure(owner)

    log =
      capture_log(fn ->
        summary = RolloutDrain.start_drain([name: harness.name, timeout_ms: 5_000] ++ WebsocketRolloutDrainSupport.deadline_options(harness.deadline))
        assert %{result: :error, owners_seen: 1, owners_failed: 1, owners_drained: 0} = summary
        assert_receive {:release_sql_failure, :division_by_zero}
        record(%{scenario: "coordinator_checked_sql_failure", postgres_code: "division_by_zero", summary: summary})
      end)

    assert log =~ "reason_class=Postgrex.Error"
    assert :ok = WebsocketRolloutDrainSupport.await_rollout_drain_harness(harness)
  end

  test "a live upstream after the close callback is unfinished shutdown" do
    {_session, owner, _setup} = owner_fixture(:local)
    ref = observe(owner)
    upstream = :sys.get_state(owner).upstream_pid
    on_exit(fn -> if Process.alive?(upstream), do: UpstreamWebsocketSession.close(upstream) end)
    :sys.replace_state(owner, fn state -> put_in(state.callbacks.upstream_closer, fn _ -> :ok end) end)
    monitor = Process.monitor(owner)
    assert :ok = WebsocketOwnerSession.drain_owner(owner)
    assert_receive {:websocket_owner_drain_finished, ^owner, ^ref, {:error, :shutdown_incomplete}}, @timeout
    assert_receive {:DOWN, ^monitor, :process, ^owner, :normal}, @timeout
    assert Process.alive?(upstream)
    UpstreamWebsocketSession.close(upstream)
  end

  test "an already released original lease does not release a successor lease" do
    {session, owner, setup} = owner_fixture(:local)
    ref = observe(owner)
    assert :ok = SessionContinuity.release_owner_lease(session, session.owner_lease_token, "owner_unavailable_takeover")
    {:ok, auth} = Access.authenticate_authorization_header(setup.authorization)
    {:ok, successor} = Gateway.start_codex_session(auth, %{session_header: setup.session_header, session_header_source: "x-codex-window-id", owner_instance_id: "synthetic-successor"})
    assert successor.owner_lease_token != session.owner_lease_token
    before = Repo.get_by!(BridgeOwnerLease, codex_session_id: successor.id, lease_token: successor.owner_lease_token)
    monitor = Process.monitor(owner)
    assert :ok = WebsocketOwnerSession.drain_owner(owner)
    assert_receive {:websocket_owner_drain_finished, ^owner, ^ref, :ok}, @timeout
    assert_receive {:DOWN, ^monitor, :process, ^owner, :normal}, @timeout
    assert Repo.get!(BridgeOwnerLease, before.id) == before
    assert before.status == "active"
  end

  defp inject_release_sql_failure(owner) do
    observer = self()

    :sys.replace_state(owner, fn state ->
      put_in(state.persistence.release_owner_lease, fn _session, _token, _reason, _cause ->
        {:error, %Postgrex.Error{postgres: %{code: code}}} = result = Repo.query("SELECT 1 / 0")
        send(observer, {:release_sql_failure, code})
        result
      end)
    end)
  end

  defp owner_fixture(topology) do
    if topology == :peer, do: enter_peer_owner_topology!(), else: OwnerCrashAfterSendScenario.per_process_connections!()
    upstream = start_upstream(FakeUpstream.websocket_text_frames([]))
    setup = gateway_setup(upstream)
    session_header = Ecto.UUID.generate() <> ":0"
    attrs = %{session_header: session_header, session_header_source: "x-codex-window-id"}

    {session, owner} =
      if topology == :peer do
        peer = start_peer_session_owner!(setup, attrs)
        {peer.session, peer.owner_pid}
      else
        register_unboxed_pool_cleanup!(setup)
        {:ok, auth} = Access.authenticate_authorization_header(setup.authorization)
        start_remote_session_owner!(auth, attrs, node(), :real)
      end

    {session, owner, Map.put(setup, :session_header, session_header)}
  end

  defp observe(owner) do
    ref = make_ref()
    clock = :sys.get_state(RolloutDrain).drain_policy.now_ms
    assert {:ok, _status} = GenServer.call(owner, {:local_drain_status, %{at: clock.() + @timeout, now_ms: clock, observer_ref: ref}}, @timeout)
    assert_receive {:websocket_owner_drain_observing, ^owner, ^ref}, @timeout
    ref
  end

  defp await_no_observers(owner, limit) do
    unless :sys.get_state(owner).drain_observers == %{} do
      assert System.monotonic_time(:millisecond) < limit

      receive do
      after
        10 -> await_no_observers(owner, limit)
      end
    end
  end

  defp record(receipt) do
    if path = System.get_env("CODEX_POOLER_HELD_USAGE_EVIDENCE"), do: File.write!(path, CodexPooler.JSON.encode!(receipt) <> "\n", [:append])
  end
end
