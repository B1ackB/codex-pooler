defmodule CodexPoolerWeb.Runtime.BackendCodexWebsocketOwnerForwarding.OwnerDeathTest do
  use CodexPoolerWeb.ConnCase, async: false

  @moduletag capture_log: true

  import Ecto.Query
  import ExUnit.CaptureLog

  import CodexPoolerWeb.Runtime.BackendCodexTestSupport
  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport
  import CodexPoolerWeb.Runtime.BackendCodexWebsocketOwnerForwardingSupport

  alias CodexPooler.Access
  alias CodexPooler.Access.APIKey
  alias CodexPooler.Accounting.Attempt
  alias CodexPooler.Accounting.ClientRetry
  alias CodexPooler.Accounting.LedgerEntry
  alias CodexPooler.Accounting.Request
  alias CodexPooler.Accounting.RequestClientRetryLink
  alias CodexPooler.Accounting.RequestLogFact
  alias CodexPooler.Accounting.RequestReplayEntitlement
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway.Persistence.BridgeOwnerLease
  alias CodexPooler.Gateway.Persistence.BridgeSessionAlias
  alias CodexPooler.Gateway.Persistence.CodexSession
  alias CodexPooler.Gateway.Persistence.CodexTurn
  alias CodexPooler.Gateway.Transports.Websocket.ActivityRegistry
  alias CodexPooler.Gateway.Transports.Websocket.UpstreamWebsocketSession.TerminalDiscriminator
  alias CodexPooler.Gateway.Transports.Websocket.WebsocketOwnerSession
  alias CodexPooler.Gateway.Transports.WebsocketOwnerNodeHarness
  alias CodexPooler.Gateway.Websocket, as: Gateway
  alias CodexPooler.Gateway.Websocket.Adapter
  alias CodexPooler.Platform.{ExecutionIdentity, ExecutionRegistry, ExecutionTerminalProof, ExecutionTerminalProofs}
  alias CodexPooler.Pools.ModelServingOverride
  alias CodexPooler.Repo
  alias CodexPooler.Upstreams.Schemas.UpstreamIdentity
  alias CodexPoolerWeb.CodexResponsesSocket
  alias CodexPoolerWeb.Runtime.BackendCodexWebsocketOwnerForwardingSupport.ReplayRemoteNodeClient
  alias CodexPoolerWeb.Runtime.BackendCodexWebsocketOwnerForwardingSupport.TurnBudgetNodeClient
  alias CodexPoolerWeb.Runtime.OwnerCrashAfterSendScenario
  alias CodexPoolerWeb.Runtime.WebsocketCleanupFence
  alias Ecto.Adapters.SQL.Sandbox

  # Failure-detection budget for an expected message: a green run returns as
  # soon as the message arrives, so only a missing one spends it.
  @detection_timeout_ms 15_000

  setup do
    CodexPooler.TestAppEnv.restore_on_exit(:websocket_owner_forwarding_enabled)
    Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, true)

    on_exit(fn ->
      TurnBudgetNodeClient.reset()
      ReplayRemoteNodeClient.reset()
    end)
  end

  @tag :committed_cleanup_jobs
  test "owner-death fixture teardown removes owned jobs and preserves a shared identity job" do
    upstream = start_upstream(FakeUpstream.json_response(%{}))
    setup = Sandbox.unboxed_run(Repo, fn -> gateway_setup(upstream) end)

    cleanup = fn ->
      purge_committed_pool_rows!(setup.pool.id, setup.identity.id, setup.pricing.id)
    end

    on_exit(cleanup)

    CodexPooler.CommittedJobCleanupSupport.assert_cleanup_jobs!(
      setup.pool,
      setup.identity,
      setup.assignment,
      cleanup
    )
  end

  test "remote owner loss before visible output recovers without re-resolving the turn mode" do
    upstream =
      start_upstream(
        FakeUpstream.json_response(%{
          "id" => "resp_owner_mode_loss_recovered",
          "object" => "response"
        })
      )

    setup = gateway_setup(upstream)
    scope = model_serving_scope()
    revision = set_model_serving_mode!(scope, setup, "full")
    {:ok, auth} = Access.authenticate_authorization_header(setup.authorization)
    {:ok, state} = owner_socket(auth, "ws-owner-mode-loss", "owner-mode-loss")
    remote_node = :"codex_pooler@lost-mode-owner.example"

    base_node_opts =
      WebsocketOwnerNodeHarness.node_client_opts([remote_node],
        calls: %{remote_node => :success}
      )

    remote_state = remote_owner_state(state, remote_node, base_node_opts)
    release_ref = make_ref()
    parent = self()

    try do
      lost_turn =
        Task.async(fn ->
          WebsocketOwnerNodeHarness.with_node_client(
            [remote_node],
            [
              calls: %{
                remote_node => {:barrier_return, parent, release_ref, {:error, :owner_unavailable}}
              },
              notify: parent,
              capture_request_to: parent
            ],
            fn node_opts ->
              Gateway.run_websocket_response(
                auth,
                model_serving_owner_payload(setup, "remote-owner-loss", "client-true"),
                owner_response_options(remote_state, node_opts),
                fn _data -> :ok end
              )
            end
          )
        end)

      assert_remote_submit_request_v8!(remote_state, remote_node)

      assert_receive {:websocket_owner_harness_call_barrier, rpc_pid, ^release_ref, :remote_submit_request_v8},
                     @detection_timeout_ms

      try do
        _revision = set_model_serving_mode!(scope, setup, "lite", revision)
        send(rpc_pid, {:websocket_owner_harness_release_call, release_ref})

        assert :ok = Task.await(lost_turn, 3_000)
      after
        send(rpc_pid, {:websocket_owner_harness_release_call, release_ref})
      end

      original_downstream = remote_state.websocket_owner_downstream

      assert_receive {:websocket_owner_frame, correlation_id, recovered_epoch, {:data, recovered_metadata_frame}},
                     @detection_timeout_ms

      assert correlation_id == original_downstream.correlation_id
      assert recovered_epoch > original_downstream.epoch

      assert %{
               "type" => "codex.response.metadata",
               "headers" => %{"x-models-etag" => _models_etag}
             } = CodexPooler.JSON.decode!(recovered_metadata_frame)

      assert_receive {:websocket_owner_frame, ^correlation_id, ^recovered_epoch, {:data, recovered_frame}},
                     @detection_timeout_ms

      assert owner_response_id(recovered_frame) == "resp_owner_mode_loss_recovered"

      assert_receive {:websocket_owner_frame, ^correlation_id, ^recovered_epoch, :complete},
                     @detection_timeout_ms

      assert [recovered_upstream_request] = FakeUpstream.requests(upstream)
      assert_canonical_full_owner_request!(recovered_upstream_request)

      assert [request] = request_logs(setup.pool.id)
      assert request.retry_count == 0
      assert request.last_error_code == nil
      assert_owner_mode_accounting!(request, "full", "succeeded", remote_node)
    after
      CodexResponsesSocket.terminate(:closed, remote_state)
    end
  end

  # The owner dies before the turn's payload left (its upstream session is
  # held at the payload write, before the forwarder's observer marks the turn
  # started), so the forwarder hands the turn to a replacement owner, which
  # sends it once. A turn whose payload had started to leave is never handed
  # over (findings#327, `owner_crash_after_send_test.exs`); this test used to
  # kill the owner after the provider had the payload and counted the
  # replacement's second send of it as the recovery.
  @tag :owner_crash_recovery
  test "replacement owner preserves the active proxy epoch after pre-visible owner death" do
    hold_ref = make_ref()

    upstream =
      start_upstream(
        # Strict finite scenario: the replacement owner sends exactly one lite
        # turn, the one its predecessor never wrote, and the next socket sends
        # exactly one full turn.
        # provenance: synthetic_adversarial
        FakeUpstream.strict_sequence([
          FakeUpstream.expect_request(
            method: "WEBSOCKET",
            json: [valid: true, equals: %{"type" => "response.create"}],
            respond:
              FakeUpstream.websocket_text_frames([
                CodexPooler.JSON.encode!(%{
                  "id" => "resp_owner_mode_kill_recovered",
                  "object" => "response"
                })
              ])
          ),
          FakeUpstream.expect_request(
            method: "WEBSOCKET",
            json: [valid: true, equals: %{"type" => "response.create"}],
            respond:
              FakeUpstream.websocket_text_frames([
                CodexPooler.JSON.encode!(%{
                  "id" => "resp_owner_mode_kill_next_turn",
                  "object" => "response"
                })
              ])
          )
        ])
      )

    setup = gateway_setup(upstream)
    scope = model_serving_scope()
    revision = set_model_serving_mode!(scope, setup, "lite")
    {:ok, auth} = Access.authenticate_authorization_header(setup.authorization)
    held_write = OwnerCrashAfterSendScenario.held_write_boundary(self(), hold_ref, :linked, :before_mark)
    {:ok, state} = owner_socket(auth, "ws-owner-mode-kill", "owner-mode-kill", websocket_owner_forwarder_opts: [upstream: held_write])
    remote_node = :"codex_pooler@killed-mode-owner.example"

    base_node_opts =
      WebsocketOwnerNodeHarness.node_client_opts([remote_node],
        calls: %{remote_node => :success}
      )

    stale_downstream = state.websocket_owner_downstream
    {:ok, old_owner_pid} = WebsocketOwnerSession.lookup(state.codex_session.id)

    assert {:ok, active_downstream} =
             WebsocketOwnerSession.attach_downstream(old_owner_pid, %{
               pid: self(),
               correlation_id: stale_downstream.correlation_id
             })

    assert stale_downstream.epoch == 1
    assert active_downstream.epoch == 2

    active_state = %{state | websocket_owner_downstream: active_downstream}
    remote_state = remote_owner_state(active_state, remote_node, base_node_opts)
    old_lease = active_owner_lease(state.codex_session.id)
    old_owner_ref = Process.monitor(old_owner_pid)
    parent = self()

    stale_frame = CodexPooler.JSON.encode!(%{"id" => "resp_stale_owner_attachment"})

    assert {:ok, ^remote_state} =
             CodexResponsesSocket.handle_info(
               {:websocket_owner_frame, stale_downstream.correlation_id, stale_downstream.epoch, {:data, stale_frame}},
               remote_state
             )

    try do
      interrupted_turn =
        Task.async(fn ->
          WebsocketOwnerNodeHarness.with_node_client(
            [remote_node],
            [
              calls: %{remote_node => :success},
              notify: parent,
              capture_request_to: parent
            ],
            fn node_opts ->
              Gateway.run_websocket_response(
                auth,
                model_serving_owner_payload(setup, "remote-owner-kill", "client-false"),
                owner_response_options(remote_state, node_opts),
                fn _data -> :ok end
              )
            end
          )
        end)

      assert_remote_submit_request_v8!(remote_state, remote_node)

      assert_receive {:payload_write_held, held_session, ^hold_ref, :unmarked}, @detection_timeout_ms
      held_session_monitor = Process.monitor(held_session)

      try do
        assert FakeUpstream.count(upstream) == 0

        assert [in_progress_request] = request_logs(setup.pool.id)
        assert in_progress_request.status == "in_progress"
        assert in_progress_request.retry_count == 0

        assert [in_progress_attempt] =
                 Repo.all(from(a in Attempt, where: a.request_id == ^in_progress_request.id))

        assert in_progress_attempt.status == "in_progress"

        assert in_progress_turn =
                 Repo.one!(from(t in CodexTurn, where: t.request_id == ^in_progress_request.id))

        assert in_progress_turn.status == "in_progress"
        assert is_nil(in_progress_turn.first_visible_output_at)
        assert active_owner_lease(state.codex_session.id).lease_token == old_lease.lease_token

        _revision = set_model_serving_mode!(scope, setup, "full", revision)

        Process.exit(old_owner_pid, :kill)
        assert_receive {:DOWN, ^old_owner_ref, :process, ^old_owner_pid, :killed}, @detection_timeout_ms
        # The held upstream session goes with its owner, before its write.
        assert_receive {:DOWN, ^held_session_monitor, :process, ^held_session, :killed}, @detection_timeout_ms

        assert :ok = Task.await(interrupted_turn, 3_000)

        assert_receive {:websocket_owner_runtime_recovered, correlation_id, epoch, runtime},
                       @detection_timeout_ms

        assert correlation_id == active_downstream.correlation_id
        assert epoch == active_downstream.epoch

        assert {:ok, recovered_remote_state} =
                 CodexResponsesSocket.handle_info(
                   {:websocket_owner_runtime_recovered, correlation_id, epoch, runtime},
                   remote_state
                 )

        refute recovered_remote_state.websocket_owner_lease_token ==
                 remote_state.websocket_owner_lease_token

        assert FakeUpstream.count(upstream) == 1
        assert [recovered_request] = request_logs(setup.pool.id)
        assert_owner_mode_accounting!(recovered_request, "lite", "succeeded", remote_node)

        assert recovered_turn =
                 Repo.one!(from(t in CodexTurn, where: t.request_id == ^recovered_request.id))

        assert recovered_turn.status == "succeeded"
        refute is_nil(recovered_turn.first_visible_output_at)

        assert {:push, {:text, recovered_frame}, recovered_remote_state} =
                 receive_owner_socket_push(recovered_remote_state)

        assert owner_response_id(recovered_frame) == "resp_owner_mode_kill_recovered"

        # The recovered response reaches the client once: the frame above, then nothing but the turn's completion. The helper that
        # waits for the completion used to drop every frame it met on the way, so a duplicate of the recovered frame passed
        # (findings#303 row 303-13); the owner's messages arrive in order, so this list is complete once `:complete` came.
        assert {{:ok, recovered_remote_state}, frames_after_recovered} =
                 receive_owner_socket_complete_frames(recovered_remote_state)

        assert frames_after_recovered == []

        active_correlation_id = active_downstream.correlation_id
        active_epoch = active_downstream.epoch

        refute_receive {:websocket_owner_frame, ^active_correlation_id, ^active_epoch, _payload},
                       100

        replacement_session = Repo.get!(CodexSession, state.codex_session.id)
        replacement_lease = active_owner_lease(state.codex_session.id)
        released_lease = Repo.get!(BridgeOwnerLease, old_lease.id)

        assert released_lease.status == "released"
        assert released_lease.metadata["release_reason"] == "owner_unavailable_takeover"
        assert replacement_lease.lease_token != old_lease.lease_token
        assert replacement_lease.lease_token == replacement_session.owner_lease_token

        assert recovered_remote_state.websocket_owner_lease_token ==
                 replacement_session.owner_lease_token

        assert recovered_remote_state.codex_session.owner_lease_token ==
                 replacement_session.owner_lease_token

        assert replacement_lease.owner_instance_id == replacement_session.owner_instance_id

        assert {:ok, replacement_owner_pid} =
                 WebsocketOwnerSession.lookup(state.codex_session.id)

        assert replacement_owner_pid != old_owner_pid

        replacement_owner_state = :sys.get_state(replacement_owner_pid)
        assert replacement_owner_state.owner_lease_token == replacement_lease.lease_token
        assert replacement_owner_state.downstream == active_downstream

        {:ok, next_state} =
          owner_socket(auth, "ws-owner-mode-kill-next", "owner-mode-kill")

        next_remote_state = remote_owner_state(next_state, remote_node, base_node_opts)

        try do
          assert :ok =
                   WebsocketOwnerNodeHarness.with_node_client(
                     [remote_node],
                     [
                       calls: %{remote_node => :success},
                       notify: self(),
                       capture_request_to: self()
                     ],
                     fn node_opts ->
                       Gateway.run_websocket_response(
                         auth,
                         model_serving_owner_payload(
                           setup,
                           "remote-owner-kill-next",
                           "client-true"
                         ),
                         owner_response_options(next_remote_state, node_opts),
                         fn _data -> :ok end
                       )
                     end
                   )

          assert_remote_submit_request_v8!(next_remote_state, remote_node)

          assert {:push, {:text, next_frame}, next_remote_state} =
                   receive_owner_socket_push(next_remote_state)

          assert owner_response_id(next_frame) == "resp_owner_mode_kill_next_turn"
          assert {:ok, _next_remote_state} = receive_owner_socket_complete(next_remote_state)
        after
          CodexResponsesSocket.terminate(:closed, next_remote_state)
        end

        assert [recovered_lite_request, full_request] = await_upstream_requests(upstream, 2)

        assert_canonical_lite_owner_request!(recovered_lite_request)
        assert_canonical_full_owner_request!(full_request)

        assert [lite_request, full_request] = request_logs(setup.pool.id)
        assert lite_request.retry_count == 0
        assert_owner_mode_accounting!(lite_request, "lite", "succeeded", remote_node)
        assert_owner_mode_accounting!(full_request, "full", "succeeded", remote_node)

        assert active_owner_lease(state.codex_session.id).lease_token ==
                 replacement_lease.lease_token

        assert :ok = FakeUpstream.verify!(upstream)
      after
        if Process.alive?(interrupted_turn.pid) do
          Task.shutdown(interrupted_turn, :brutal_kill)
        end
      end
    after
      CodexResponsesSocket.terminate(:closed, remote_state)
    end
  end

  @tag :owner_crash_recovery
  @tag :replay_cleanup
  @tag :replay_topology
  test "remote owner process death after visible output remains terminal" do
    release_ref = make_ref()
    upstream_boundary = visible_blocking_owner_upstream_boundary(self(), release_ref)
    upstream = start_upstream(FakeUpstream.json_response(%{"unexpected" => true}))
    setup = gateway_setup(upstream)
    {:ok, auth} = Access.authenticate_authorization_header(setup.authorization)

    {:ok, state} =
      owner_socket(auth, "ws-owner-visible-kill", "owner-visible-kill", websocket_owner_forwarder_opts: [upstream: upstream_boundary])

    remote_node = :"codex_pooler@visible-killed-owner.example"

    node_opts =
      [upstream: upstream_boundary] ++
        WebsocketOwnerNodeHarness.node_client_opts([remote_node],
          calls: %{remote_node => :success}
        )

    remote_state = remote_owner_state(state, remote_node, node_opts)
    old_lease = active_owner_lease(state.codex_session.id)
    {:ok, owner_pid} = WebsocketOwnerSession.lookup(state.codex_session.id)
    owner_ref = Process.monitor(owner_pid)
    parent = self()

    try do
      visible_turn =
        Task.async(fn ->
          WebsocketOwnerNodeHarness.with_node_client(
            [remote_node],
            [calls: %{remote_node => :success}, notify: parent],
            fn harness_opts ->
              Gateway.run_websocket_response(
                auth,
                websocket_payload(setup, "visible owner crash"),
                owner_response_options(
                  remote_state,
                  [upstream: upstream_boundary] ++ harness_opts
                ),
                fn _data -> :ok end
              )
            end
          )
        end)

      assert_receive {:visible_blocking_owner_upstream, worker_pid, ^release_ref}, @detection_timeout_ms

      try do
        assert {:push, {:text, visible_frame}, _remote_state} =
                 receive_owner_socket_push(remote_state)

        assert owner_response_id(visible_frame) == "resp_owner_visible_before_crash"

        Process.exit(owner_pid, :kill)
        assert_receive {:DOWN, ^owner_ref, :process, ^owner_pid, :killed}, @detection_timeout_ms
        send(worker_pid, {:visible_blocking_owner_release, release_ref})

        assert {:error, %{code: "owner_crashed", status: 502}} =
                 Task.await(visible_turn, 3_000)

        assert {:error, :owner_unavailable} =
                 WebsocketOwnerSession.lookup(state.codex_session.id)

        assert active_owner_lease(state.codex_session.id).lease_token == old_lease.lease_token
        assert Repo.get!(BridgeOwnerLease, old_lease.id).status == "active"
        assert FakeUpstream.count(upstream) == 0
      after
        send(worker_pid, {:visible_blocking_owner_release, release_ref})

        if Process.alive?(visible_turn.pid) do
          Task.shutdown(visible_turn, :brutal_kill)
        end
      end
    after
      CodexResponsesSocket.terminate(:closed, remote_state)
    end
  end

  test "malformed remote owner reply settles once as owner_crashed" do
    upstream = start_upstream(FakeUpstream.json_response(%{"unexpected" => true}))
    setup = gateway_setup(upstream)
    {:ok, auth} = Access.authenticate_authorization_header(setup.authorization)

    {:ok, state} = owner_socket(auth, "ws-owner-malformed-reply", "owner-malformed-reply")
    remote_node = :"codex_pooler@malformed-reply-owner.example"
    private_owner_body = "private owner reply body"

    malformed_reply =
      {:ok,
       %{
         body: private_owner_body,
         terminal: "response.failed",
         status: 502,
         headers: %{}
       }}

    node_opts =
      WebsocketOwnerNodeHarness.node_client_opts([remote_node],
        calls: %{remote_node => {:return, malformed_reply}},
        capture_request_to: self()
      )

    remote_state = remote_owner_state(state, remote_node, node_opts)

    alias_ids_before =
      Repo.all(
        from(alias_record in BridgeSessionAlias,
          where: alias_record.codex_session_id == ^remote_state.codex_session.id,
          select: alias_record.id,
          order_by: [asc: alias_record.id]
        )
      )

    logs =
      capture_stream_outcome_telemetry(fn ->
        logs =
          capture_log(fn ->
            try do
              assert {:error, %{code: "owner_crashed", status: 502}} =
                       Gateway.run_websocket_response(
                         auth,
                         websocket_payload(setup, "malformed owner reply"),
                         owner_response_options(remote_state, node_opts),
                         fn _data -> :ok end
                       )
            after
              # The forced malformed reply also reaches the remote detach call, so
              # terminate doubles as the detach-containment regression; that
              # detach runs in the session cleanup, awaited inside the capture.
              assert :ok = WebsocketCleanupFence.terminate_and_await!(:closed, remote_state)
            end
          end)

        # A malformed owner reply is settled as `owner_crashed`, and a crashed
        # owner is an interruption like a drained or lost one (findings#228).
        assert_receive {:stream_outcome,
                        %{
                          outcome: "interrupted",
                          downstream_transport: "websocket",
                          upstream_transport: "websocket"
                        }}

        refute_received {:stream_outcome, _metadata}
        logs
      end)

    refute logs =~ private_owner_body
    refute logs =~ "websocket response task failed"

    assert [request] = request_logs(setup.pool.id)
    assert request.status == "failed"
    assert request.transport == "websocket"
    assert request.response_status_code == 502
    assert request.last_error_code == "owner_crashed"

    # The submit-boundary line must carry the same upgrade request id the rest
    # of the websocket log family uses, so the two can be joined.
    assert logs =~ "canonical_error=owner_crashed request_id=ws-owner-malformed-reply"

    assert [attempt] = Repo.all(from(a in Attempt, where: a.request_id == ^request.id))
    assert attempt.status == "failed"

    assert [turn] =
             Repo.all(from(t in CodexTurn, where: t.codex_session_id == ^remote_state.codex_session.id))

    assert turn.status == "interrupted"

    assert Repo.all(
             from(alias_record in BridgeSessionAlias,
               where: alias_record.codex_session_id == ^remote_state.codex_session.id,
               select: alias_record.id,
               order_by: [asc: alias_record.id]
             )
           ) == alias_ids_before

    assert FakeUpstream.count(upstream) == 0

    assert_remote_submit_request_v8!(remote_state, remote_node)
  end

  test "local owner crash interrupts active turn without waiting for lease expiry" do
    # The owner is killed with `:kill` while it may hold a database query. On
    # the shared sandbox connection that kill takes the test's own connection
    # down with it (seen on a loaded CI runner as `DBConnection.OwnershipError`
    # at terminate), so this test gives every process its own connection, like
    # the peer-owner crash tests above.
    assert :ok = Sandbox.mode(Repo, :auto)
    on_exit(fn -> assert :ok = Sandbox.mode(Repo, :manual) end)

    upstream = start_upstream(FakeUpstream.json_response(%{"id" => "resp_owner_crash"}))
    setup = gateway_setup(upstream)
    # Auto mode commits; the rows this test owns are purged so table-wide
    # assertions elsewhere in the file keep an empty baseline.
    pool_id = setup.pool.id
    identity_id = setup.identity.id
    pricing_id = setup.pricing.id
    on_exit(fn -> purge_committed_pool_rows!(pool_id, identity_id, pricing_id) end)
    {:ok, auth} = Access.authenticate_authorization_header(setup.authorization)

    {:ok, state} =
      CodexResponsesSocket.init(%{
        auth: auth,
        opts: %{
          request_id: "ws-owner-crash",
          accepted_turn_state: "stable-ws-owner-crash",
          client_ip: "127.0.0.1"
        }
      })

    %{request: request, attempt: attempt, turn: turn, state: state} =
      active_socket_turn_fixture(setup, upstream, state)

    {:ok, owner_pid} = WebsocketOwnerSession.lookup(state.codex_session.id)
    owner_ref = Process.monitor(owner_pid)

    release_task = suspend_cleanup_task!(state)
    Process.exit(owner_pid, :kill)
    assert_receive {:DOWN, ^owner_ref, :process, ^owner_pid, :killed}

    owner_monitor = state.websocket_owner_monitor
    assert_receive {:DOWN, ^owner_monitor, :process, ^owner_pid, :killed} = owner_down

    {handle_result, logs} =
      with_log(fn -> CodexResponsesSocket.handle_info(owner_down, state) end)

    assert {:stop, :normal, {1011, "websocket owner crashed"}, stopped_state} =
             handle_result

    refute Map.has_key?(stopped_state, :websocket_owner_monitor)
    refute Map.has_key?(stopped_state, :websocket_owner_pid)
    refute logs =~ "owner_unavailable_takeover"
    refute logs =~ "pinned_continuation_reauth_required"
    refute logs =~ "owner_drained"
    refute logs =~ "client_disconnected"
    assert_no_leak!("local owner crash monitor logs", logs)

    assert_owner_interruption_state!(%{
      request: request,
      attempt: attempt,
      turn: turn,
      session: state.codex_session,
      error_code: "owner_crashed"
    })

    assert released_owner_lease(
             state.codex_session.id,
             state.codex_session.owner_lease_token
           ).metadata["release_reason"] == "owner_crashed"

    release_task.()
    stop_parked_response_tasks!(stopped_state)

    CodexResponsesSocket.terminate(
      :closed,
      Map.delete(stopped_state, :websocket_owner_downstream)
    )
  end

  test "unexpected owner monitor exit still crashes active turn" do
    assert_abnormal_owner_monitor_down_crashes_active_turn!(
      {:unexpected_owner_exit, :boom},
      "unexpected-exit"
    )
  end

  test "owner monitor normal exit drains active turn without closing websocket" do
    assert_graceful_owner_monitor_down_drains_active_turn!(:normal, "normal")
  end

  test "owner monitor shutdown exit drains active turn and finalizes request attempt turn" do
    assert_graceful_owner_monitor_down_drains_active_turn!(:shutdown, "shutdown")
  end

  test "owner monitor rolling restart exit drains active turn and releases lease" do
    assert_graceful_owner_monitor_down_drains_active_turn!(
      {:shutdown, :rolling_restart},
      "rolling-restart"
    )
  end

  # The idle native socket closes 1001 once its owner is gone (findings#276):
  # every response its client could anchor on went with the owner's upstream
  # connection, and the client's next request goes out whole on a new socket.
  test "idle owner monitor shutdown exit drains lease and closes the idle native socket without warning or finalization" do
    upstream =
      start_upstream(FakeUpstream.json_response(%{"id" => "resp_idle_owner_shutdown"}))

    setup = gateway_setup(upstream)
    {:ok, auth} = Access.authenticate_authorization_header(setup.authorization)

    {:ok, state} =
      CodexResponsesSocket.init(%{
        auth: auth,
        opts: %{
          request_id: "ws-owner-monitor-idle-shutdown",
          accepted_turn_state: "stable-ws-owner-monitor-idle-shutdown",
          client_ip: "127.0.0.1"
        }
      })

    {owner_pid, owner_monitor, owner_down} = owner_monitor_down(:shutdown)

    monitored_state = %{
      state
      | websocket_owner_pid: owner_pid,
        websocket_owner_monitor: owner_monitor
    }

    {handle_result, warning_logs} =
      with_log([level: :warning], fn ->
        CodexResponsesSocket.handle_info(owner_down, monitored_state)
      end)

    assert {:stop, :normal, {1001, "websocket owner is draining"}, stopped_state} = handle_result
    refute Map.has_key?(stopped_state, :websocket_owner_monitor)
    refute Map.has_key?(stopped_state, :websocket_owner_pid)
    refute Map.has_key?(stopped_state, :owner_exit_close_pending)
    assert warning_logs == ""
    assert_no_leak!("idle owner shutdown monitor logs", warning_logs)

    assert released_owner_lease(
             state.codex_session.id,
             state.codex_session.owner_lease_token
           ).metadata["release_reason"] == "owner_drained"

    assert Repo.aggregate(
             from(r in Request, where: r.pool_id == ^setup.pool.id),
             :count
           ) == 0

    assert Repo.aggregate(
             from(a in Attempt,
               join: r in Request,
               on: a.request_id == r.id,
               where: r.pool_id == ^setup.pool.id
             ),
             :count
           ) == 0

    assert Repo.aggregate(
             from(t in CodexTurn, where: t.codex_session_id == ^state.codex_session.id),
             :count
           ) == 0

    CodexResponsesSocket.terminate(
      :closed,
      Map.delete(stopped_state, :websocket_owner_downstream)
    )
  end

  test "intentional stale owner replacement does not close monitored socket as crashed" do
    upstream = start_upstream(FakeUpstream.json_response(%{"id" => "resp_owner_stale_down"}))
    setup = gateway_setup(upstream)
    {:ok, auth} = Access.authenticate_authorization_header(setup.authorization)

    {:ok, state} =
      CodexResponsesSocket.init(%{
        auth: auth,
        opts: %{
          request_id: "ws-owner-stale-down",
          accepted_turn_state: "stable-ws-owner-stale-down",
          client_ip: "127.0.0.1"
        }
      })

    %{request: request, attempt: attempt, turn: turn, state: state} =
      active_socket_turn_fixture(setup, upstream, state)

    release_task = suspend_cleanup_task!(state)
    {:ok, owner_pid} = WebsocketOwnerSession.lookup(state.codex_session.id)
    owner_ref = Process.monitor(owner_pid)
    owner_monitor = state.websocket_owner_monitor

    :ok = GenServer.stop(owner_pid, {:shutdown, :stale_owner})
    assert_receive {:DOWN, ^owner_ref, :process, ^owner_pid, {:shutdown, :stale_owner}}

    assert_receive {:DOWN, ^owner_monitor, :process, ^owner_pid, {:shutdown, :stale_owner}} =
                     owner_down

    {handle_result, logs} =
      with_log(fn -> CodexResponsesSocket.handle_info(owner_down, state) end)

    assert {:ok, kept_state} = handle_result
    refute Map.has_key?(kept_state, :websocket_owner_monitor)
    refute Map.has_key?(kept_state, :websocket_owner_pid)
    refute logs =~ "owner_crashed"
    refute logs =~ "owner_drained"
    refute logs =~ "pinned_continuation_reauth_required"
    assert_no_leak!("stale owner monitor logs", logs)

    assert Repo.get!(Request, request.id).status == "in_progress"
    assert Repo.get!(Attempt, attempt.id).status == "in_progress"
    assert Repo.get!(CodexTurn, turn.id).status == "in_progress"

    refute released_owner_lease_optional(
             state.codex_session.id,
             state.codex_session.owner_lease_token
           )

    assert active_owner_lease(state.codex_session.id).lease_token ==
             state.codex_session.owner_lease_token

    assert kept_state.codex_session.owner_lease_token == state.codex_session.owner_lease_token

    release_task.()

    CodexResponsesSocket.terminate(
      :closed,
      Map.delete(kept_state, :websocket_owner_downstream)
    )
  end

  # Removes every row an auto-mode test committed for its Pool, children first.
  defp purge_committed_pool_rows!(pool_id, identity_id, pricing_id) do
    Sandbox.unboxed_run(Repo, fn ->
      request_ids = Repo.all(from(r in Request, where: r.pool_id == ^pool_id, select: r.id))
      session_ids = Repo.all(from(s in CodexSession, where: s.pool_id == ^pool_id, select: s.id))

      Repo.delete_all(from(l in BridgeOwnerLease, where: l.codex_session_id in ^session_ids))
      Repo.delete_all(from(t in CodexTurn, where: t.codex_session_id in ^session_ids))
      Repo.delete_all(from(e in RequestReplayEntitlement, where: e.request_id in ^request_ids))

      Repo.delete_all(
        from(l in RequestClientRetryLink,
          where: l.predecessor_request_id in ^request_ids or l.successor_request_id in ^request_ids
        )
      )

      Repo.delete_all(from(l in LedgerEntry, where: l.request_id in ^request_ids))
      Repo.delete_all(from(a in Attempt, where: a.request_id in ^request_ids))
      Repo.delete_all(from(f in RequestLogFact, where: f.request_id in ^request_ids))
      Repo.delete_all(from(r in Request, where: r.pool_id == ^pool_id))
      Repo.delete_all(from(s in CodexSession, where: s.pool_id == ^pool_id))
      # Read before the keys go: the fixture owner is only recorded as their creator.
      owner_ids = CodexPooler.PoolerFixtures.api_key_creator_ids([pool_id])
      Repo.delete_all(from(k in APIKey, where: k.pool_id == ^pool_id))

      CodexPooler.PoolerFixtures.delete_committed_pools!([pool_id], owner_ids)

      Repo.delete_all(
        from(s in CodexPooler.Upstreams.Schemas.EncryptedSecret,
          where: s.upstream_identity_id == ^identity_id
        )
      )

      Repo.delete_all(from(i in UpstreamIdentity, where: i.id == ^identity_id))
      Repo.delete_all(from(p in CodexPooler.Catalog.PricingSnapshot, where: p.id == ^pricing_id))
    end)

    :ok
  end

  defp assert_abnormal_owner_monitor_down_crashes_active_turn!(owner_reason, suffix) do
    upstream =
      start_upstream(FakeUpstream.json_response(%{"id" => "resp_owner_#{suffix}"}))

    setup = gateway_setup(upstream)
    {:ok, auth} = Access.authenticate_authorization_header(setup.authorization)

    {:ok, state} =
      CodexResponsesSocket.init(%{
        auth: auth,
        opts: %{
          request_id: "ws-owner-monitor-#{suffix}",
          accepted_turn_state: "stable-ws-owner-monitor-#{suffix}",
          client_ip: "127.0.0.1"
        }
      })

    %{request: request, attempt: attempt, turn: turn, state: state} =
      active_socket_turn_fixture(setup, upstream, state)

    {owner_pid, owner_monitor, owner_down} = owner_monitor_down(owner_reason)

    monitored_state = %{
      state
      | websocket_owner_pid: owner_pid,
        websocket_owner_monitor: owner_monitor
    }

    {handle_result, logs} =
      with_log(fn -> CodexResponsesSocket.handle_info(owner_down, monitored_state) end)

    assert {:stop, :normal, {1011, "websocket owner crashed"}, stopped_state} =
             handle_result

    refute Map.has_key?(stopped_state, :websocket_owner_monitor)
    refute Map.has_key?(stopped_state, :websocket_owner_pid)
    refute logs =~ "owner_unavailable_takeover"
    refute logs =~ "owner_drained"
    refute logs =~ "client_disconnected"
    assert_no_leak!("owner #{suffix} abnormal monitor logs", logs)

    assert_owner_interruption_state!(%{
      request: request,
      attempt: attempt,
      turn: turn,
      session: state.codex_session,
      error_code: "owner_crashed"
    })

    assert released_owner_lease(
             state.codex_session.id,
             state.codex_session.owner_lease_token
           ).metadata["release_reason"] == "owner_crashed"

    stop_parked_response_tasks!(stopped_state)

    CodexResponsesSocket.terminate(
      :closed,
      Map.delete(stopped_state, :websocket_owner_downstream)
    )
  end

  defp assert_graceful_owner_monitor_down_drains_active_turn!(owner_reason, suffix) do
    upstream =
      start_upstream(FakeUpstream.json_response(%{"id" => "resp_owner_#{suffix}"}))

    setup = gateway_setup(upstream)
    {:ok, auth} = Access.authenticate_authorization_header(setup.authorization)

    {:ok, state} =
      CodexResponsesSocket.init(%{
        auth: auth,
        opts: %{
          request_id: "ws-owner-monitor-#{suffix}",
          accepted_turn_state: "stable-ws-owner-monitor-#{suffix}",
          client_ip: "127.0.0.1"
        }
      })

    %{request: request, attempt: attempt, turn: turn, state: state} =
      active_socket_turn_fixture(setup, upstream, state)

    {owner_pid, owner_monitor, owner_down} = owner_monitor_down(owner_reason)

    monitored_state = %{
      state
      | websocket_owner_pid: owner_pid,
        websocket_owner_monitor: owner_monitor
    }

    {handle_result, logs} =
      with_log(fn -> CodexResponsesSocket.handle_info(owner_down, monitored_state) end)

    assert {:ok, kept_state} = handle_result
    refute Map.has_key?(kept_state, :websocket_owner_monitor)
    refute Map.has_key?(kept_state, :websocket_owner_pid)
    refute logs =~ "owner_crashed"
    refute logs =~ "owner_unavailable_takeover"
    refute logs =~ "client_disconnected"
    assert_no_leak!("owner #{suffix} monitor logs", logs)

    assert_owner_interruption_state!(%{
      request: request,
      attempt: attempt,
      turn: turn,
      session: state.codex_session,
      error_code: "owner_drained"
    })

    assert released_owner_lease(
             state.codex_session.id,
             state.codex_session.owner_lease_token
           ).metadata["release_reason"] == "owner_drained"

    stop_parked_response_tasks!(kept_state)

    CodexResponsesSocket.terminate(
      :closed,
      Map.delete(kept_state, :websocket_owner_downstream)
    )
  end

  defp owner_monitor_down(owner_reason) do
    owner_pid =
      spawn(fn ->
        receive do
          {:finish_owner, :normal} -> :ok
          {:finish_owner, reason} -> exit(reason)
        end
      end)

    owner_monitor = Process.monitor(owner_pid)
    send(owner_pid, {:finish_owner, owner_reason})
    assert_receive {:DOWN, ^owner_monitor, :process, ^owner_pid, ^owner_reason} = owner_down
    {owner_pid, owner_monitor, owner_down}
  end

  test "missing cleanup witness for an accepted owner task remains observable and preserves work" do
    upstream = start_upstream(FakeUpstream.json_response(%{"unexpected" => true}))
    setup = gateway_setup(upstream)
    {:ok, auth} = Access.authenticate_authorization_header(setup.authorization)
    {:ok, state} = owner_socket(auth, "owner-missing-witness", "owner-missing-witness")
    %{state: state, request: request} = active_socket_turn_fixture(setup, upstream, state)
    release_task = suspend_cleanup_task!(state)

    incomplete =
      state
      |> Map.delete(:websocket_owner_cleanup_witness)
      |> Map.delete(:websocket_owner_cleanup_task)

    {_result, logs} =
      with_log(fn ->
        Adapter.handle_monitor_down(incomplete, state.websocket_owner_pid, :shutdown)
      end)

    assert logs =~ "failure_reason=stale_owner_cleanup"
    assert Repo.reload!(request).status == "in_progress"

    assert active_owner_lease(state.codex_session.id).lease_token ==
             state.websocket_owner_lease_token

    assert Process.alive?(state.websocket_owner_pid)
    release_task.()
    CodexResponsesSocket.terminate(:closed, state)
  end

  defp visible_blocking_owner_upstream_boundary(test_pid, release_ref) do
    %{
      start: fn -> Agent.start_link(fn -> :ready end) end,
      send: fn _upstream_pid, request, writer ->
        frame =
          CodexPooler.JSON.encode!(%{
            "id" => "resp_owner_visible_before_crash",
            "object" => "response"
          })

        decoded = CodexPooler.JSON.decode!(frame)

        cond do
          is_function(request.frame_observer, 2) -> request.frame_observer.(frame, decoded)
          is_function(request.frame_observer, 1) -> request.frame_observer.(frame)
          true -> :ok
        end

        writer.(frame, TerminalDiscriminator.classify(frame))
        send(test_pid, {:visible_blocking_owner_upstream, self(), release_ref})

        receive do
          {:visible_blocking_owner_release, ^release_ref} -> :ok
        after
          5_000 -> exit(:visible_blocking_owner_timeout)
        end
      end,
      close: fn upstream_pid -> Agent.stop(upstream_pid) end
    }
  end

  # The owner's upstream connection process exits while a turn is running
  # (findings#273). The owner settles that turn (its output-commit probe,
  # then its `upstream_stream_error` when the client saw output, `:complete`
  # and the task's reply) and retires with `:owner_crashed`, and the socket
  # closes 1011. The response task used to crash in
  # `Finalization.Websocket.finalize_failed/2` on the owner's reply, which
  # carries no response headers, and a public socket cancelled the task that
  # was settling the owner's result when the owner went away.
  #
  # One node, local owner, owner forwarding on, native and public `/v1`
  # websockets, the Pool's default serving mode; FakeUpstream holds the
  # request at a frame barrier before any frame, or after `response.created`
  # and a text delta the client received, and the test kills the owner's
  # upstream session process.
  describe "the owner's upstream connection process exits mid-turn" do
    for route <- ["/backend-api/codex/responses", "/v1/responses"], hold <- [:before_any_event, :after_visible_output] do
      @tag route: route, hold: hold
      test "#{hold} on #{route}: the task settles the turn once, the owner retires, one terminal", ctx do
        release_ref = make_ref()

        upstream =
          start_upstream(
            # provenance: synthetic_adversarial (the owner's upstream session process dies while the provider holds the turn)
            FakeUpstream.barrier_websocket_frames(upstream_exit_frames(), notify: self(), release_ref: release_ref)
          )

        setup = gateway_setup(upstream)
        assert :ok = CodexPooler.Events.subscribe_pool(setup.pool)
        {_server, port} = start_public_endpoint_with_server!()
        before = WebsocketCleanupFence.listener_sockets()
        turn_state = if ctx.route == "/v1/responses", do: "", else: "ws-owner-upstream-exit-#{ctx.hold}"
        {conn, websocket, ref} = public_websocket_connect!(port, setup, turn_state, ctx.route)
        socket = WebsocketCleanupFence.await_new_listener_socket!(before)

        {result, log} =
          with_info_log(fn ->
            {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, upstream_exit_payload(setup))
            assert_receive {:fake_upstream_frame_barrier, 0, _handler, ^release_ref}, @detection_timeout_ms
            {conn, websocket, shown} = show_upstream_exit_output!(ctx.hold, upstream, release_ref, conn, websocket, ref)

            socket_state = socket_connection_state!(socket)
            owner = socket_state.websocket_owner_pid
            [task] = MapSet.to_list(socket_state.tasks)
            owner_ref = Process.monitor(owner)
            task_ref = Process.monitor(task)
            :ok = slow_owner_interruption!(owner)
            Process.exit(:sys.get_state(owner).upstream_pid, :kill)

            assert_receive {:DOWN, ^owner_ref, :process, ^owner, owner_reason}, @detection_timeout_ms
            assert_receive {:DOWN, ^task_ref, :process, ^task, task_reason}, @detection_timeout_ms
            assert_receive {CodexPooler.Events, %{reason: "request_finalized"}}, @detection_timeout_ms
            {conn, _websocket, frames} = receive_frames_until_close!(conn, websocket, ref)
            Mint.HTTP.close(conn)
            :ok = WebsocketCleanupFence.await_listener_socket_cleanup!(socket)
            %{shown: shown, frames: frames, owner_reason: owner_reason, task_reason: task_reason, session_id: socket_state.codex_session.id}
          end)

        # The task settled the owner's result and exited normally; the owner
        # retired after settling the turn.
        assert result.task_reason == :normal
        assert result.owner_reason == :owner_crashed

        # One terminal: the owner's error after visible output, the 1011 close
        # before any (an error the socket authors may precede it too).
        texts = for {:text, text} <- result.frames, do: CodexPooler.JSON.decode!(text)["type"]
        assert List.last(result.frames) == {:close, 1011, "websocket owner crashed"}

        case ctx.hold do
          :before_any_event -> assert texts in [[], ["error"]]
          :after_visible_output -> assert result.shown ++ texts == ["response.created", "response.output_text.delta", "error"]
        end

        # The turn is closed once, as an owner crash, by the owner before it
        # answers the task (findings#270 row 270-167): the same record on every
        # run, the shape a released client's resend is admitted against, where
        # the owner's exit and the task used to race for it (499 with the turn
        # interrupted, or 502 with the same interrupted turn).
        assert [request] = Repo.all(from(r in Request, where: r.pool_id == ^setup.pool.id))
        assert {request.status, request.last_error_code, request.response_status_code} == {"failed", "owner_crashed", 499}
        assert [attempt] = Repo.all(from(a in Attempt, where: a.request_id == ^request.id))
        assert {attempt.status, attempt.network_error_code, attempt.upstream_status_code} == {"failed", "owner_crashed", 499}
        assert attempt.error_message == "websocket owner stopped unexpectedly before the turn completed"
        assert %CodexTurn{status: "interrupted", error_code: "owner_crashed"} = Repo.get_by!(CodexTurn, request_id: request.id)
        assert %CodexSession{status: "interrupted"} = Repo.get!(CodexSession, result.session_id)
        assert ledger_entry_kinds(request) == ["release", "reservation", "settlement"]
        assert %BridgeOwnerLease{status: "released"} = Repo.get_by!(BridgeOwnerLease, codex_session_id: result.session_id)

        # No crash and no aborted settlement: the only error line is the
        # owner's own exit report.
        refute log =~ "websocket response task failed"
        refute log =~ "Postgrex.Protocol"
        assert Enum.all?(error_lines(log), &(&1 =~ "WebsocketOwnerSession.Registry" and &1 =~ "terminating"))
      end
    end
  end

  # The resend requires the actual executor's monitored terminal proof.
  # Publish only that retained registry entry through the normal proof API:
  # a background publisher here could consume unrelated executions left by
  # prior sandbox tests and commit them during a peer fixture's mode switch.
  # Publisher scheduling has its own tests; this public Socket boundary owns
  # proof-backed admission on one node with the provider held before output.
  describe "the released client's resend after the owner's upstream connection dies before any output" do
    @tag :scoped_owner_terminal_proof
    test "is admitted once the actual executor's retained terminal proof is published" do
      release_ref = make_ref()
      created = CodexPooler.JSON.encode!(%{"type" => "response.created", "response" => %{"id" => "resp_owner_death_resend", "status" => "in_progress"}})

      upstream =
        start_upstream(
          # provenance: synthetic_adversarial (the owner's upstream session process dies while the provider holds the turn, then the resend is served)
          FakeUpstream.repeat_last([
            FakeUpstream.barrier_websocket_frames([created], notify: self(), release_ref: release_ref),
            completed_response_frames("resp_owner_death_resend_served", [], 3, 2)
          ])
        )

      setup = gateway_setup(upstream)
      {_server, port} = start_public_endpoint_with_server!()
      frame = released_client_frame(setup, "019a0000-0000-7000-8000-000000000283")
      turn = frame.(native_text_input("owner death resend"), Ecto.UUID.generate(), %{})

      before = WebsocketCleanupFence.listener_sockets()
      {conn, websocket, ref} = public_websocket_connect!(port, setup, "ws-owner-death-resend")
      socket = WebsocketCleanupFence.await_new_listener_socket!(before)
      {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, turn)
      assert_receive {:fake_upstream_frame_barrier, 0, _handler, ^release_ref}, @detection_timeout_ms
      owner = socket_connection_state!(socket).websocket_owner_pid
      owner_ref = Process.monitor(owner)
      Process.exit(:sys.get_state(owner).upstream_pid, :kill)
      assert_receive {:DOWN, ^owner_ref, :process, ^owner, :owner_crashed}, @detection_timeout_ms
      {conn, _websocket, frames} = receive_frames_until_close!(conn, websocket, ref)
      assert List.last(frames) == {:close, 1011, "websocket owner crashed"}
      Mint.HTTP.close(conn)
      :ok = WebsocketCleanupFence.await_listener_socket_cleanup!(socket)

      assert [predecessor] = Repo.all(from(r in Request, where: r.pool_id == ^setup.pool.id))
      assert {predecessor.status, predecessor.last_error_code, predecessor.response_status_code} == {"failed", "owner_crashed", 499}
      attempt = Repo.get_by!(Attempt, request_id: predecessor.id)
      :ok = await_execution_proof!(attempt, System.monotonic_time(:millisecond) + 2_000)
      assert %ExecutionTerminalProof{end_kind: "process_down"} = Repo.get!(ExecutionTerminalProof, attempt.owner_execution_id)

      # The same frame on a new socket, as the released client resends it.
      before = WebsocketCleanupFence.listener_sockets()
      {conn, websocket, ref} = public_websocket_connect!(port, setup, "ws-owner-death-resend")
      socket = WebsocketCleanupFence.await_new_listener_socket!(before)
      {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, turn)
      {conn, _websocket, terminal} = receive_native_terminal!(conn, websocket, ref)
      Mint.HTTP.close(conn)
      :ok = WebsocketCleanupFence.await_listener_socket_cleanup!(socket)

      assert %{"type" => "response.completed", "response" => %{"id" => "resp_owner_death_resend_served"}} = terminal
      assert %RequestClientRetryLink{successor_request_id: successor_id} = Repo.get_by!(RequestClientRetryLink, predecessor_request_id: predecessor.id)
      assert %Request{status: "succeeded"} = Repo.get!(Request, successor_id)
      assert %CodexTurn{status: "interrupted", error_code: "owner_crashed"} = Repo.get_by!(CodexTurn, request_id: predecessor.id)
      assert FakeUpstream.count(upstream) == 2
    end
  end

  for {mode, control} <- [{"full", :valid}, {"lite", :valid}, {"full", :remote_502}] ++ Enum.map([:missing_proof, :wrong_boot, :wrong_generation, :wrong_epoch, :wrong_witness, :visible_output, :hard_anchor], &{"full", &1}) do
    @tag :scoped_owner_terminal_proof
    @tag :proven_continuation_owner_crash
    if control == :remote_502, do: @tag(:remote_proven_owner_crash)
    @tag slow: "public websocket owner crash, executor proof publication and reconnect"
    test "a proven owner crash on a codex-request continuation recovers after visible history in #{mode} with #{control}" do
      backlog = if unquote(control) == :remote_502, do: unrelated_pending_executions!(), else: []
      release_ref = make_ref()
      reasoning = %{"type" => "reasoning", "id" => "rs_prior_step", "encrypted_content" => "synthetic-encrypted", "summary" => []}
      call = %{"type" => "function_call", "id" => "fc_prior_step", "call_id" => "call_prior_step", "name" => "sample_tool", "arguments" => "{}"}
      created = CodexPooler.JSON.encode!(%{"type" => "response.created", "response" => %{"id" => "resp_continuation_held", "status" => "in_progress"}})
      held_frames = if unquote(control) == :visible_output, do: [created, CodexPooler.JSON.encode!(%{"type" => "response.output_item.done", "item" => reasoning})], else: [created]

      upstream =
        start_upstream(
          FakeUpstream.strict_sequence([
            completed_response_frames("resp_prior_step", [reasoning, call], 3, 2),
            FakeUpstream.barrier_websocket_frames(held_frames, notify: self(), release_ref: release_ref),
            completed_response_frames("resp_continuation_recovered", [], 3, 2)
          ])
        )

      if unquote(control) == :remote_502, do: enter_peer_owner_topology!()
      setup = gateway_setup(upstream)
      set_model_serving_mode!(model_serving_scope(), setup, unquote(mode))
      peer = if unquote(control) == :remote_502, do: start_peer_session_owner!(setup, %{accepted_turn_state: "proven-continuation-owner-crash"})
      {_server, port} = start_public_endpoint_with_server!()
      frame = released_client_frame(setup, Ecto.UUID.generate())
      turn_id = Ecto.UUID.generate()
      opening_input = native_text_input("synthetic tool request")
      opening = frame.(opening_input, turn_id, %{})
      continuation = frame.(opening_input ++ [reasoning, call, %{"type" => "function_call_output", "call_id" => "call_prior_step", "output" => "synthetic result"}], turn_id, %{})
      before = WebsocketCleanupFence.listener_sockets()
      {conn, websocket, ref} = public_websocket_connect!(port, setup, "proven-continuation-owner-crash")
      socket = WebsocketCleanupFence.await_new_listener_socket!(before)
      {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, opening)
      {conn, websocket, terminal} = receive_native_terminal!(conn, websocket, ref)
      assert terminal["type"] == "response.completed"
      [_opening_request] = await_succeeded_pool_requests!(setup.pool.id, 1)
      {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, continuation)
      assert_receive {:fake_upstream_frame_barrier, 0, _handler, ^release_ref}, @detection_timeout_ms

      {conn, websocket} =
        if unquote(control) == :visible_output do
          :ok = FakeUpstream.release_remaining_frames(upstream, release_ref)
          assert_receive {:fake_upstream_frame_barrier, 2, _handler, ^release_ref}, @detection_timeout_ms
          await_proven_visible_output!(conn, websocket, ref)
        else
          {conn, websocket}
        end

      {conn, websocket} =
        if unquote(control) == :remote_502 do
          :ok = FakeUpstream.release_frame(upstream, release_ref)
          assert_receive {:fake_upstream_frame_barrier, 1, _handler, ^release_ref}, @detection_timeout_ms
          {conn, websocket, created_frame} = public_websocket_receive_text!(conn, websocket, ref)
          assert %{"type" => "response.created"} = CodexPooler.JSON.decode!(created_frame)
          {conn, websocket}
        else
          {conn, websocket}
        end

      owner = socket_connection_state!(socket).websocket_owner_pid
      if peer, do: assert(node(owner) == peer.node and node(owner) != node())
      owner_ref = Process.monitor(owner)
      upstream_pid = if peer, do: :sys.get_state(owner).upstream_pid
      upstream_monitor = if peer, do: Process.monitor(upstream_pid)

      receipt_hold =
        if unquote(control) == :remote_502 do
          execution_ids = Repo.all(from a in Attempt, join: r in Request, on: r.id == a.request_id, where: r.pool_id == ^setup.pool.id, select: a.owner_execution_id)
          on_exit(fn -> Repo.delete_all(from p in ExecutionTerminalProof, where: p.execution_id in ^execution_ids) end)
          :ok = :sys.suspend(socket)

          on_exit(fn ->
            try do
              :sys.resume(socket)
            catch
              :exit, _gone -> :ok
            end
          end)

          Process.exit(owner, :kill)
          assert_receive {:DOWN, ^owner_ref, :process, ^owner, :killed}, @detection_timeout_ms
          await_proven_owner_502!(setup.pool.id)
          predecessor = Repo.one!(from r in Request, where: r.pool_id == ^setup.pool.id and r.last_error_code == "owner_crashed")
          attempt = Repo.get_by!(Attempt, request_id: predecessor.id)
          hold = hold_owner_receipt!(attempt, socket)
          :ok = :sys.resume(socket)
          hold
        else
          Process.exit(:sys.get_state(owner).upstream_pid, :kill)
          assert_receive {:DOWN, ^owner_ref, :process, ^owner, :owner_crashed}, @detection_timeout_ms
          nil
        end

      {conn, _websocket, frames} = receive_frames_until_close!(conn, websocket, ref)
      assert List.last(frames) == {:close, 1011, "websocket owner crashed"}
      Mint.HTTP.close(conn)
      :ok = WebsocketCleanupFence.await_listener_socket_cleanup!(socket)

      [prior, predecessor] = Repo.all(from r in Request, where: r.pool_id == ^setup.pool.id, order_by: [asc: r.admitted_at])
      assert prior.status == "succeeded"
      assert Repo.get_by!(CodexTurn, request_id: prior.id).first_visible_output_at != nil
      assert String.starts_with?(predecessor.correlation_id, "codex-request:")
      expected_status = if unquote(control) == :remote_502, do: 502, else: 499
      assert {predecessor.status, predecessor.last_error_code, predecessor.response_status_code} == {"failed", "owner_crashed", expected_status}
      attempt = Repo.get_by!(Attempt, request_id: predecessor.id)

      if peer do
        assert_receive {:DOWN, ^upstream_monitor, :process, ^upstream_pid, _reason}, @detection_timeout_ms
        prove_held_owner_receipt!(receipt_hold, attempt, socket)
      end

      :ok = await_execution_proof!(attempt, System.monotonic_time(:millisecond) + 2_000)
      assert ExecutionTerminalProofs.terminal?(attempt)
      assert attempt.replay_generation == 0
      refute ClientRetry.verified_owner_crash?(Repo.get_by!(CodexTurn, request_id: prior.id), prior, Repo.get_by!(Attempt, request_id: prior.id))
      assert Repo.get_by!(CodexTurn, request_id: predecessor.id).first_visible_output_at != nil == (unquote(control) == :visible_output)
      proof = Repo.get!(ExecutionTerminalProof, attempt.owner_execution_id)

      case unquote(control) do
        :missing_proof -> Repo.delete!(proof)
        :wrong_boot -> attempt |> Ecto.Changeset.change(owner_instance_boot_id: "different-owned-test-boot") |> Repo.update!()
        :wrong_generation -> attempt |> Ecto.Changeset.change(replay_generation: 1) |> Repo.update!()
        :wrong_epoch -> predecessor |> Ecto.Changeset.change(native_client_retry_auth_epoch: predecessor.native_client_retry_auth_epoch + 1) |> Repo.update!()
        :wrong_witness -> predecessor |> Ecto.Changeset.change(native_client_retry_digest: :crypto.hash(:sha256, "different-owned-test-witness")) |> Repo.update!()
        _other -> :ok
      end

      attempt = Repo.reload!(attempt)
      predecessor = Repo.reload!(predecessor)

      retry_frame = if unquote(control) == :hard_anchor, do: continuation |> CodexPooler.JSON.decode!() |> Map.put("previous_response_id", "resp_prior_step") |> CodexPooler.JSON.encode!(), else: continuation
      telemetry_ref = make_ref()
      telemetry_id = {__MODULE__, :proven_continuation_refusal, telemetry_ref}
      test_pid = self()
      :ok = :telemetry.attach(telemetry_id, [:codex_pooler, :gateway, :duplicate_turn, :refused], fn _event, _measurements, metadata, _config -> send(test_pid, {telemetry_ref, metadata}) end, nil)
      on_exit(fn -> :telemetry.detach(telemetry_id) end)
      before = WebsocketCleanupFence.listener_sockets()
      {conn, websocket, ref} = public_websocket_connect!(port, setup, "proven-continuation-owner-crash")
      socket = WebsocketCleanupFence.await_new_listener_socket!(before)
      {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, retry_frame)
      {conn, _websocket, terminal} = receive_native_terminal!(conn, websocket, ref)
      Mint.HTTP.close(conn)
      :ok = WebsocketCleanupFence.await_listener_socket_cleanup!(socket)

      stages =
        cond do
          unquote(control) == :hard_anchor ->
            nil

          get_in(terminal, ["error", "code"]) == "duplicate_turn" ->
            assert_receive {^telemetry_ref, %{stage: stage, transport: "websocket"}}, @detection_timeout_ms
            [stage]

          true ->
            []
        end

      rows = Repo.all(from r in Request, where: r.pool_id == ^setup.pool.id)
      entries = Repo.all(from l in LedgerEntry, where: l.request_id in ^Enum.map(rows, & &1.id))
      IO.puts("PROVEN-CONTINUATION-METADATA " <> CodexPooler.JSON.encode!(%{control: Atom.to_string(unquote(control)), serving_mode: predecessor.request_metadata["routing"]["model_serving_mode"], claim_class: String.split(predecessor.correlation_id, ":") |> hd(), prior_visible: Repo.get_by!(CodexTurn, request_id: prior.id).first_visible_output_at != nil, same_semantic_turn: Repo.get_by!(CodexTurn, request_id: prior.id).semantic_turn_digest == Repo.get_by!(CodexTurn, request_id: predecessor.id).semantic_turn_digest, latest_generation: attempt.replay_generation, first_visible: Repo.get_by!(CodexTurn, request_id: predecessor.id).first_visible_output_at != nil, proof_kind: proof.end_kind, exact_terminal_proof: ExecutionTerminalProofs.terminal?(attempt), proof_interruption: proof.interruption_code, request_status: predecessor.status, request_error: predecessor.last_error_code, response_status: predecessor.response_status_code, usage_status: predecessor.usage_status, unchanged_retry_frame: retry_frame == continuation, refusal_stages: stages, retry_wire_code: get_in(terminal, ["error", "code"]), upstream_dispatch_count: FakeUpstream.count(upstream), request_count: length(rows), ledger_counts: Enum.frequencies_by(entries, & &1.entry_kind)}))

      if unquote(control) == :remote_502 do
        assert length(ExecutionRegistry.pending_proofs(backlog)) == length(backlog), "scoped publication consumed an unrelated ended execution"
        refute Repo.exists?(from p in ExecutionTerminalProof, where: p.execution_id in ^backlog), "scoped publication persisted an unrelated ended execution"
        CodexPooler.TestDiagnostics.puts("owner_proof_backlog unrelated_pending=3 unrelated_published=0 unrelated_acknowledged=0 sandbox_transition_preserved=true")
      end

      if unquote(control) in [:valid, :remote_502] do
        assert %{"type" => "response.completed", "response" => %{"id" => "resp_continuation_recovered"}} = terminal
        assert %RequestClientRetryLink{successor_request_id: successor_id} = Repo.get_by!(RequestClientRetryLink, predecessor_request_id: predecessor.id)
        assert Repo.get!(Request, successor_id).status == "succeeded"
        assert FakeUpstream.count(upstream) == 3
        assert length(rows) == 3
        assert Enum.frequencies_by(entries, & &1.entry_kind) == %{"reservation" => 3, "settlement" => 3, "release" => 3}
      else
        if unquote(control) == :hard_anchor do
          assert %{"type" => "error", "error" => %{"code" => code}} = terminal
          assert code in ["duplicate_turn", "previous_response_not_found"]
        else
          assert %{"type" => "error", "status" => 409, "error" => %{"code" => "duplicate_turn"}} = terminal
        end

        refute Repo.exists?(from l in RequestClientRetryLink, where: l.predecessor_request_id == ^predecessor.id)
        assert FakeUpstream.count(upstream) == 2

        if unquote(control) == :hard_anchor do
          assert length(rows) == 3
          denied = Enum.find(rows, &(&1.id not in [prior.id, predecessor.id]))
          assert denied.status == "failed"
          assert denied.last_error_code == "stream_incomplete"
          denied_settlement = Enum.find(entries, &(&1.request_id == denied.id and &1.entry_kind == "settlement"))
          IO.puts("ANCHOR-DENIAL-METADATA " <> CodexPooler.JSON.encode!(%{status: denied.status, error: denied.last_error_code, usage_status: denied.usage_status, total_tokens: denied_settlement.total_tokens, amount_status: denied_settlement.amount_status, settled_cost_zero: if(is_nil(denied_settlement.settled_cost_micros), do: nil, else: Decimal.equal?(denied_settlement.settled_cost_micros, 0)), no_upstream_dispatch: FakeUpstream.count(upstream) == 2}))
          assert Enum.frequencies_by(entries, & &1.entry_kind) == %{"reservation" => 3, "settlement" => 3, "release" => 3}
        else
          assert length(rows) == 2
          assert Enum.frequencies_by(entries, & &1.entry_kind) == %{"reservation" => 2, "settlement" => 2, "release" => 2}
        end
      end
    end
  end

  defp await_proven_owner_502!(pool_id) do
    await_proven_owner_502!(pool_id, System.monotonic_time(:millisecond) + @detection_timeout_ms)
  end

  defp await_proven_owner_502!(pool_id, deadline) do
    case Repo.one(from r in Request, where: r.pool_id == ^pool_id and r.last_error_code == "owner_crashed") do
      %Request{status: "failed", response_status_code: response_status} ->
        IO.puts("REMOTE-OWNER-TERMINAL-METADATA " <> CodexPooler.JSON.encode!(%{response_status: response_status, error: "owner_crashed"}))
        assert response_status == 502
        :ok

      _pending ->
        assert System.monotonic_time(:millisecond) < deadline

        receive do
        after
          5 -> await_proven_owner_502!(pool_id, deadline)
        end
    end
  end

  defp await_proven_visible_output!(conn, websocket, ref) do
    {conn, websocket, frame} = public_websocket_receive_text!(conn, websocket, ref)

    case CodexPooler.JSON.decode!(frame) do
      %{"type" => "response.output_item.done", "item" => %{"type" => "reasoning"}} -> {conn, websocket}
      _progress -> await_proven_visible_output!(conn, websocket, ref)
    end
  end

  defp unrelated_pending_executions! do
    for _index <- 1..3 do
      execution_id = Ecto.UUID.generate()

      CodexPooler.UnboxedFixture.register_unboxed_cleanup!(fn ->
        Repo.delete_all(from p in ExecutionTerminalProof, where: p.execution_id == ^execution_id)
        :ok = ExecutionRegistry.acknowledge([execution_id])
      end)

      parent = self()

      actor =
        start_supervised!(
          {Task,
           fn ->
             :ok = ExecutionRegistry.register(execution_id)
             send(parent, {:unrelated_execution_registered, execution_id, self()})

             receive do
               :finish_unrelated_execution -> :ok
             end
           end},
          id: {:unrelated_execution, execution_id}
        )

      monitor = Process.monitor(actor)
      assert_receive {:unrelated_execution_registered, ^execution_id, ^actor}, @detection_timeout_ms
      send(actor, :finish_unrelated_execution)
      assert_receive {:DOWN, ^monitor, :process, ^actor, :normal}, @detection_timeout_ms
      assert :dead = ExecutionRegistry.status(execution_id, actor)
      assert [%{owner_execution_id: ^execution_id, end_kind: "process_down"}] = ExecutionRegistry.pending_proofs([execution_id])
      execution_id
    end
  end

  defp hold_owner_receipt!(attempt, socket) do
    assert attempt.owner_instance_id == Atom.to_string(node())
    executor = attempt.owner_process_id |> String.to_charlist() |> :erlang.list_to_pid()
    assert ExecutionRegistry.status(attempt.owner_execution_id, executor) == :alive
    hold = make_ref()
    test_pid = self()
    claim = :atomics.new(1, [])
    handler = {__MODULE__, :receipt_commit_hold, hold}

    on_exit(fn ->
      :telemetry.detach(handler)
      send(executor, {hold, :release})
    end)

    :ok =
      :telemetry.attach(
        handler,
        [:codex_pooler, :repo, :query],
        fn _event, _measurements, metadata, _config ->
          if self() == executor and metadata.query == "begin" and :atomics.compare_exchange(claim, 1, 0, 1) == :ok do
            {token, entry} = Enum.find(:sys.get_state(ActivityRegistry).delivery_receipts, fn {_token, entry} -> entry.identity.attempt_id == attempt.id end)
            send(test_pid, {hold, :receipt_transaction, self(), token, Map.take(entry, [:task, :actor, :socket, :identity, :finished?]), elem(metadata.result, 1).connection_id})

            receive do
              {^hold, :release} -> :ok
            after
              @detection_timeout_ms -> raise "owner receipt transaction was not released"
            end
          end

          if self() == executor and metadata.query == "commit" and :atomics.get(claim, 1) == 1 do
            send(test_pid, {hold, :receipt_commit, self()})
          end
        end,
        nil
      )

    %{ref: hold, executor: executor, monitor: Process.monitor(executor), socket: socket}
  end

  defp prove_held_owner_receipt!(%{ref: hold, executor: executor, monitor: monitor}, attempt, socket) do
    assert_receive {^hold, :receipt_transaction, ^executor, token, entry, backend}, @detection_timeout_ms
    assert is_reference(token)
    assert %{rows: [["idle in transaction", true]]} = Repo.query!("SELECT state, xact_start IS NOT NULL FROM pg_stat_activity WHERE pid = $1", [backend])
    assert entry == %{task: executor, actor: executor, socket: socket, identity: %{request_id: attempt.request_id, attempt_id: attempt.id, replay_generation: attempt.replay_generation}, finished?: false}
    assert ExecutionIdentity.status(attempt) == :alive
    assert ExecutionRegistry.pending_proofs([attempt.owner_execution_id]) == []
    refute ExecutionTerminalProofs.terminal?(attempt)
    refute Map.has_key?(Repo.reload!(attempt).response_metadata, "downstream_delivery")
    CodexPooler.TestDiagnostics.puts("owner_receipt_boundary phase=held real_transaction=true exact_actor=true exact_task=true exact_generation=true socket_close=1011 upstream_down=true execution=alive proof=false receipt_committed=false")
    # The same deadline bounds an executor that remains alive. A socket close
    # alone must neither publish its proof nor authorize another attempt.
    started_at = System.monotonic_time(:millisecond)
    assert_raise ExUnit.AssertionError, ~r/terminal proof was not available before the deadline/, fn -> await_execution_proof!(attempt, started_at + 2_000) end
    assert System.monotonic_time(:millisecond) - started_at >= 2_000
    assert ExecutionIdentity.status(attempt) == :alive
    assert ExecutionRegistry.pending_proofs([attempt.owner_execution_id]) == []
    refute ExecutionTerminalProofs.terminal?(attempt)
    assert Repo.aggregate(from(a in Attempt, where: a.request_id == ^attempt.request_id), :count) == 1
    CodexPooler.TestDiagnostics.puts("owner_receipt_boundary phase=deadline alive=true proof=false attempt_count=1 original_budget_ms=2000")

    # Observe two real registry calls by this waiter. The second proves it
    # consumed :alive and kept waiting before the receipt is allowed to commit.
    registry = Process.whereis(ExecutionRegistry)
    assert {:flags, []} = :erlang.trace_info(registry, :flags)
    on_exit(fn -> :erlang.trace(registry, false, [:receive]) end)
    assert 1 = :erlang.trace(registry, true, [:receive])
    waiter = Task.async(fn -> await_execution_proof!(attempt, System.monotonic_time(:millisecond) + 2_000) end)
    execution_id = attempt.owner_execution_id
    waiter_pid = waiter.pid

    try do
      for _observation <- 1..2 do
        assert_receive {:trace, ^registry, :receive, {:"$gen_call", {^waiter_pid, _reply}, {:status, ^execution_id, ^executor}}}, @detection_timeout_ms
      end

      send(executor, {hold, :release})
      assert :ok = Task.await(waiter, @detection_timeout_ms)
    after
      :erlang.trace(registry, false, [:receive])
      send(executor, {hold, :release})
    end

    assert_receive {^hold, :receipt_commit, ^executor}, @detection_timeout_ms
    assert_receive {:DOWN, ^monitor, :process, ^executor, :normal}, @detection_timeout_ms
    assert ExecutionIdentity.status(attempt) == :dead
    assert %{"outcome" => "aborted"} = Repo.reload!(attempt).response_metadata["downstream_delivery"]
    CodexPooler.TestDiagnostics.puts("owner_receipt_boundary phase=released real_commit=true original_executor_down=true execution=dead receipt_committed=true")
  end

  defp await_execution_proof!(attempt, deadline) do
    registry_node = Enum.find([node() | Node.list(:connected)], &(Atom.to_string(&1) == attempt.owner_instance_id))
    assert registry_node, "the actual executor's registry node is unavailable"
    registry = {ExecutionRegistry, registry_node}

    case ExecutionIdentity.status(attempt) do
      :alive -> await_execution_proof_retry!(attempt, deadline)
      :dead -> publish_execution_proof!(attempt, deadline, registry_node, registry)
      :unknown -> flunk("the actual executor's execution identity is unknown")
    end
  end

  defp publish_execution_proof!(attempt, deadline, registry_node, registry) do
    case ExecutionRegistry.pending_proofs([attempt.owner_execution_id], registry) do
      [proof] ->
        fields = [:owner_execution_id, :owner_instance_id, :owner_instance_boot_id, :owner_process_id]
        assert Map.take(proof, fields) == Map.take(attempt, fields)
        assert {:ok, 1} = ExecutionTerminalProofs.publish([proof])
        assert ExecutionTerminalProofs.terminal?(attempt)
        assert :ok = ExecutionRegistry.acknowledge([attempt.owner_execution_id], registry)
        CodexPooler.TestDiagnostics.puts("owner_proof_scope registry_node=#{if registry_node == node(), do: :local, else: :peer} actual_executor_down=true exact_proof_persisted=true acknowledged_execution_count=1")
        :ok

      [] ->
        if ExecutionTerminalProofs.terminal?(attempt) do
          :ok
        else
          await_execution_proof_retry!(attempt, deadline)
        end

      :unknown ->
        flunk("the actual executor's registry is unavailable")
    end
  end

  defp await_execution_proof_retry!(attempt, deadline) do
    remaining = deadline - System.monotonic_time(:millisecond)
    assert remaining > 0, "the actual executor's retained terminal proof was not available before the deadline"

    receive do
    after
      min(5, remaining) -> await_execution_proof!(attempt, deadline)
    end
  end

  # findings#270 row 270-170: the owner's upstream connection process dies
  # during a native compaction (owner forwarding on, native, Pool forced Full,
  # one node, the real public listener, FakeUpstream). While the owner collects
  # the compaction, before any of its frames or after its output item, the
  # owner settles the compaction as an owner crash and retires. While the
  # compaction's task confirms the collected result, the owner retires under
  # that confirmation, which answers the task as an owner already gone instead
  # of crashing it. Either way the socket closes 1011 with nothing before the
  # Close, each request settles once, and the owner's own exit report is the
  # only error line. The sandbox runs in auto mode: the killed process may be
  # inside a query, which must not take another process's connection down.
  describe "the owner's upstream connection process exits during a native compaction" do
    for moment <- [:collecting_before_any_frame, :collecting_after_its_item, :confirming_the_result] do
      @tag moment: moment
      test "#{moment}: the socket closes 1011, each request settles once, no task crash", ctx do
        enter_peer_owner_topology!()
        release_ref = make_ref()
        compact_item = %{"type" => "compaction", "encrypted_content" => "synthetic-owner-death-#{ctx.moment}"}
        compaction = Enum.map(compaction_events(compact_item), &CodexPooler.JSON.encode!/1)

        compaction_respond =
          if ctx.moment == :confirming_the_result,
            do: FakeUpstream.websocket_text_frames(compaction),
            else: FakeUpstream.barrier_websocket_frames(compaction, notify: self(), release_ref: release_ref)

        upstream =
          start_upstream(
            # provenance: synthetic_adversarial (compaction v2-shaped mid-turn frames; the owner's upstream session process dies while the owner collects or confirms the compaction)
            FakeUpstream.strict_sequence([
              compaction_turn_request([forbidden: ["previous_response_id"]], FakeUpstream.websocket_text_frames([CodexPooler.JSON.encode!(compaction_anchor_event())])),
              compaction_turn_request([equals: %{"previous_response_id" => "resp_owner_death_anchor"}], compaction_respond)
            ])
          )

        setup = gateway_setup(upstream, compact?: true)
        register_unboxed_pool_cleanup!(setup)
        Repo.insert!(%ModelServingOverride{pool_id: setup.pool.id, exposed_model_id: setup.model.exposed_model_id, mode: "full"})
        assert :ok = CodexPooler.Events.subscribe_pool(setup.pool)
        {_server, port} = start_public_endpoint_with_server!()
        before = WebsocketCleanupFence.listener_sockets()
        turn_id = "owner-death-compaction-#{ctx.moment}"
        {conn, websocket, ref} = public_websocket_connect!(port, setup, turn_id)
        socket = WebsocketCleanupFence.await_new_listener_socket!(before)

        {result, log} =
          with_info_log(fn ->
            anchor_input = [%{"type" => "message", "role" => "user", "content" => "synthetic compaction anchor"}]
            {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, compaction_turn_frame(setup, turn_id, "turn", anchor_input, nil))
            {conn, websocket, anchor} = receive_native_terminal!(conn, websocket, ref)
            assert %{"type" => "response.completed", "response" => %{"id" => "resp_owner_death_anchor"}} = anchor
            assert_receive {CodexPooler.Events, %{reason: "request_finalized", payload: %{"status" => "succeeded"}}}, @detection_timeout_ms
            _idle = await_socket_connection_state!(socket, &(MapSet.size(&1.tasks) == 0))
            owner = socket_connection_state!(socket).websocket_owner_pid
            owner_ref = Process.monitor(owner)
            :ok = slow_owner_interruption!(owner)
            hold = if ctx.moment == :confirming_the_result, do: hold_settled_websocket_turn!()
            trigger = [%{"type" => "custom_tool_call_output", "call_id" => "call_#{turn_id}", "output" => "synthetic tool output"}, %{"type" => "compaction_trigger"}]
            {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, compaction_turn_frame(setup, turn_id, "compaction", trigger, "resp_owner_death_anchor"))
            killed = kill_owner_upstream_during!(ctx.moment, owner, upstream, release_ref, hold)

            assert_receive {:DOWN, ^owner_ref, :process, ^owner, owner_reason}, @detection_timeout_ms
            {conn, _websocket, frames} = receive_frames_until_close!(conn, websocket, ref)
            assert_receive {CodexPooler.Events, %{reason: "request_finalized"}}, @detection_timeout_ms
            Mint.HTTP.close(conn)
            :ok = WebsocketCleanupFence.await_listener_socket_cleanup!(socket)
            %{frames: frames, owner_reason: owner_reason, killed: killed}
          end)

        # One terminal: the 1011 close, which the owner crash error the socket
        # authors may precede when it reaches the client first.
        assert result.owner_reason == :owner_crashed
        assert List.last(result.frames) == {:close, 1011, "websocket owner crashed"}
        texts = for {:text, text} <- result.frames, do: CodexPooler.JSON.decode!(text)
        assert texts in [[], [%{"type" => "error", "status" => 502, "error" => %{"code" => "owner_crashed", "message" => "websocket owner stopped unexpectedly", "param" => nil, "type" => "server_error"}}]]
        assert length(result.frames) == length(texts) + 1

        assert [anchor, compaction] = Repo.all(from(r in Request, where: r.pool_id == ^setup.pool.id, order_by: r.admitted_at))
        assert {anchor.endpoint, anchor.status} == {"/backend-api/codex/responses", "succeeded"}
        assert compaction.endpoint == "/backend-api/codex/responses/compact"

        # A compaction collected in full was settled before its confirmation;
        # one still collecting is settled by its owner as an owner crash.
        if ctx.moment == :confirming_the_result do
          assert {compaction.status, compaction.response_status_code} == {"succeeded", 200}
        else
          assert {compaction.status, compaction.last_error_code, compaction.response_status_code} == {"failed", "owner_crashed", 499}
        end

        for request <- [anchor, compaction] do
          assert [_attempt] = Repo.all(from(a in Attempt, where: a.request_id == ^request.id))
          assert ledger_entry_kinds(request) == ["release", "reservation", "settlement"]
        end

        # No crash and no aborted settlement: the only error lines are the
        # owner's own exit report and, when the kill landed inside a query of
        # the killed upstream connection process, that process's connection.
        refute log =~ "MatchError"
        refute log =~ "websocket response task failed"
        killed_client = "client #{inspect(result.killed)} exited"
        assert Enum.all?(error_lines(log), &((&1 =~ "WebsocketOwnerSession.Registry" and &1 =~ "terminating") or (&1 =~ "Postgrex.Protocol" and &1 =~ killed_client)))
        assert FakeUpstream.count(upstream) == 2
      end
    end
  end

  # The owner's interruption of a crashed turn takes 200 ms, as behind a slow
  # database. The task the owner answers settles the same turn from the
  # owner's error, so an interruption that ran only as the owner exited lost
  # the turn to the task on every such run, and to a plain race otherwise
  # (findings#270 row 270-167); settled before the answer, it is the only
  # record whatever the timing.
  defp slow_owner_interruption!(owner) do
    :sys.replace_state(owner, fn state ->
      interrupt = state.persistence.interrupt_codex_session

      put_in(state.persistence.interrupt_codex_session, fn session_id, opts ->
        Process.sleep(200)
        interrupt.(session_id, opts)
      end)
    end)

    :ok
  end

  defp kill_owner_upstream_during!(:collecting_before_any_frame, owner, _upstream, release_ref, _hold) do
    assert_receive {:fake_upstream_frame_barrier, 0, _handler, ^release_ref}, @detection_timeout_ms
    kill_owner_upstream!(owner)
  end

  defp kill_owner_upstream_during!(:collecting_after_its_item, owner, upstream, release_ref, _hold) do
    assert_receive {:fake_upstream_frame_barrier, 0, _handler, ^release_ref}, @detection_timeout_ms
    assert :ok = FakeUpstream.release_frame(upstream, release_ref)
    assert_receive {:fake_upstream_frame_barrier, 1, _handler, ^release_ref}, @detection_timeout_ms
    kill_owner_upstream!(owner)
  end

  # The compaction's task is held between its settlement and its confirmation;
  # the owner is suspended while its upstream connection process is killed and
  # the task is let go, so the owner takes the upstream's exit before the
  # task's confirmation reaches it and retires with that call pending.
  defp kill_owner_upstream_during!(:confirming_the_result, owner, _upstream, _release_ref, hold) do
    assert_receive {^hold, :held, task}, @detection_timeout_ms
    upstream_pid = :sys.get_state(owner).upstream_pid
    :ok = :sys.suspend(owner)
    ^upstream_pid = kill_owner_upstream!(owner, upstream_pid)
    :ok = release_settled_websocket_turn(hold, task)
    await_owner_mailbox!(owner, 2, System.monotonic_time(:millisecond) + @detection_timeout_ms)
    :ok = :sys.resume(owner)
    upstream_pid
  end

  # Returns the killed upstream connection process.
  defp kill_owner_upstream!(owner, upstream_pid \\ nil) do
    upstream_pid = upstream_pid || :sys.get_state(owner).upstream_pid
    upstream_ref = Process.monitor(upstream_pid)
    Process.exit(upstream_pid, :kill)
    assert_receive {:DOWN, ^upstream_ref, :process, ^upstream_pid, :killed}, @detection_timeout_ms
    upstream_pid
  end

  defp await_owner_mailbox!(owner, count, deadline) do
    {:message_queue_len, queued} = Process.info(owner, :message_queue_len)

    cond do
      queued >= count ->
        :ok

      System.monotonic_time(:millisecond) >= deadline ->
        flunk("the owner never got the upstream's exit and the task's confirmation (#{queued} queued)")

      true ->
        receive do
        after
          1 -> await_owner_mailbox!(owner, count, deadline)
        end
    end
  end

  defp compaction_turn_request(json, respond) do
    json = Keyword.update(Keyword.merge([valid: true], json), :equals, %{"type" => "response.create"}, &Map.put(&1, "type", "response.create"))
    FakeUpstream.expect_request(method: "WEBSOCKET", websocket_connection_ordinal: 1, json: Keyword.put_new(json, :equals, %{"type" => "response.create"}), respond: respond)
  end

  defp compaction_anchor_event,
    do: %{"type" => "response.completed", "response" => %{"id" => "resp_owner_death_anchor", "status" => "completed", "output" => [], "usage" => %{"input_tokens" => 1_200, "output_tokens" => 9, "total_tokens" => 1_209}}}

  defp compaction_events(item) do
    [
      %{"type" => "response.output_item.done", "item" => item},
      %{"type" => "response.completed", "response" => %{"id" => "resp_owner_death_compact", "status" => "completed", "output" => [item], "usage" => %{"input_tokens" => 3_000, "output_tokens" => 40, "total_tokens" => 3_040}}}
    ]
  end

  # The released client's mid-turn frames: turn metadata naming the turn, its
  # window and the request kind; a compaction's also names its compaction.
  defp compaction_turn_frame(setup, turn_id, request_kind, input, anchor) do
    metadata =
      %{"turn_id" => turn_id, "window_id" => "owner-death-window-1", "context_window_id" => "00000000-0000-4000-8000-000000000170", "window_number" => 1, "request_kind" => request_kind}
      |> then(&if(request_kind == "compaction", do: Map.put(&1, "compaction", %{"trigger" => "auto", "reason" => "context_limit", "implementation" => "responses_compaction_v2", "phase" => "mid_turn", "strategy" => "memento"}), else: &1))

    %{"type" => "response.create", "model" => setup.model.exposed_model_id, "input" => input, "stream" => true, "generate" => true, "client_metadata" => %{"turn_id" => turn_id, "x-codex-turn-metadata" => CodexPooler.JSON.encode!(metadata)}}
    |> then(&if(anchor, do: Map.put(&1, "previous_response_id", anchor), else: &1))
    |> CodexPooler.JSON.encode!()
  end

  defp upstream_exit_frames do
    [
      %{"type" => "response.created", "response" => %{"id" => "resp_owner_upstream_exit", "status" => "in_progress"}},
      %{"type" => "response.output_text.delta", "delta" => "synthetic visible text"},
      %{"type" => "response.completed", "response" => %{"id" => "resp_owner_upstream_exit", "status" => "completed", "output" => [], "usage" => %{"input_tokens" => 2, "output_tokens" => 1, "total_tokens" => 3}}}
    ]
    |> Enum.map(&CodexPooler.JSON.encode!/1)
  end

  defp upstream_exit_payload(setup),
    do: CodexPooler.JSON.encode!(%{"type" => "response.create", "model" => setup.model.exposed_model_id, "input" => native_text_input("owner upstream exit"), "stream" => true, "generate" => true})

  defp show_upstream_exit_output!(:before_any_event, _upstream, _release_ref, conn, websocket, _ref), do: {conn, websocket, []}

  # Releases response.created and the delta, and returns once the client got
  # the delta, so it was shown output before the owner's upstream dies.
  defp show_upstream_exit_output!(:after_visible_output, upstream, release_ref, conn, websocket, ref) do
    for ordinal <- [1, 2] do
      assert :ok = FakeUpstream.release_frame(upstream, release_ref)
      assert_receive {:fake_upstream_frame_barrier, ^ordinal, _handler, ^release_ref}, @detection_timeout_ms
    end

    receive_shown_output!(conn, websocket, ref, [])
  end

  defp receive_shown_output!(conn, websocket, ref, shown) do
    {conn, websocket, text} = public_websocket_receive_text!(conn, websocket, ref)
    shown = shown ++ [CodexPooler.JSON.decode!(text)["type"]]

    if List.last(shown) == "response.output_text.delta",
      do: {conn, websocket, shown},
      else: receive_shown_output!(conn, websocket, ref, shown)
  end

  defp error_lines(log), do: log |> String.split("\n") |> Enum.filter(&(&1 =~ "[error]"))

  defp released_owner_lease_optional(session_id, lease_token) do
    Repo.one(
      from lease in BridgeOwnerLease,
        where:
          lease.codex_session_id == ^session_id and lease.lease_token == ^lease_token and
            lease.status == "released",
        limit: 1
    )
  end
end
