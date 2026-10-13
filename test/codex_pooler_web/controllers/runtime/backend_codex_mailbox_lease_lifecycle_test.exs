defmodule CodexPoolerWeb.Runtime.BackendCodexMailboxLeaseLifecycleTest do
  use CodexPoolerWeb.ConnCase, async: false

  import Ecto.Query
  import CodexPoolerWeb.Runtime.BackendCodexTestSupport
  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport, only: [model_serving_scope: 0, set_model_serving_mode!: 3, with_info_log: 1]

  alias CodexPooler.Access
  alias CodexPooler.Accounting
  alias CodexPooler.Accounting.{Attempt, LedgerEntry, Request, RequestClientRetryLink}
  alias CodexPooler.Accounting.RequestLifecycle.DeadExecutionRecovery
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway.OperationalSettings
  alias CodexPooler.Gateway.Payloads.RequestOptions
  alias CodexPooler.Gateway.Persistence.{BridgeOwnerLease, CodexSession, CodexTurn, RuntimeCleanup}
  alias CodexPooler.Gateway.Persistence.SessionContinuity
  alias CodexPooler.Gateway.Runtime.Finalization.ExpiredOwnerGenerationCleanup, as: ExpiredCleanup
  alias CodexPooler.Gateway.Transports.Websocket.{OwnerDefaults, WebsocketOwnerSession}
  alias CodexPooler.Gateway.Websocket, as: Gateway
  alias CodexPooler.Platform.ExecutionIdentity
  alias CodexPooler.Platform.ExecutionRegistry
  alias CodexPooler.Platform.ExecutionTerminalProof
  alias CodexPooler.Platform.ExecutionTerminalProofs
  alias CodexPooler.Platform.InstancePresence
  alias CodexPooler.Platform.InstancePresence.Identity, as: PresenceIdentity
  alias CodexPooler.Platform.InstancePresence.Instance, as: PresenceInstance
  alias CodexPooler.Repo
  alias CodexPooler.UnboxedFixture
  alias CodexPoolerWeb.Runtime.{BackendCodexWebsocketOwnerForwardingSupport, MailboxLeaseLifecycleSupport, MailboxPrefixRaceSupport, WebsocketCleanupFence}
  alias Ecto.Adapters.SQL.Sandbox

  @moduletag capture_log: true
  @budget 15_000
  @path "/backend-api/codex/responses"
  # The lease every scenario opens with. It outlives any scheduling delay in the setup (the owner's start, the upgrade, the first
  # turn), the way the HTTP owner lease tests keep their pre-dispatch window on a stable ttl; the ttl a test is about starts at its
  # boundary (`observe_boundary!/1`). With the claim's ttl from the start, one stall of the renewing owner (or of its database
  # round trip) longer than the ttl made the next renewal find the lease expired and retire the owner (findings#303 row 303-11).
  @stable_ttl 30
  # What the claims observe at their boundary: a healthy owner renews across its own initial deadline (three renewals fit in it);
  # every other claim only needs a lease that really expires while its owner cannot renew.
  @healthy_claim_ttl 3
  @expiry_claim_ttl 1
  # The expired-generation cleanup works under the product's owner call budget (5 s; its own deadline is one second less, and it
  # needs one more second of it to prove the provider connection closed). A stall of about 3 s inside a held cleanup (the session
  # row it waits for, the owner's mailbox behind the closing socket's own cleanup) exhausts that budget before the cleanup reaches
  # the step a test is about, and the next step then meets a different state (findings#303 row 303-11). Only tests set this
  # budget, through `OwnerDefaults`: the tests whose claim is not the budget run on the detection budget, the two tagged
  # `owner_call_budget: :product` keep the product's.
  @owner_call_budget_ms @budget

  setup_all do
    %{queue_peer: BackendCodexWebsocketOwnerForwardingSupport.start_shared_bridge_peer!()}
  end

  setup context do
    CodexPooler.DataCase.stop_sandbox(context.sandbox_owner, context.sandbox_settings_cache)
    on_exit(fn -> Sandbox.mode(Repo, :manual) end)
    :ok = Sandbox.mode(Repo, :auto)
    CodexPooler.TestAppEnv.restore_on_exit(:websocket_owner_forwarding_enabled, false)
    CodexPooler.TestAppEnv.restore_on_exit(OperationalSettings)
    settings = %{OperationalSettings.current() | bridge_owner_lease_ttl_seconds: @stable_ttl, bridge_owner_lease_renewal_seconds: 1}
    Application.put_env(:codex_pooler, OperationalSettings, settings: settings)
    owner_defaults = if Map.get(context, :owner_call_budget) == :product, do: [], else: [owner_call_timeout_ms: @owner_call_budget_ms]

    if owner_defaults != [] do
      CodexPooler.TestAppEnv.restore_on_exit(OwnerDefaults)
      Application.put_env(:codex_pooler, OwnerDefaults, owner_defaults)
    end

    %{lease_settings: settings, owner_defaults: owner_defaults}
  end

  for mode <- ["full", "lite"], carrier <- [:http, :owner_local, :owner_remote], boundary <- [:healthy, :disconnect_wins, :cleanup_wins, :settled_then_expiry] do
    @tag mode: mode, carrier: carrier, boundary: boundary
    @tag slow: if(boundary == :healthy, do: "observes the actual three-second PostgreSQL ownership deadline with real periodic renewal", else: "observes the actual one-second PostgreSQL ownership deadline cross")
    if boundary in [:disconnect_wins, :cleanup_wins], do: @tag(mailbox_lease_negative: true)
    if mode == "full" and carrier == :owner_remote and boundary == :disconnect_wins, do: @tag(mailbox_expired_cut_control: true)
    if carrier in [:owner_local, :owner_remote] and boundary == :disconnect_wins, do: @tag(mailbox_expired_ordering: true)
    if mode == "full" and carrier == :owner_local and boundary == :healthy, do: @tag(mailbox_healthy_renewal_control: true)

    test "#{mode} #{carrier} mailbox lease boundary #{boundary}", context do
      with_info_log(fn -> run_scenario(context) end)
    end
  end

  for carrier <- [:owner_local, :owner_remote] do
    @tag mode: "full", carrier: carrier, boundary: :disconnect_wins, mailbox_expired_end_write_failure: true
    @tag slow: "observes actual expiry and PostgreSQL rejection after physical generation close"
    test "#{carrier} keeps failed end write selectable and retries exact retained close proof", context do
      with_info_log(fn ->
        fixture = open_scenario!(context)
        trigger = install_end_write_failure!()
        fixture = observe_boundary!(fixture)
        close_transport!(fixture.transport, fixture.carrier)
        {cleanup, ref} = fixture.cleanup
        send(cleanup.pid, {:release_runtime_cleanup_owner_candidates, ref})
        assert :owner_unavailable = assert_cleanup_failure!(cleanup, fixture)
        assert_exact_fixture_executions_down!(fixture)
        attempt = Repo.get_by!(Attempt, request_id: fixture.original.id)
        assert {:ok, %{"phase" => "authorized"}} = ExpiredCleanup.read(attempt)
        assert Repo.get!(Request, fixture.original.id).status == "in_progress"
        assert attempt.status == "in_progress"
        assert ledger_kinds!(fixture.original.id) == %{"reservation" => 1}
        assert FakeUpstream.count(fixture.upstream) == 1
        drop_end_write_failure!(trigger)
        Application.delete_env(:codex_pooler, :runtime_cleanup_owner_candidate_test_barrier)
        assert {:ok, _summary} = RuntimeCleanup.cleanup_expired_runtime_state(db_now())
        [terminal] = await_settled!(fixture.setup, 1)
        assert_expired_stop_after_executor_down!(fixture, terminal)
        assert terminal.last_error_code == "owner_unavailable"
        assert ledger_kinds!(fixture.original.id) == %{"reservation" => 1, "release" => 1, "settlement" => 1}
        assert FakeUpstream.count(fixture.upstream) == 1
        refute Repo.exists?(from l in RequestClientRetryLink, where: l.predecessor_request_id == ^fixture.original.id)
        CodexPooler.TestDiagnostics.puts(fn -> CodexPooler.JSON.encode!(%{scenario: "actual_end_write_failure_retry", carrier: fixture.carrier, real_postgres_rejection: true, exact_executor_down: true, authorization_not_end: true, active_rows_retained: true, existing_cleanup_retry: true, physical_sends: 1, terminal_status: 499, terminal_reason: "owner_unavailable", settlement_count: 1}) end)
      end)
    end
  end

  for carrier <- [:owner_local, :owner_remote] do
    @tag mode: "full", carrier: carrier, boundary: :disconnect_wins, mailbox_expired_token_race: true
    @tag slow: "real expiry takeover wins before the exact stop second guard"
    test "#{carrier} refuses stop after real expired-token takeover and clears only its unsignalled authorization", context do
      with_info_log(fn ->
        fixture = open_scenario!(context)
        task = active_owner_task!(fixture.actor)
        task_monitor = Process.monitor(task)
        checkpoint = arm_expired_checkpoint!(fixture.actor, :authorized)
        fixture = observe_boundary!(fixture)
        {cleanup, ref} = fixture.cleanup
        send(cleanup.pid, {:release_runtime_cleanup_owner_candidates, ref})
        assert_receive {:expired_owner_generation_checkpoint, owner, ^checkpoint, :authorized, witness}, @budget
        assert owner == fixture.actor
        opts = RequestOptions.for_websocket(%{bridge_owner_lease_ttl_seconds: 30})
        assert {:ok, renewed} = on_actor_node!(owner, SessionContinuity, :renew_owner_token, [fixture.old_session, fixture.old_session.owner_lease_token, opts, [take_over_expired: true]])
        assert renewed.owner_lease_token != fixture.old_session.owner_lease_token
        send(owner, {:release_expired_owner_generation_checkpoint, checkpoint})
        assert {:stale_owner, _clause} = assert_cleanup_failure!(cleanup, fixture)
        assert on_actor_node!(owner, Process, :alive?, [task])
        refute_receive {:DOWN, ^task_monitor, :process, ^task, _reason}, 20
        assert Process.alive?(fixture.handler)
        assert :none = ExpiredCleanup.read(Repo.get!(Attempt, witness["attempt_id"]))
        assert Repo.get!(Request, fixture.original.id).status == "in_progress"
        assert ledger_kinds!(fixture.original.id) == %{"reservation" => 1}
        assert FakeUpstream.count(fixture.upstream) == 1
        release_provider(fixture.handler, fixture.carrier, fixture.gate)
        [terminal] = await_settled!(fixture.setup, 1)
        CodexPooler.TestDiagnostics.puts(fn -> CodexPooler.JSON.encode!(%{scenario: "actual_token_takeover_before_second_guard", carrier: fixture.carrier, signal_issued: false, exact_task_preserved_at_guard: true, cause_removed: true, physical_sends: 1, terminal_status: terminal.response_status_code, terminal_reason: terminal.last_error_code, settlement_count: 1}) end)
        assert terminal.status == "succeeded"
        assert_settled_once!([terminal])
        close_transport!(fixture.transport, fixture.carrier)
      end)
    end
  end

  @tag mode: "full", carrier: :owner_local, boundary: :disconnect_wins, mailbox_expired_deadline: true, lease_ttl: 1, owner_call_budget: :product
  @tag slow: "holds a real PostgreSQL owner lock through the unchanged five-second command budget"
  test "SQL lock deadline exits its child and queued expired command retains real owner state", context do
    with_info_log(fn ->
      fixture = open_scenario!(context)
      expired_packet_deadline = System.monotonic_time(:millisecond) + 4000
      fixture = observe_boundary!(fixture)
      {cleanup, ref} = fixture.cleanup
      # Held only once `observe_boundary!/1` has switched the owner's renewal timer off: a tick that fires while the
      # test holds the row blocks the owner on it, and the suppression's `:sys.get_state` then times out (findings#303
      # row 303-9). The cleanup stays parked at its barrier until the release below, so the row is still held when its
      # second guard asks for it.
      holder = hold_session_row!(fixture.old_session.id)
      started = System.monotonic_time(:millisecond)
      send(cleanup.pid, {:release_runtime_cleanup_owner_candidates, ref})
      waiter = await_session_waiter!(holder.backend, System.monotonic_time(:millisecond) + @budget)
      candidate = expired_candidate!(fixture.old_session.id)
      queued = Task.async(fn -> GenServer.call(fixture.actor, {:recover_expired_generation, candidate, expired_packet_deadline}, 5000) end)
      assert :owner_unavailable = assert_cleanup_failure!(cleanup, fixture)
      assert System.monotonic_time(:millisecond) - started < 5000
      assert {:error, :owner_unavailable} = Task.await(queued, @budget)
      assert {:ok, _status} = WebsocketOwnerSession.owner_status(fixture.actor)
      assert Process.alive?(fixture.actor) and Process.alive?(fixture.handler)
      assert Repo.get!(Request, fixture.original.id).status == "in_progress"
      assert :none = ExpiredCleanup.read(Repo.get_by!(Attempt, request_id: fixture.original.id))
      assert_backend_transaction_released!(waiter)
      release_session_row!(holder)
      Application.delete_env(:codex_pooler, :runtime_cleanup_owner_candidate_test_barrier)
      close_transport!(fixture.transport, fixture.carrier)
      assert {:ok, _summary} = RuntimeCleanup.cleanup_expired_runtime_state(db_now())
      assert_exact_fixture_executions_down!(fixture)
      [terminal] = await_settled!(fixture.setup, 1)
      assert_expired_stop_after_executor_down!(fixture, terminal)
      assert FakeUpstream.count(fixture.upstream) == 1
      CodexPooler.TestDiagnostics.puts(fn -> CodexPooler.JSON.encode!(%{scenario: "actual_sql_and_owner_queue_deadline", actual_distinct_backends: waiter != holder.backend, owner_call_budget_ms: 5000, caller_bounded: true, sql_transaction_released: true, queued_expired_state_preserved: true, physical_sends: 1}) end)
    end)
  end

  for carrier <- [:owner_local, :owner_remote] do
    @tag mode: "full", carrier: carrier, boundary: :disconnect_wins, mailbox_expired_buffered_terminal: true
    @tag slow: "real provider terminal wins while the second-guard Session lock is blocked"
    test "#{carrier} preserves exact successful terminal queued during second guard row wait", context do
      with_info_log(fn ->
        fixture = open_scenario!(context)
        task = active_owner_task!(fixture.actor)
        monitor = Process.monitor(task)
        checkpoint = arm_expired_checkpoint!(fixture.actor, :authorized)
        fixture = observe_boundary!(fixture)
        {cleanup, ref} = fixture.cleanup
        send(cleanup.pid, {:release_runtime_cleanup_owner_candidates, ref})
        assert_receive {:expired_owner_generation_checkpoint, owner, ^checkpoint, :authorized, witness}, @budget
        assert owner == fixture.actor
        holder = hold_session_row!(fixture.old_session.id)
        send(owner, {:release_expired_owner_generation_checkpoint, checkpoint})
        waiter = await_session_waiter!(holder.backend, System.monotonic_time(:millisecond) + @budget)
        release_provider(fixture.handler, fixture.carrier, fixture.gate)
        assert_receive {:DOWN, ^monitor, :process, ^task, :normal}, @budget
        release_session_row!(holder)
        assert {:stale_owner, _clause} = assert_cleanup_failure!(cleanup, fixture)
        {conn, ws, text} = public_websocket_receive_text!(fixture.transport.conn, fixture.transport.ws, fixture.transport.ref)
        assert CodexPooler.JSON.decode!(text)["type"] == "response.completed"
        [terminal] = await_settled!(fixture.setup, 1)
        assert terminal.status == "succeeded" and terminal.response_status_code == 200
        assert terminal.last_error_code == nil
        assert :none = ExpiredCleanup.read(Repo.get!(Attempt, witness["attempt_id"]))
        assert_settled_once!([terminal])
        assert FakeUpstream.count(fixture.upstream) == 1
        assert_backend_transaction_released!(waiter)
        close_transport!(%{fixture.transport | conn: conn, ws: ws}, fixture.carrier)
        CodexPooler.TestDiagnostics.puts(fn -> CodexPooler.JSON.encode!(%{scenario: "actual_buffered_terminal_during_second_lock", carrier: fixture.carrier, exact_task_down: "normal", live_downstream_completed_frame: true, success_preserved: true, authorization_cleared: true, expiry_signal_issued: false, physical_sends: 1, response_status_code: 200}) end)
      end)
    end
  end

  @tag mode: "full", carrier: :owner_remote, boundary: :disconnect_wins, lease_ttl: 1, mailbox_expired_unmarked_gone: true
  @tag slow: "ends the genuine serving VM and publishes a real exclusive-slot successor"
  test "authoritative VM supersession retains old unmarked expired websocket recovery", context do
    with_info_log(fn ->
      fixture = open_scenario!(context)
      slot = "sample-expired-slot-#{Ecto.UUID.generate()}"
      record_fixture_presence!(fixture.actor, slot)
      fixture = observe_boundary!(fixture)
      pause_fixture_executor!(fixture.executor)
      close_transport!(fixture.transport, fixture.carrier)
      assert :ok = on_actor_node!(fixture.actor, :init, :stop, [])
      assert_receive {:DOWN, generation_monitor, :process, handler, _reason}, @budget
      assert generation_monitor == fixture.generation_monitor and handler == fixture.generation_actor
      await_peer_disconnected!(node(fixture.actor), System.monotonic_time(:millisecond) + @budget)
      successor = BackendCodexWebsocketOwnerForwardingSupport.start_bridge_peer!(:current, fixture.setup.identity, repo: :real)
      record_fixture_presence_node!(successor, slot)
      owner = PresenceIdentity.owner(fixture.old_session.owner_instance_id, fixture.old_session.owner_instance_boot_id)
      assert InstancePresence.superseded?(owner)
      {cleanup, ref} = fixture.cleanup
      send(cleanup.pid, {:release_runtime_cleanup_owner_candidates, ref})
      assert {:ok, _summary} = Task.await(cleanup, @budget)
      [terminal] = await_settled!(fixture.setup, 1)
      assert terminal.last_error_code == "owner_unavailable" and terminal.response_status_code == 499
      assert :none = ExpiredCleanup.read(Repo.get_by!(Attempt, request_id: terminal.id))
      resume_fixture_executor!(fixture.executor)
      assert_receive {:DOWN, execution_monitor, :process, executor, _reason}, @budget
      assert execution_monitor == fixture.execution_monitor and executor == fixture.executor
      [after_caller] = await_settled!(fixture.setup, 1)
      assert after_caller.last_error_code == "owner_unavailable"
      assert_settled_once!([after_caller])
      assert FakeUpstream.count(fixture.upstream) == 1
      assert Repo.aggregate(from(a in Attempt, where: a.request_id == ^terminal.id), :count) == 1
      CodexPooler.TestDiagnostics.puts(fn -> CodexPooler.JSON.encode!(%{scenario: "real_unmarked_expired_vm_supersession", serving_vm_ended: true, actual_successor_vm: true, shared_pg_authority: true, physical_sends: 1, one_attempt: true, original_owner_failure_retained_after_caller_down: true}) end)
    end)
  end

  for carrier <- [:owner_local, :owner_remote], stage <- [:authorized, :physical_end] do
    @tag mode: "full", carrier: carrier, boundary: :disconnect_wins, lease_ttl: 1, crash_stage: stage, mailbox_expired_receiver_crash: true
    @tag slow: "real serving actor crash before/after signal and published existing recovery"
    test "#{carrier} actor crash at#{stage} leaves exact cause recoverable without retrying old dispatch", context do
      with_info_log(fn ->
        fixture = open_scenario!(context)
        checkpoint = arm_expired_checkpoint!(fixture.actor, context.crash_stage)
        fixture = observe_boundary!(fixture)
        close_transport!(fixture.transport, fixture.carrier)
        {cleanup, ref} = fixture.cleanup
        send(cleanup.pid, {:release_runtime_cleanup_owner_candidates, ref})
        assert_receive {:expired_owner_generation_checkpoint, owner, ^checkpoint, observed_stage, witness}, @budget
        assert owner == fixture.actor and observed_stage == context.crash_stage
        assert true = on_actor_node!(owner, Process, :exit, [owner, :kill])
        assert :owner_unavailable = assert_cleanup_failure!(cleanup, fixture)
        assert_exact_fixture_executions_down!(fixture)
        assert FakeUpstream.count(fixture.upstream) == 1
        assert {:ok, %{"phase" => "authorized"}} = ExpiredCleanup.read(Repo.get!(Attempt, witness["attempt_id"]))
        assert Repo.get!(Request, fixture.original.id).status == "in_progress"
        attempt = Repo.get!(Attempt, witness["attempt_id"])
        execution_id = attempt.owner_execution_id
        UnboxedFixture.register_unboxed_cleanup!(fn -> Repo.delete_all(from p in ExecutionTerminalProof, where: p.execution_id == ^execution_id) end)
        assert :ok = ExecutionRegistry.retire_ended()
        [proof] = ExecutionRegistry.pending_proofs([execution_id])
        assert {:ok, _published} = ExecutionTerminalProofs.publish([proof])
        assert :ok = ExecutionRegistry.acknowledge([execution_id])
        # This case proves actor-death recovery, not the production sweep's 500ms latency budget.
        assert {:ok, summary} = DeadExecutionRecovery.recover_execution_ids([attempt.owner_execution_id], db_now(), timeout: @budget)
        assert summary.dead_execution_attempts_recovered == 1
        [terminal] = await_settled!(fixture.setup, 1)
        assert_expired_stop_after_executor_down!(fixture, terminal)
        assert_settled_once!([terminal])
        assert Repo.aggregate(from(a in Attempt, where: a.request_id == ^terminal.id), :count) == 1
        assert FakeUpstream.count(fixture.upstream) == 1
        CodexPooler.TestDiagnostics.puts(fn -> CodexPooler.JSON.encode!(%{scenario: "actual_receiver_crash_existing_recovery", carrier: fixture.carrier, crash_stage: context.crash_stage, actual_linked_producer_dead: true, attempt_death_not_substituted: true, existing_published_selector: true, physical_sends: 1, one_attempt: true, terminal_reason: "owner_unavailable", response_status_code: 499}) end)
      end)
    end
  end

  for carrier <- [:owner_local, :owner_remote] do
    @tag mode: "full", carrier: carrier, boundary: :healthy, lease_ttl: 1, mailbox_expired_ambient_guard: true
    @tag slow: "real local/remote live owner and genuine completed frame with arity-only transaction trace"
    test "#{carrier} ambient transaction makes zero owner RPC and retains genuine active work", context do
      with_info_log(fn ->
        fixture = open_scenario!(context)
        MailboxPrefixRaceSupport.suppress_owned_periodic_renewal!(fixture.actor, :owner)
        assert %{genuine_expiry: true, unchanged_deadlines: true} = MailboxLeaseLifecycleSupport.renew_and_observe_expiry!(Repo.get!(CodexSession, fixture.old_session.id))
        candidate = expired_candidate!(fixture.old_session.id)

        calls =
          trace_ambient_owner_calls!(fn ->
            assert {:ok, :protected} =
                     Repo.transaction(fn ->
                       assert {:error, :caller_transaction} = RuntimeCleanup.cleanup_expired_runtime_state(db_now())
                       assert {:error, :caller_transaction} = WebsocketOwnerSession.recover_expired_generation(candidate)
                       :protected
                     end)
          end)

        assert calls == 0
        assert Process.alive?(fixture.handler)
        release_provider(fixture.handler, fixture.carrier, fixture.gate)
        {conn, ws, text} = public_websocket_receive_text!(fixture.transport.conn, fixture.transport.ws, fixture.transport.ref)
        assert CodexPooler.JSON.decode!(text)["type"] == "response.completed"
        [terminal] = await_settled!(fixture.setup, 1)
        assert terminal.status == "succeeded"
        assert_settled_once!([terminal])
        close_transport!(%{fixture.transport | conn: conn, ws: ws}, fixture.carrier)
        CodexPooler.TestDiagnostics.puts(fn -> CodexPooler.JSON.encode!(%{scenario: "actual_ambient_owner_guard", carrier: fixture.carrier, arity_only_trace: true, owner_rpc_calls: calls, genuine_completed_frame: true, physical_sends: FakeUpstream.count(fixture.upstream)}) end)
      end)
    end
  end

  for carrier <- [:owner_local, :owner_remote] do
    @tag mode: "full", carrier: carrier, boundary: :cleanup_wins, lease_ttl: 1, mailbox_expired_public_projection: true
    @tag slow: "actual cleanup winning before physical client cut with public owner failure projection"
    test "#{carrier} public failure omits internal stop disposition while accounting remains499", context do
      with_info_log(fn ->
        fixture = open_scenario!(context) |> observe_boundary!()
        release_cleanup!(fixture.cleanup)
        {conn, ws, text} = public_websocket_receive_text!(fixture.transport.conn, fixture.transport.ws, fixture.transport.ref)
        event = CodexPooler.JSON.decode!(text)
        refute text =~ "expired_owner_stop_disposition"
        error = event["error"] || get_in(event, ["response", "error"])
        assert error["code"] == "owner_unavailable"
        close_transport!(%{fixture.transport | conn: conn, ws: ws}, fixture.carrier)
        assert_exact_fixture_executions_down!(fixture)
        [terminal] = await_settled!(fixture.setup, 1)
        assert terminal.last_error_code == "owner_unavailable" and terminal.response_status_code == 499
        attempt = Repo.get_by!(Attempt, request_id: terminal.id)
        refute CodexPooler.JSON.encode!(terminal.request_metadata) =~ "expired_owner_stop_disposition"
        refute CodexPooler.JSON.encode!(attempt.response_metadata) =~ "expired_owner_stop_disposition"
        assert {:ok, %{"phase" => "ended"}} = ExpiredCleanup.read(attempt)
        assert_settled_once!([terminal])
        assert FakeUpstream.count(fixture.upstream) == 1
        CodexPooler.TestDiagnostics.puts(fn -> CodexPooler.JSON.encode!(%{scenario: "actual_public_stop_projection", carrier: fixture.carrier, event_type: event["type"], public_code: error["code"], internal_disposition_absent_on_wire_and_metadata: true, response_status_code: 499, physical_sends: 1}) end)
      end)
    end
  end

  defp trace_ambient_owner_calls!(operation) do
    parent = self()
    ref = make_ref()
    tracer = spawn(fn -> count_ambient_calls(parent, ref, 0) end)
    patterns = [{WebsocketOwnerSession, :local_recover_expired_generation, 1}, {:erpc, :call, 5}]
    on_exit(fn -> if Process.alive?(tracer), do: Process.exit(tracer, :kill) end)
    Enum.each(patterns, &:erlang.trace_pattern(&1, true, [:local]))
    :erlang.trace(self(), true, [:call, :arity, {:tracer, tracer}])

    try do
      operation.()
    after
      :erlang.trace(self(), false, [:call])
      Enum.each(patterns, &:erlang.trace_pattern(&1, false, [:local]))
    end

    send(tracer, {:finish, ref})
    assert_receive {:ambient_owner_calls, ^ref, count}, @budget
    count
  end

  defp count_ambient_calls(parent, ref, count) do
    receive do
      {:trace, _pid, :call, {_module, _function, _arity}} -> count_ambient_calls(parent, ref, count + 1)
      {:finish, ^ref} -> send(parent, {:ambient_owner_calls, ref, count})
    end
  end

  @tag mode: "full", carrier: :owner_local, boundary: :disconnect_wins, lease_ttl: 1, mailbox_expired_uncertain_origin: true
  @tag slow: "actual SQL phase death leaves uncertain authorization that a later command cannot erase"
  test "later unsignalled command retains prior uncertain authorization origin", context do
    with_info_log(fn ->
      fixture = open_scenario!(context)
      old_task = active_owner_task!(fixture.actor)
      checkpoint = arm_expired_checkpoint!(fixture.actor, :authorized)
      fixture = observe_boundary!(fixture)
      {cleanup, ref} = fixture.cleanup
      send(cleanup.pid, {:release_runtime_cleanup_owner_candidates, ref})
      assert_receive {:expired_owner_generation_checkpoint, owner, ^checkpoint, :authorized, original}, @budget
      holder = hold_session_row!(fixture.old_session.id)
      send(owner, {:release_expired_owner_generation_checkpoint, checkpoint})
      await_session_waiter!(holder.backend, System.monotonic_time(:millisecond) + @budget)
      children = Task.Supervisor.children(CodexPooler.Gateway.Transports.Websocket.WebsocketOwnerSession.TaskSupervisor)
      [phase_child] = Enum.reject(children, &(&1 == old_task))
      assert true = Process.exit(phase_child, :kill)
      assert :owner_unavailable = assert_cleanup_failure!(cleanup, fixture)
      release_session_row!(holder)
      assert {:ok, ^original} = ExpiredCleanup.read(Repo.get!(Attempt, original["attempt_id"]))
      candidate = expired_candidate!(fixture.old_session.id)
      second = Task.async(fn -> WebsocketOwnerSession.recover_expired_generation(candidate) end)
      assert_receive {:expired_owner_generation_checkpoint, ^owner, ^checkpoint, :authorized, reused}, @budget
      assert reused == original
      opts = RequestOptions.for_websocket(%{bridge_owner_lease_ttl_seconds: 30})
      assert {:ok, _renewed} = SessionContinuity.renew_owner_token(fixture.old_session, fixture.old_session.owner_lease_token, opts, take_over_expired: true)
      send(owner, {:release_expired_owner_generation_checkpoint, checkpoint})
      assert {:error, {:stale_owner, :decision_origin}} = Task.await(second, @budget)
      assert {:ok, ^original} = ExpiredCleanup.read(Repo.get!(Attempt, original["attempt_id"]))
      assert Process.alive?(old_task) and Process.alive?(fixture.handler)
      release_provider(fixture.handler, fixture.carrier, fixture.gate)
      {conn, ws, text} = public_websocket_receive_text!(fixture.transport.conn, fixture.transport.ws, fixture.transport.ref)
      assert CodexPooler.JSON.decode!(text)["type"] == "response.completed"
      [terminal] = await_settled!(fixture.setup, 1)
      assert terminal.status == "succeeded"
      assert_settled_once!([terminal])
      close_transport!(%{fixture.transport | conn: conn, ws: ws}, fixture.carrier)
      CodexPooler.TestDiagnostics.puts(fn -> CodexPooler.JSON.encode!(%{scenario: "actual_prior_uncertain_origin_retained", actual_sql_child_crash: true, old_decision_preserved: true, later_false_signal_not_authority: true, genuine_completed_frame: true, physical_sends: FakeUpstream.count(fixture.upstream)}) end)
    end)
  end

  @tag mode: "full", carrier: :owner_remote, boundary: :disconnect_wins, lease_ttl: 1, mailbox_expired_db_only: true
  @tag slow: "actual producer VM termination and PostgreSQL-only successor recovery of exact marked cause"
  test "producer VM supersession lets a DB-only peer corroborate and finalize marked authorization", context do
    with_info_log(fn ->
      fixture = open_scenario!(context)
      slot = "sample-marked-slot-#{Ecto.UUID.generate()}"
      record_fixture_presence!(fixture.actor, slot)
      checkpoint = arm_expired_checkpoint!(fixture.actor, :authorized)
      fixture = observe_boundary!(fixture)
      pause_fixture_executor!(fixture.executor)
      close_transport!(fixture.transport, fixture.carrier)
      {cleanup, ref} = fixture.cleanup
      send(cleanup.pid, {:release_runtime_cleanup_owner_candidates, ref})
      assert_receive {:expired_owner_generation_checkpoint, owner, ^checkpoint, :authorized, witness}, @budget
      assert :ok = on_actor_node!(owner, :init, :stop, [])
      assert :owner_unavailable = assert_cleanup_failure!(cleanup, fixture)
      assert_receive {:DOWN, generation_monitor, :process, handler, _reason}, @budget
      assert generation_monitor == fixture.generation_monitor and handler == fixture.generation_actor
      await_peer_disconnected!(node(owner), System.monotonic_time(:millisecond) + @budget)
      worker = BackendCodexWebsocketOwnerForwardingSupport.start_bridge_peer!(:current, fixture.setup.identity, repo: :real)
      record_fixture_presence_node!(worker, slot)
      assert node(owner) not in :erpc.call(worker, Node, :list, [])
      producer = Map.new(witness["producer"], fn {key, value} -> {String.to_existing_atom(key), value} end)
      assert :unknown = :erpc.call(worker, ExecutionIdentity, :status, [producer])
      assert {:ok, _cleanup_summary} = :erpc.call(worker, RuntimeCleanup, :cleanup_expired_runtime_state, [db_now()])
      observed = Repo.get!(Attempt, witness["attempt_id"])
      assert {:ok, %{"phase" => "ended", "end_kind" => "producer_vm_superseded"} = ended} = ExpiredCleanup.read(observed)
      assert ended["request_id"] == fixture.original.id
      resume_fixture_executor!(fixture.executor)
      assert_receive {:DOWN, execution_monitor, :process, executor, _reason}, @budget
      assert execution_monitor == fixture.execution_monitor and executor == fixture.executor
      [terminal] = await_settled!(fixture.setup, 1)
      assert_expired_stop_after_executor_down!(fixture, terminal)
      assert_settled_once!([terminal])
      assert FakeUpstream.count(fixture.upstream) == 1
      CodexPooler.TestDiagnostics.puts(fn -> CodexPooler.JSON.encode!(%{scenario: "real_db_only_producer_supersession", producer_node_disconnected: true, producer_registry_status: "unknown", real_successor_vm: true, pg_supersession: true, durable_end_kind: "producer_vm_superseded", physical_sends: 1, response_status_code: 499}) end)
    end)
  end

  @tag mode: "full", carrier: :owner_remote, boundary: :disconnect_wins, lease_ttl: 1, mailbox_expired_unknown_controls: true
  @tag slow: "actual serving Registry loss stays unknown under missing stale or different-slot presence"
  test "unknown registered producer cannot gain end from missing stale foreign-slot or null-boot evidence", context do
    with_info_log(fn ->
      fixture = open_scenario!(context)
      checkpoint = arm_expired_checkpoint!(fixture.actor, :authorized)
      fixture = observe_boundary!(fixture)
      {cleanup, ref} = fixture.cleanup
      send(cleanup.pid, {:release_runtime_cleanup_owner_candidates, ref})
      assert_receive {:expired_owner_generation_checkpoint, owner, ^checkpoint, :authorized, witness}, @budget
      producer = Map.new(witness["producer"], fn {key, value} -> {String.to_existing_atom(key), value} end)
      assert :alive = ExecutionIdentity.status(producer)
      assert :ok = on_actor_node!(owner, GenServer, :stop, [CodexPooler.Platform.ExecutionRegistry, :normal])
      assert {:ok, _registry} = on_actor_node!(owner, GenServer, :start, [CodexPooler.Platform.ExecutionRegistry, nil, [name: CodexPooler.Platform.ExecutionRegistry]])
      assert :unknown = ExecutionIdentity.status(producer)
      attempt = Repo.get!(Attempt, witness["attempt_id"])
      assert {:ok, ^attempt} = ExpiredCleanup.observe_pending(attempt)
      slot = "sample-unknown-slot-#{Ecto.UUID.generate()}"
      presence = record_fixture_presence!(owner, slot)
      Repo.update_all(from(i in PresenceInstance, where: i.instance_id == ^presence.instance_id), set: [last_seen_at: DateTime.add(db_now(), -600, :second)])
      assert {:ok, ^attempt} = ExpiredCleanup.observe_pending(attempt)
      successor = BackendCodexWebsocketOwnerForwardingSupport.start_bridge_peer!(:current, fixture.setup.identity, repo: :real)
      record_fixture_presence_node!(successor, "sample-different-slot-#{Ecto.UUID.generate()}")
      assert {:ok, ^attempt} = ExpiredCleanup.observe_pending(attempt)
      corrupted = put_in(witness, ["producer", "owner_instance_boot_id"], nil)
      invalid_attempt = %{attempt | response_metadata: %{"expired_owner_stop" => corrupted}}
      assert {:error, :invalid_expired_owner_stop} = ExpiredCleanup.read(invalid_attempt)
      assert {:ok, ^invalid_attempt} = ExpiredCleanup.observe_pending(invalid_attempt)
      assert {:ok, ^witness} = ExpiredCleanup.read(Repo.get!(Attempt, attempt.id))
      assert Process.alive?(fixture.handler)
      send(owner, {:release_expired_owner_generation_checkpoint, checkpoint})
      assert :owner_unavailable = assert_cleanup_failure!(cleanup, fixture)
      assert :none = ExpiredCleanup.read(Repo.get!(Attempt, attempt.id))
      release_provider(fixture.handler, fixture.carrier, fixture.gate)
      {conn, ws, text} = public_websocket_receive_text!(fixture.transport.conn, fixture.transport.ws, fixture.transport.ref)
      assert CodexPooler.JSON.decode!(text)["type"] == "response.completed"
      [terminal] = await_settled!(fixture.setup, 1)
      assert terminal.status == "succeeded"
      assert_settled_once!([terminal])
      close_transport!(%{fixture.transport | conn: conn, ws: ws}, fixture.carrier)
      CodexPooler.TestDiagnostics.puts(fn -> CodexPooler.JSON.encode!(%{scenario: "actual_unknown_producer_controls", real_registry_replacement: true, missing_presence_no_end: true, stale_presence_no_end: true, different_slot_no_end: true, null_boot_no_end: true, healthy_work_not_stopped: true, genuine_completed_frame: true, physical_sends: 1}) end)
    end)
  end

  for carrier <- [:owner_local, :owner_remote], rollback <- [:authorization, :second_guard] do
    @tag mode: "full", carrier: carrier, boundary: :disconnect_wins, lease_ttl: 1, rollback_stage: rollback, mailbox_expired_rollback: true
    @tag slow: "actual guarded PostgreSQL rollback before local signal with live completed frame"
    test "#{carrier} #{rollback} rollback leaves healthy task and no cause", context do
      with_info_log(fn ->
        fixture = open_scenario!(context)
        put_owned_actor_env!(fixture.actor, :expired_owner_generation_rollback, context.rollback_stage)
        fixture = observe_boundary!(fixture)
        task = active_owner_task!(fixture.actor)
        {cleanup, ref} = fixture.cleanup
        send(cleanup.pid, {:release_runtime_cleanup_owner_candidates, ref})
        assert :sample_expired_owner_rollback = assert_cleanup_failure!(cleanup, fixture)
        assert on_actor_node!(fixture.actor, Process, :alive?, [task])
        assert Process.alive?(fixture.handler)
        assert :none = ExpiredCleanup.read(Repo.get_by!(Attempt, request_id: fixture.original.id))
        assert ledger_kinds!(fixture.original.id) == %{"reservation" => 1}
        release_provider(fixture.handler, fixture.carrier, fixture.gate)
        {conn, ws, text} = public_websocket_receive_text!(fixture.transport.conn, fixture.transport.ws, fixture.transport.ref)
        assert CodexPooler.JSON.decode!(text)["type"] == "response.completed"
        [terminal] = await_settled!(fixture.setup, 1)
        assert terminal.status == "succeeded"
        assert_settled_once!([terminal])
        close_transport!(%{fixture.transport | conn: conn, ws: ws}, fixture.carrier)
        CodexPooler.TestDiagnostics.puts(fn -> CodexPooler.JSON.encode!(%{scenario: "actual_expired_guard_rollback", carrier: fixture.carrier, stage: context.rollback_stage, sql_rollback: true, cause_absent: true, task_not_stopped: true, genuine_completed_frame: true, physical_sends: 1}) end)
      end)
    end
  end

  for carrier <- [:owner_local, :owner_remote] do
    @tag mode: "full", carrier: carrier, boundary: :disconnect_wins, lease_ttl: 1, mailbox_expired_post_stop_token: true
    @tag slow: "real token takeover after physical end must leave new Session and Lease untouched"
    test "#{carrier} post-stop takeover finalizes only the old tuple", context do
      with_info_log(fn ->
        fixture = open_scenario!(context)
        checkpoint = arm_expired_checkpoint!(fixture.actor, :ended)
        fixture = observe_boundary!(fixture)
        close_transport!(fixture.transport, fixture.carrier)
        {cleanup, ref} = fixture.cleanup
        send(cleanup.pid, {:release_runtime_cleanup_owner_candidates, ref})
        assert_receive {:expired_owner_generation_checkpoint, owner, ^checkpoint, :ended, _ended}, @budget
        opts = RequestOptions.for_websocket(%{bridge_owner_lease_ttl_seconds: 30})
        assert {:ok, renewed} = on_actor_node!(owner, SessionContinuity, :renew_owner_token, [fixture.old_session, fixture.old_session.owner_lease_token, opts, [take_over_expired: true]])
        assert renewed.owner_lease_token != fixture.old_session.owner_lease_token
        lease = Repo.get_by!(BridgeOwnerLease, codex_session_id: renewed.id, status: "active")
        send(owner, {:release_expired_owner_generation_checkpoint, checkpoint})
        assert {:ok, _summary} = Task.await(cleanup, @budget)
        assert_exact_fixture_executions_down!(fixture)
        [terminal] = await_settled!(fixture.setup, 1)
        assert_expired_stop_after_executor_down!(fixture, terminal)
        actual_session = Repo.get!(CodexSession, renewed.id)
        actual_lease = Repo.get!(BridgeOwnerLease, lease.id)
        fields = [:owner_instance_id, :owner_instance_boot_id, :owner_lease_token, :owner_lease_expires_at, :status]
        assert Map.take(actual_session, fields) == Map.take(renewed, fields)
        assert Map.take(actual_lease, [:lease_token, :expires_at, :status]) == Map.take(lease, [:lease_token, :expires_at, :status])
        assert_settled_once!([terminal])
        assert FakeUpstream.count(fixture.upstream) == 1
        CodexPooler.TestDiagnostics.puts(fn -> CodexPooler.JSON.encode!(%{scenario: "actual_post_stop_token_takeover", carrier: fixture.carrier, real_takeover: true, newer_session_lease_unchanged: true, old_request_only: true, response_status_code: 499, physical_sends: 1}) end)
      end)
    end
  end

  for mutation <- [:principal_epoch, :execution_identity, :replay_generation, :owner_boot, :request_model, :unrelated_turn] do
    @tag mode: "full", carrier: :owner_local, boundary: :disconnect_wins, lease_ttl: 1, guard_mutation: mutation, mailbox_expired_scope_guard: true
    @tag slow: "real locked scope mutation before second guard preserves actual old task"
    test "second guard refuses#{mutation} without stopping unrelated scope", context do
      with_info_log(fn ->
        fixture = open_scenario!(context)
        checkpoint = arm_expired_checkpoint!(fixture.actor, :authorized)
        fixture = observe_boundary!(fixture)
        task = active_owner_task!(fixture.actor)
        {cleanup, ref} = fixture.cleanup
        send(cleanup.pid, {:release_runtime_cleanup_owner_candidates, ref})
        assert_receive {:expired_owner_generation_checkpoint, owner, ^checkpoint, :authorized, witness}, @budget
        restore = mutate_expired_scope!(fixture, witness, context.guard_mutation)
        send(owner, {:release_expired_owner_generation_checkpoint, checkpoint})
        assert_cleanup_failure!(cleanup, fixture)
        assert Process.alive?(task) and Process.alive?(fixture.handler)
        assert Repo.get!(Request, fixture.original.id).status == "in_progress"
        assert ledger_kinds!(fixture.original.id) == %{"reservation" => 1}
        restore.()
        release_provider(fixture.handler, fixture.carrier, fixture.gate)
        {conn, ws, text} = public_websocket_receive_text!(fixture.transport.conn, fixture.transport.ws, fixture.transport.ref)
        assert CodexPooler.JSON.decode!(text)["type"] == "response.completed"
        [terminal] = await_settled!(fixture.setup, 1)
        assert terminal.status == "succeeded"
        assert_settled_once!([terminal])
        close_transport!(%{fixture.transport | conn: conn, ws: ws}, fixture.carrier)
        CodexPooler.TestDiagnostics.puts(fn -> CodexPooler.JSON.encode!(%{scenario: "actual_second_guard_scope_refusal", mutation: context.guard_mutation, actual_task_preserved: true, no_generation_stop: true, no_extra_ledger: true, restored_owned_fixture_completed: true, physical_sends: FakeUpstream.count(fixture.upstream)}) end)
      end)
    end
  end

  defp mutate_expired_scope!(_fixture, witness, :principal_epoch), do: change_owned_scope!(Request, witness["request_id"], :native_client_retry_auth_epoch, 1)
  defp mutate_expired_scope!(_fixture, witness, :execution_identity), do: change_owned_scope!(Attempt, witness["attempt_id"], :owner_execution_id, Ecto.UUID.generate())
  defp mutate_expired_scope!(_fixture, witness, :replay_generation), do: change_owned_scope!(Attempt, witness["attempt_id"], :replay_generation, 1)
  defp mutate_expired_scope!(_fixture, witness, :owner_boot), do: change_owned_scope!(CodexSession, witness["session_id"], :owner_instance_boot_id, "sample-other-boot")

  defp mutate_expired_scope!(fixture, witness, :request_model) do
    model = CodexPooler.PoolerFixtures.model_fixture(fixture.setup.pool, %{exposed_model_id: "sample-other-model-#{Ecto.UUID.generate()}"})
    change_owned_scope!(Request, witness["request_id"], :model_id, model.id)
  end

  defp mutate_expired_scope!(fixture, witness, :unrelated_turn) do
    turn = Repo.get!(CodexTurn, witness["turn_id"])
    clock = db_now()
    assert {:ok, auth} = Access.authenticate_authorization_header(fixture.setup.authorization)
    assert {:ok, reserved} = Accounting.reserve(auth, fixture.setup.model, fixture.payload, %{correlation_id: Ecto.UUID.generate(), transport: "websocket"})
    other = Repo.insert!(%CodexTurn{codex_session_id: fixture.old_session.id, request_id: reserved.request.id, turn_sequence: turn.turn_sequence + 1, transport_kind: "websocket", status: "in_progress", created_at: clock, updated_at: clock, started_at: clock})

    restore = fn ->
      Repo.delete_all(from t in CodexTurn, where: t.id == ^other.id)
      Repo.delete_all(from r in Request, where: r.id == ^reserved.request.id)
    end

    on_exit(restore)
    restore
  end

  defp change_owned_scope!(schema, id, field, value) do
    row = Repo.get!(schema, id)
    old = Map.fetch!(row, field)
    Repo.update!(Ecto.Changeset.change(row, %{field => value}))
    restore = fn -> Repo.update_all(from(r in schema, where: r.id == ^id), set: [{field, old}]) end
    on_exit(restore)
    restore
  end

  @tag mode: "full", carrier: :owner_remote, boundary: :disconnect_wins, lease_ttl: 1, mailbox_expired_checkout_queue: true, owner_call_budget: :product
  @tag slow: "real serving Repo checkout queue remains bounded by existing owner budget"
  test "never granted second SQL checkout returns before owner call budget without guessing stop", context do
    with_info_log(fn ->
      fixture = open_scenario!(context)
      checkpoint = arm_expired_checkpoint!(fixture.actor, :authorized)
      fixture = observe_boundary!(fixture)
      {cleanup, ref} = fixture.cleanup
      started = System.monotonic_time(:millisecond)
      send(cleanup.pid, {:release_runtime_cleanup_owner_candidates, ref})
      assert_receive {:expired_owner_generation_checkpoint, owner, ^checkpoint, :authorized, witness}, @budget
      holders = hold_peer_pool!(owner)
      assert Enum.map(holders, & &1.backend) |> Enum.uniq() |> length() == 2
      send(owner, {:release_expired_owner_generation_checkpoint, checkpoint})
      assert :owner_unavailable = assert_cleanup_failure!(cleanup, fixture)
      assert System.monotonic_time(:millisecond) - started < 5000
      assert Process.alive?(fixture.handler)
      assert {:ok, ^witness} = ExpiredCleanup.read(Repo.get!(Attempt, witness["attempt_id"]))
      assert Repo.get!(Request, fixture.original.id).status == "in_progress"
      assert {:ok, _status} = WebsocketOwnerSession.owner_status(owner)
      release_peer_pool!(holders)
      release_provider(fixture.handler, fixture.carrier, fixture.gate)
      {conn, ws, text} = public_websocket_receive_text!(fixture.transport.conn, fixture.transport.ws, fixture.transport.ref)
      assert CodexPooler.JSON.decode!(text)["type"] == "response.completed"
      [terminal] = await_settled!(fixture.setup, 1)
      assert terminal.status == "succeeded"
      assert_settled_once!([terminal])
      close_transport!(%{fixture.transport | conn: conn, ws: ws}, fixture.carrier)
      CodexPooler.TestDiagnostics.puts(fn -> CodexPooler.JSON.encode!(%{scenario: "actual_never_granted_sql_checkout", real_pool_connections_held: 2, callback_bounded: true, authorization_retained_unknown: true, no_stop: true, genuine_completed_frame: true, physical_sends: 1}) end)
    end)
  end

  defp hold_peer_pool!(owner) do
    Enum.map(1..2, fn _ ->
      ref = make_ref()
      holder_key = {__MODULE__, :checkout_holder, ref}

      on_exit(fn -> release_registered_checkout(holder_key, ref) end)

      assert {:ok, pid} = on_actor_node!(owner, MailboxPrefixRaceSupport, :hold_peer_checkout, [self(), ref])
      :persistent_term.put(holder_key, pid)
      assert_receive {:owned_peer_checkout, ^pid, ^ref, backend}, @budget
      %{pid: pid, ref: ref, backend: backend}
    end)
  end

  defp release_peer_pool!(holders), do: Enum.each(holders, &send(&1.pid, {:release_owned_peer_checkout, &1.ref}))

  defp release_registered_checkout(holder_key, ref) do
    case :persistent_term.get(holder_key, nil) do
      nil -> :ok
      pid -> send(pid, {:release_owned_peer_checkout, ref})
    end

    :persistent_term.erase(holder_key)
  end

  defp put_owned_actor_env!(owner, key, value) do
    previous = on_actor_node!(owner, Application, :fetch_env, [:codex_pooler, key])

    on_exit(fn ->
      if node(owner) == node() or node(owner) in Node.list(), do: restore_owned_actor_env!(owner, key, previous)
    end)

    :ok = on_actor_node!(owner, Application, :put_env, [:codex_pooler, key, value])
  end

  defp restore_owned_actor_env!(owner, key, {:ok, value}), do: on_actor_node!(owner, Application, :put_env, [:codex_pooler, key, value])
  defp restore_owned_actor_env!(owner, key, :error), do: on_actor_node!(owner, Application, :delete_env, [:codex_pooler, key])

  defp record_fixture_presence!(owner, slot), do: record_fixture_presence_node!(node(owner), slot)

  defp record_fixture_presence_node!(target, slot) do
    :ok = :erpc.call(target, Application, :put_env, [:codex_pooler, :instance_slot_id, slot])
    identity = :erpc.call(target, InstancePresence, :local_identity, [])
    on_exit(fn -> Repo.delete_all(from i in PresenceInstance, where: i.instance_id == ^identity.instance_id) end)
    assert {:ok, _failed_beats} = :erpc.call(target, CodexPooler.InstancePresencePeer, :publish_local_presence, [@budget], @budget + 5_000)
    Repo.get!(PresenceInstance, identity.instance_id)
  end

  defp pause_fixture_executor!(executor) do
    on_exit(fn -> resume_fixture_executor!(executor) end)
    assert true = on_actor_node!(executor, :erlang, :suspend_process, [executor])
  end

  defp resume_fixture_executor!(executor) do
    if node(executor) == node() or node(executor) in Node.list() do
      try do
        on_actor_node!(executor, :erlang, :resume_process, [executor])
      catch
        :error, :badarg -> :ok
      end
    end
  end

  defp await_peer_disconnected!(peer, deadline) do
    if peer in Node.list() do
      assert System.monotonic_time(:millisecond) < deadline

      receive do
      after
        10 -> await_peer_disconnected!(peer, deadline)
      end
    end
  end

  defp expired_candidate!(session_id) do
    session = Repo.get!(CodexSession, session_id)
    %{session_id: session.id, owner_instance_id: session.owner_instance_id, owner_instance_boot_id: session.owner_instance_boot_id, owner_lease_token: session.owner_lease_token, owner_lease_expires_at: session.owner_lease_expires_at}
  end

  defp hold_session_row!(session_id) do
    supervisor = start_supervised!(Task.Supervisor)
    parent = self()
    ref = make_ref()

    task =
      Task.Supervisor.async_nolink(supervisor, fn ->
        Repo.transaction(fn ->
          Repo.one!(from s in CodexSession, where: s.id == ^session_id, lock: "FOR UPDATE")
          [[backend]] = Repo.query!("SELECT pg_backend_pid()").rows
          send(parent, {:session_row_held, ref, backend})
          receive do: ({:release_session_row, ^ref} -> :ok)
        end)
      end)

    on_exit(fn ->
      send(task.pid, {:release_session_row, ref})
      if Process.alive?(task.pid), do: Task.shutdown(task, :brutal_kill)
    end)

    assert_receive {:session_row_held, ^ref, backend}, @budget
    %{task: task, ref: ref, backend: backend}
  end

  defp release_session_row!(%{task: task, ref: ref}) do
    send(task.pid, {:release_session_row, ref})
    assert {:ok, :ok} = Task.await(task, @budget)
  end

  defp await_session_waiter!(holder, deadline) do
    Repo.query!("SELECT pg_stat_clear_snapshot()")
    rows = Repo.query!("SELECT a.pid FROM pg_stat_activity a WHERE a.datname=current_database() AND a.state='active' AND a.wait_event_type='Lock' AND $1=ANY(pg_blocking_pids(a.pid)) AND a.query LIKE '%codex_sessions%'", [holder]).rows

    case rows do
      [[waiter] | _] ->
        waiter

      [] ->
        assert System.monotonic_time(:millisecond) < deadline, "actual second guard did not reach PostgreSQL row wait"

        receive do
        after
          10 -> await_session_waiter!(holder, deadline)
        end
    end
  end

  # The deadline's cut returns before the backend reacts: Postgrex cancels and closes the connection on its own
  # process while the caller moves on, so the waiter is still in its row-lock wait for a moment and only then aborts
  # its transaction (findings#303 row 303-9). The holder keeps the row throughout, so what is polled, from fresh
  # snapshots, is the backend ending its own transaction, never a release by the holder.
  defp assert_backend_transaction_released!(backend), do: await_backend_transaction_released!(backend, System.monotonic_time(:millisecond) + @budget)

  defp await_backend_transaction_released!(backend, deadline) do
    Repo.query!("SELECT pg_stat_clear_snapshot()")
    rows = Repo.query!("SELECT state, wait_event_type, wait_event FROM pg_stat_activity WHERE pid=$1 AND xact_start IS NOT NULL", [backend]).rows

    case rows do
      [] ->
        :ok

      [[state, wait_event_type, wait_event]] ->
        assert System.monotonic_time(:millisecond) < deadline, "backend #{backend} still holds its transaction: state=#{state} wait_event=#{wait_event_type}/#{wait_event}"

        receive do
        after
          10 -> await_backend_transaction_released!(backend, deadline)
        end
    end
  end

  defp active_owner_task!(owner) do
    state = :sys.get_state(owner, @budget)
    state.active_turn.task_pid
  end

  defp arm_expired_checkpoint!(owner, stage) do
    ref = make_ref()
    target = node(owner)
    previous = on_actor_node!(owner, Application, :fetch_env, [:codex_pooler, :expired_owner_generation_checkpoint])

    on_exit(fn ->
      send(owner, {:release_expired_owner_generation_checkpoint, ref})

      if target == node() or target in Node.list() do
        restore_expired_checkpoint!(owner, previous)
      end
    end)

    :ok = on_actor_node!(owner, Application, :put_env, [:codex_pooler, :expired_owner_generation_checkpoint, {self(), ref, stage}])
    ref
  end

  defp restore_expired_checkpoint!(owner, {:ok, value}), do: on_actor_node!(owner, Application, :put_env, [:codex_pooler, :expired_owner_generation_checkpoint, value])
  defp restore_expired_checkpoint!(owner, :error), do: on_actor_node!(owner, Application, :delete_env, [:codex_pooler, :expired_owner_generation_checkpoint])

  defp on_actor_node!(owner, module, function, args) do
    if node(owner) == node(), do: apply(module, function, args), else: :erpc.call(node(owner), module, function, args)
  end

  defp assert_exact_fixture_executions_down!(fixture) do
    %{generation_monitor: generation_monitor, generation_actor: generation_actor, execution_monitor: execution_monitor, executor: executor} = fixture
    assert_receive {:DOWN, ^generation_monitor, :process, ^generation_actor, _reason}, @budget
    assert_receive {:DOWN, ^execution_monitor, :process, ^executor, _reason}, @budget
  end

  defp ledger_kinds!(request_id), do: Repo.all(from l in LedgerEntry, where: l.request_id == ^request_id, select: l.entry_kind) |> Enum.frequencies()

  defp install_end_write_failure! do
    name = "sample_expired_stop_#{System.unique_integer([:positive])}"
    on_exit(fn -> drop_end_write_failure!(name) end)
    Repo.query!("CREATE FUNCTION #{name}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.response_metadata->'expired_owner_stop'->>'phase' = 'ended' THEN RAISE EXCEPTION 'sample ended receipt write unavailable' USING ERRCODE = '23514'; END IF; RETURN NEW; END; $$")
    Repo.query!("CREATE TRIGGER #{name} BEFORE UPDATE OF response_metadata ON attempts FOR EACH ROW EXECUTE FUNCTION #{name}()")
    name
  end

  defp drop_end_write_failure!(name) do
    Repo.query!("DROP TRIGGER IF EXISTS #{name} ON attempts")
    Repo.query!("DROP FUNCTION IF EXISTS #{name}()")
    :ok
  end

  defp run_scenario(context) do
    context
    |> open_scenario!()
    |> observe_boundary!()
    |> settle_predecessor!()
    |> expire_settled_predecessor!()
    |> admit_successor!()
    |> emit_lifecycle!()
  end

  defp open_scenario!(%{mode: mode, carrier: carrier, boundary: boundary, lease_settings: settings} = context) do
    Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, carrier != :http)
    gate = make_ref()
    output = %{"type" => "reasoning", "id" => "rs_synthetic_lease", "summary" => [], "encrypted_content" => "synthetic_lease"}
    first = predecessor_response(carrier, output, gate)
    upstream = start_upstream(FakeUpstream.strict_sequence([first, FakeUpstream.sse_stream([{"response.completed", completed()}])]))
    setup = gateway_setup(upstream, compact?: true)
    register_unboxed_pool_cleanup!(setup)
    set_model_serving_mode!(model_serving_scope(), setup, mode)
    thread = Ecto.UUID.generate()
    window = "#{thread}:0"
    queue_peer = if Map.get(context, :mailbox_expired_checkout_queue), do: context.queue_peer
    peer = prepare_peer!(carrier, setup, thread, settings, queue_peer)
    put_owner_defaults!(peer, context)
    port = start_public_endpoint!()
    if carrier == :http, do: MailboxPrefixRaceSupport.observe_http_heartbeat!(self())
    payload = payload(setup, thread, window)
    transport = open_prefix!(carrier, port, setup, thread, window, payload, mode, output)
    handler = await_provider_gate!(carrier, gate)
    on_exit(fn -> release_provider(handler, carrier, gate) end)
    [original] = requests(setup)
    attempt = Repo.get_by!(Attempt, request_id: original.id)
    execution_node = Enum.find([node() | Node.list()], &(Atom.to_string(&1) == attempt.owner_instance_id))
    assert is_atom(execution_node) and execution_node != nil
    executor = :erpc.call(execution_node, :erlang, :list_to_pid, [String.to_charlist(attempt.owner_process_id)])
    assert :erpc.call(execution_node, Process, :alive?, [executor])
    execution_monitor = Process.monitor(executor)
    generation_actor = if carrier == :http, do: executor, else: handler
    generation_monitor = Process.monitor(generation_actor)

    if carrier == :http do
      assert original.admission_process_id == attempt.owner_process_id
      assert original.admission_execution_id == attempt.owner_execution_id
      assert is_binary(original.admission_execution_id)
      assert ExecutionRegistry.status(original.admission_execution_id, generation_actor) == :alive
    end

    turn = Repo.get_by!(CodexTurn, request_id: original.id)
    old_session = Repo.get!(CodexSession, turn.codex_session_id)
    actor = owning_actor!(carrier, old_session.id, peer)
    assert node(actor) == if(carrier == :owner_remote, do: peer, else: node())

    %{mode: mode, carrier: carrier, boundary: boundary, claim_ttl: claim_ttl(context), setup: setup, upstream: upstream, port: port, thread: thread, window: window, payload: payload, transport: transport, handler: handler, gate: gate, original: original, executor: executor, execution_node: execution_node, execution_monitor: execution_monitor, generation_actor: generation_actor, generation_monitor: generation_monitor, actor: actor, old_session: old_session}
  end

  # A test's `lease_ttl` tag names the ttl of its claim; without one a healthy boundary observes the three-second lease that
  # renewal sustains and every other boundary a one-second lease that really expires.
  defp claim_ttl(%{lease_ttl: ttl}) when is_integer(ttl), do: ttl
  defp claim_ttl(%{boundary: :healthy}), do: @healthy_claim_ttl
  defp claim_ttl(_context), do: @expiry_claim_ttl

  defp observe_boundary!(%{boundary: boundary, carrier: carrier, actor: actor, old_session: old_session, original: original, upstream: upstream, claim_ttl: claim_ttl} = fixture) do
    # Explicit fixture-only negative profile: the identical healthy oracle must
    # fail when only its owning actor's real periodic scheduling is disabled.
    renewal_off_red? = boundary == :healthy and System.get_env("CODEX_POOLER_TEST_MAILBOX_RENEWAL_OFF_RED") == "1"
    suppressed? = boundary in [:disconnect_wins, :cleanup_wins] or renewal_off_red?

    if suppressed? do
      MailboxPrefixRaceSupport.suppress_owned_periodic_renewal!(actor, if(carrier == :http, do: :http, else: :owner))
    end

    initial = open_lease_window!(old_session.id, boundary, claim_ttl, suppressed?)
    crossed = if boundary == :settled_then_expiry, do: initial, else: await_clock!(old_session.id, initial.session_deadline)

    assert_boundary_clock!(boundary, initial, crossed)

    assert Repo.get!(Request, original.id).status == "in_progress"
    assert FakeUpstream.count(upstream) == 1
    cleanup = if boundary in [:disconnect_wins, :cleanup_wins], do: start_cleanup_selection!(old_session.id)

    Map.merge(fixture, %{initial: initial, crossed: crossed, cleanup: cleanup})
  end

  # The scenario ran on the stable ttl; the lease the boundary is about starts here. A settled predecessor is only observed: its
  # expiry is the claim of `expire_settled_predecessor!/1`, which writes the short ttl once the owner is idle.
  defp open_lease_window!(session_id, :settled_then_expiry, _claim_ttl, _suppressed?) do
    initial = MailboxLeaseLifecycleSupport.observe!(session_id)
    assert initial.session_deadline == initial.lease_deadline
    assert DateTime.compare(initial.clock, initial.session_deadline) == :lt
    initial
  end

  defp open_lease_window!(session_id, _boundary, claim_ttl, suppressed?) do
    MailboxLeaseLifecycleSupport.shorten_lease!(session_id, claim_ttl, suppressed?: suppressed?)
  end

  defp assert_boundary_clock!(:healthy, initial, crossed) do
    assert DateTime.compare(crossed.session_deadline, initial.session_deadline) == :gt, "healthy_periodic_renewal_missing: owning deadline must advance"
    assert DateTime.compare(crossed.session_deadline, crossed.clock) == :gt
  end

  defp assert_boundary_clock!(:settled_then_expiry, _initial, _crossed), do: :ok

  defp assert_boundary_clock!(_boundary, initial, crossed) do
    assert crossed.session_deadline == initial.session_deadline
    assert crossed.lease_deadline == initial.lease_deadline
    assert DateTime.compare(crossed.clock, crossed.session_deadline) != :lt
  end

  defp settle_predecessor!(%{boundary: boundary, carrier: carrier, setup: setup, cleanup: cleanup, transport: transport, handler: handler, gate: gate, executor: executor, original: original, generation_monitor: generation_monitor, generation_actor: generation_actor, execution_monitor: execution_monitor} = fixture) do
    if boundary == :cleanup_wins do
      release_cleanup!(cleanup)
      assert_cleanup_boundary!(fixture)
    end

    close_transport!(transport, carrier)
    if carrier == :http, do: release_provider(handler, carrier, gate)

    release_expired_cut_cleanup!(fixture)

    # An HTTP provider handler can remain alive as an idle keepalive connection;
    # the actual downstream request executor must exit. A websocket cut closes
    # its dedicated provider socket, whose handler must exit independently.
    assert_receive {:DOWN, ^generation_monitor, :process, ^generation_actor, _reason}, @budget, "released_cleanup_must_end_exact_owned_generation: terminal rows alone do not stop the provider"
    assert_receive {:DOWN, ^execution_monitor, :process, ^executor, _reason}, @budget, "exact persisted response execution must finish before the mailbox retry"
    if carrier == :http, do: assert(ExecutionRegistry.status(original.admission_execution_id, generation_actor) == :dead)
    [terminal] = await_settled!(setup, 1)
    assert_expired_stop_after_executor_down!(fixture, terminal)

    assert_predecessor_terminal!(terminal, boundary, carrier)

    if boundary == :disconnect_wins and carrier == :http, do: release_cleanup!(cleanup)
    assert_settled_once!([terminal])
    refute Repo.exists?(from t in CodexTurn, join: r in Request, on: r.id == t.request_id, where: r.pool_id == ^setup.pool.id and t.status == "in_progress")

    Map.put(fixture, :terminal, terminal)
  end

  defp assert_predecessor_terminal!(_terminal, :cleanup_wins, _carrier), do: :ok

  defp assert_predecessor_terminal!(terminal, boundary, carrier) do
    expected_reason = if boundary == :disconnect_wins and carrier != :http, do: "owner_unavailable", else: "client_disconnected"
    assert terminal.last_error_code == expected_reason
    assert terminal.response_status_code == if(carrier == :http, do: 200, else: 499), "expiry recovery status must remain the existing contract"
  end

  defp assert_cleanup_boundary!(%{carrier: :http, original: original, executor: executor, handler: handler}) do
    assert Process.alive?(executor) and Process.alive?(handler)
    assert Repo.get!(Request, original.id).status == "in_progress"
    assert Repo.get_by!(Attempt, request_id: original.id).status == "in_progress"
    assert ledger_kinds!(original.id) == %{"reservation" => 1}
  end

  defp assert_cleanup_boundary!(%{setup: setup}) do
    [terminal] = await_settled!(setup, 1)
    assert terminal.last_error_code == "owner_unavailable"
  end

  defp assert_expired_stop_after_executor_down!(%{boundary: :disconnect_wins, carrier: carrier, original: original}, terminal) when carrier != :http do
    [attempt] = Repo.all(from a in Attempt, where: a.request_id == ^original.id)
    assert attempt.status == "failed" and attempt.network_error_code == "owner_unavailable"
    assert attempt.replay_generation == 0
    assert terminal.status == "failed" and terminal.last_error_code == "owner_unavailable"
    assert terminal.response_status_code == 499
    assert {:ok, %{"phase" => "ended", "observed_end_at" => ended_at}} = ExpiredCleanup.read(attempt)
    assert {:ok, ended_clock, 0} = DateTime.from_iso8601(ended_at)
    assert DateTime.compare(ended_clock, terminal.completed_at) != :gt
    assert_settled_once!([terminal])
    CodexPooler.TestDiagnostics.puts(fn -> CodexPooler.JSON.encode!(%{scenario: "expired_stop_after_exact_executor_down", one_attempt: true, latest_generation: 0, reason_retained: "owner_unavailable", response_status_code: 499, durable_end_before_terminal: true, exact_executor_down: true, same_attempt_takeover: false}) end)
  end

  defp assert_expired_stop_after_executor_down!(_fixture, _terminal), do: :ok

  defp release_expired_cut_cleanup!(%{boundary: :disconnect_wins, carrier: carrier, cleanup: cleanup, setup: setup, mode: mode, handler: handler, execution_node: execution_node, executor: executor, actor: actor, upstream: upstream, original: original}) when carrier != :http do
    release_cleanup!(cleanup)
    [closed] = await_settled!(setup, 1)
    closed_attempt = Repo.get_by!(Attempt, request_id: closed.id)
    entries = Repo.all(from l in LedgerEntry, where: l.request_id == ^closed.id, select: l.entry_kind) |> Enum.frequencies()
    CodexPooler.TestDiagnostics.puts(fn -> CodexPooler.JSON.encode!(%{scenario: "expired_owner_cut_after_released_cleanup", carrier: carrier, mode: mode, cleanup_released: true, request_status: closed.status, response_status_code: closed.response_status_code, terminal_reason: closed.last_error_code, terminal_time: closed.completed_at, attempt_status: closed_attempt.status, attempt_error: closed_attempt.network_error_code, replay_generation: closed_attempt.replay_generation, attempt_completed: closed_attempt.completed_at != nil, ledger: entries, provider_alive_at_terminal: Process.alive?(handler), response_executor_alive_at_terminal: :erpc.call(execution_node, Process, :alive?, [executor]), executor_bound_to_persisted_attempt: true, owner_alive_at_terminal: :erpc.call(node(actor), Process, :alive?, [actor]), physical_sends: FakeUpstream.count(upstream), new_links: Repo.aggregate(from(l in RequestClientRetryLink, where: l.predecessor_request_id == ^original.id), :count)}) end)
  end

  defp release_expired_cut_cleanup!(_fixture), do: :ok

  defp expire_settled_predecessor!(%{boundary: boundary, carrier: carrier, actor: actor, old_session: old_session} = fixture) do
    expiry =
      if boundary == :settled_then_expiry do
        if boundary == :settled_then_expiry and carrier != :http do
          await_idle_owner!(actor, System.monotonic_time(:millisecond) + @budget)
          MailboxLeaseLifecycleSupport.suppress_owned_idle_renewal!(actor)
        end

        session = Repo.get!(CodexSession, old_session.id)
        receipt = MailboxLeaseLifecycleSupport.renew_and_observe_expiry!(session)
        if carrier != :http, do: MailboxLeaseLifecycleSupport.stop_owned_after_expiry!(actor)
        receipt
      end

    Map.put(fixture, :expiry, expiry)
  end

  defp admit_successor!(%{payload: payload, transport: transport, carrier: carrier, port: port, setup: setup, thread: thread, window: window, mode: mode, boundary: boundary, original: original, old_session: old_session, upstream: upstream} = fixture) do
    continuation = append_mailbox(payload, transport.retained)
    response = send_successor!(carrier, port, setup, thread, window, continuation, mode)

    if boundary in [:cleanup_wins, :disconnect_wins] and carrier != :http do
      assert response == :refused
      assert FakeUpstream.count(upstream) == 1
      assert length(requests(setup)) == 1
      refute Repo.exists?(from l in RequestClientRetryLink, where: l.predecessor_request_id == ^original.id)
    else
      assert response == :completed
      [predecessor, successor] = await_settled!(setup, 2)
      assert successor.status == "succeeded"
      assert successor.request_metadata["client_resend"]["predecessor_request_id"] == predecessor.id
      assert [%RequestClientRetryLink{successor_request_id: successor_id}] = Repo.all(from l in RequestClientRetryLink, where: l.predecessor_request_id == ^predecessor.id)
      assert successor_id == successor.id
      successor_turn = Repo.get_by!(CodexTurn, request_id: successor.id)
      if boundary == :settled_then_expiry, do: refute(successor_turn.codex_session_id == old_session.id), else: assert(successor_turn.codex_session_id == old_session.id)
      assert_settled_once!([predecessor, successor])
      assert FakeUpstream.count(upstream) == 2
      assert :ok = FakeUpstream.verify!(upstream)
    end

    fixture
  end

  defp emit_lifecycle!(%{setup: setup, mode: mode, carrier: carrier, boundary: boundary, actor: actor, initial: initial, crossed: crossed, expiry: expiry, terminal: terminal, upstream: upstream}) do
    WebsocketCleanupFence.await_session_cleanups!()
    refute Repo.exists?(from r in Request, where: r.pool_id == ^setup.pool.id and r.status == "in_progress")
    CodexPooler.TestDiagnostics.puts(fn -> CodexPooler.JSON.encode!(%{scenario: "mailbox_lease_boundary", mode: mode, carrier: carrier, boundary: boundary, chronology: if(boundary == :settled_then_expiry, do: "physical_cut_then_client_disconnected_settlement_then_real_idle_expiry", else: "wait_beyond_initial_ttl_then_physical_cut"), initial_ttl_crossing_observed: boundary != :settled_then_expiry, actual_remote_owner: node(actor) != node(), initial: initial, after_initial_ttl: crossed, real_postsettlement_expiry: expiry, cleanup_selection_observed: boundary in [:disconnect_wins, :cleanup_wins], provider_down_observed: carrier != :http, owning_generation_actor_down_observed: true, terminal_reason: terminal.last_error_code, terminal_time: terminal.completed_at, retry_age_us: DateTime.diff(db_now(), terminal.completed_at, :microsecond), physical_sends: FakeUpstream.count(upstream), request_count: length(requests(setup)), settlements_per_request: 1}) end)
  end

  defp prepare_peer!(carrier, setup, thread, settings, nil), do: prepare_peer!(carrier, setup, thread, settings)

  defp prepare_peer!(:owner_remote, setup, thread, settings, peer) do
    :ok = :erpc.call(peer, Application, :put_env, [:codex_pooler, OperationalSettings, [settings: settings]])
    start_serving_vm_owner!(setup, thread, peer)
    peer
  end

  defp prepare_peer!(:owner_remote, setup, thread, settings) do
    BackendCodexWebsocketOwnerForwardingSupport.ensure_test_distribution_started!()
    peer = BackendCodexWebsocketOwnerForwardingSupport.start_bridge_peer!(:current, setup.identity, repo: :real)
    previous = :erpc.call(peer, Application, :get_env, [:codex_pooler, OperationalSettings, []])
    on_exit(fn -> if peer in Node.list(), do: :erpc.call(peer, Application, :put_env, [:codex_pooler, OperationalSettings, previous]) end)
    :ok = :erpc.call(peer, Application, :put_env, [:codex_pooler, OperationalSettings, [settings: settings]])

    start_serving_vm_owner!(setup, thread, peer)

    peer
  end

  defp prepare_peer!(_carrier, _setup, _window, _settings), do: nil

  # The owner on a peer works under the peer's own budget. Only on a peer this test booted: the shared queue peer belongs to the
  # test that claims the product's budget.
  defp put_owner_defaults!(peer, %{owner_defaults: [_ | _] = defaults, carrier: :owner_remote}) when is_atom(peer) and peer != nil, do: :ok = :erpc.call(peer, Application, :put_env, [:codex_pooler, OwnerDefaults, defaults])
  defp put_owner_defaults!(_peer, _context), do: :ok

  defp start_serving_vm_owner!(setup, thread, peer) do
    assert {:ok, _registry} = :erpc.call(peer, GenServer, :start, [ExecutionRegistry, nil, [name: ExecutionRegistry]])
    assert {:ok, auth} = Access.authenticate_authorization_header(setup.authorization)
    assert {:ok, session} = :erpc.call(peer, Gateway, :start_codex_session, [auth, %{accepted_turn_state: thread}])
    lease = Repo.get_by!(CodexPooler.Gateway.Persistence.BridgeOwnerLease, codex_session_id: session.id, status: "active")
    boot = :erpc.call(peer, PresenceIdentity, :boot_id, [])
    assert is_binary(lease.owner_instance_boot_id)
    assert lease.owner_instance_boot_id == boot
    persistence = :erpc.call(peer, CodexPooler.Gateway.Transports.WebsocketOwnerNodeHarness, :real_persistence_boundary, [])
    assert {:ok, owner} = :erpc.call(peer, WebsocketOwnerSession, :start_owner, [[codex_session_id: session.id, owner_lease_token: session.owner_lease_token, owner_instance_id: session.owner_instance_id, owner_renewal_ms: 1_000, persistence: persistence]])
    on_exit(fn -> stop_serving_vm_owner!(peer, owner) end)
    :ok
  end

  defp stop_serving_vm_owner!(peer, owner) do
    if peer in Node.list() and :erpc.call(peer, Process, :alive?, [owner]) do
      monitor = Process.monitor(owner)

      try do
        :erpc.call(peer, GenServer, :stop, [owner, :normal, 5_000])
      catch
        :exit, _concurrent_shutdown -> :ok
      end

      assert_receive {:DOWN, ^monitor, :process, ^owner, reason}, @budget
      assert reason in [:normal, :noproc, {:shutdown, :stale_owner}]
    end
  end

  defp owning_actor!(:http, _session, _peer) do
    assert_receive {:owned_http_heartbeat, heartbeat}, @budget
    heartbeat
  end

  defp owning_actor!(:owner_remote, session, peer) do
    assert {:ok, owner} = :erpc.call(peer, WebsocketOwnerSession, :lookup, [session])
    owner
  end

  defp owning_actor!(:owner_local, session, _peer) do
    assert {:ok, owner} = WebsocketOwnerSession.lookup(session)
    owner
  end

  defp await_idle_owner!(owner, deadline) do
    if :sys.get_state(owner, @budget).active_turn != nil do
      assert System.monotonic_time(:millisecond) < deadline, "owned executor ended but owner still tracks its turn"

      receive do
      after
        10 -> await_idle_owner!(owner, deadline)
      end
    end
  end

  defp start_cleanup_selection!(session_id) do
    ref = make_ref()
    CodexPooler.TestAppEnv.restore_on_exit(:runtime_cleanup_owner_candidate_test_barrier)
    Application.put_env(:codex_pooler, :runtime_cleanup_owner_candidate_test_barrier, {self(), ref})
    task = Task.async(fn -> receive do: (:start_cleanup -> RuntimeCleanup.cleanup_expired_runtime_state(db_now())) end)

    on_exit(fn ->
      send(task.pid, {:release_runtime_cleanup_owner_candidates, ref})
      if Process.alive?(task.pid), do: Task.shutdown(task, :brutal_kill)
    end)

    send(task.pid, :start_cleanup)
    assert_receive {:runtime_cleanup_owner_candidates_selected, cleanup, ^ref, candidates}, @budget
    assert cleanup == task.pid
    assert Enum.any?(candidates, &(&1.session_id == session_id))
    {task, ref}
  end

  defp release_cleanup!({task, ref}) do
    send(task.pid, {:release_runtime_cleanup_owner_candidates, ref})
    assert {:ok, _summary} = Task.await(task, @budget)
  end

  defp assert_cleanup_failure!(task, fixture) do
    session_id = fixture.old_session.id

    assert {:error, {:runtime_cleanup_failed, [owner_recovery: [{^session_id, reason}]]}, summary} = Task.await(task, @budget)
    assert summary.expired_owner_sessions_failed == 1
    assert summary.expired_owner_sessions_recovered == 0
    reason
  end

  defp predecessor_response(:http, output, gate) do
    first = event(%{"type" => "response.output_item.done", "item" => output})
    tail = event(%{"type" => "response.reasoning_text.delta", "delta" => String.duplicate("synthetic", 10_000)})
    {:gated_terminal_sse, [first], [tail], self(), gate}
  end

  defp predecessor_response(_carrier, output, gate) do
    frames = [%{"type" => "response.created", "response" => %{"id" => "resp_synthetic_lease", "status" => "in_progress", "output" => []}}, %{"type" => "response.output_item.added", "output_index" => 0, "item" => output}, %{"type" => "response.output_item.done", "output_index" => 0, "item" => output}]
    FakeUpstream.delayed_terminal_sse_stream(frames, completed(), notify: self(), release_ref: gate)
  end

  defp await_provider_gate!(:http, gate) do
    assert_receive {:fake_upstream_gate, :before_terminal, handler, ^gate}, @budget
    handler
  end

  defp await_provider_gate!(_carrier, gate) do
    assert_receive {:fake_upstream_timeout_barrier, :before_terminal, handler, ^gate}, @budget
    handler
  end

  defp release_provider(handler, :http, gate), do: send(handler, {:fake_upstream_release_gate, gate})
  defp release_provider(handler, _carrier, gate), do: send(handler, {:fake_upstream_release_timeout, gate})

  defp open_prefix!(:http, port, setup, thread, _window, payload, mode, _output) do
    {conn, ref} = start_http!(port, setup, thread, payload, mode)
    on_exit(fn -> Mint.HTTP.close(conn) end)
    {conn, retained} = receive_http_prefix!(conn, ref, "")
    %{conn: conn, ref: ref, retained: retained}
  end

  defp open_prefix!(_carrier, port, setup, thread, window, payload, mode, _output) do
    {conn, ws, ref, _} = public_websocket_connect_with_request_headers!(port, setup, thread, @path, headers(mode, window))
    on_exit(fn -> Mint.HTTP.close(conn) end)
    {conn, ws} = public_websocket_send_text!(conn, ws, ref, CodexPooler.JSON.encode!(Map.put(payload, "type", "response.create")))

    {conn, ws, types, retained} =
      Enum.reduce(1..3, {conn, ws, [], nil}, fn _, {conn, ws, types, retained} ->
        {conn, ws, text} = public_websocket_receive_text!(conn, ws, ref)
        frame = CodexPooler.JSON.decode!(text)
        retained = if frame["type"] == "response.output_item.done", do: frame["item"], else: retained
        {conn, ws, types ++ [frame["type"]], retained}
      end)

    assert types == ["response.created", "response.output_item.added", "response.output_item.done"]
    assert is_map(retained)
    %{conn: conn, ws: ws, ref: ref, retained: retained}
  end

  defp close_transport!(transport, :http) do
    :ok = :inet.setopts(Mint.HTTP.get_socket(transport.conn), linger: {true, 0})
    Mint.HTTP.close(transport.conn)
  end

  defp close_transport!(transport, _carrier), do: Mint.HTTP.close(transport.conn)

  defp send_successor!(:http, port, setup, thread, _window, payload, mode) do
    {conn, ref} = start_http!(port, setup, thread, payload, mode)

    try do
      receive_http_terminal!(conn, ref, nil, "")
    after
      Mint.HTTP.close(conn)
    end
  end

  defp send_successor!(_carrier, port, setup, thread, window, payload, mode) do
    {conn, ws, ref, _} = public_websocket_connect_with_request_headers!(port, setup, thread, @path, headers(mode, window))

    try do
      {conn, ws} = public_websocket_send_text!(conn, ws, ref, CodexPooler.JSON.encode!(Map.put(payload, "type", "response.create")))
      receive_ws_terminal!(conn, ws, ref)
    after
      Mint.HTTP.close(conn)
    end
  end

  defp receive_ws_terminal!(conn, ws, ref) do
    {conn, ws, text} = public_websocket_receive_text!(conn, ws, ref)

    case CodexPooler.JSON.decode!(text)["type"] do
      "response.completed" -> :completed
      type when type in ["error", "response.failed"] -> :refused
      _progress -> receive_ws_terminal!(conn, ws, ref)
    end
  end

  defp start_http!(port, setup, thread, payload, mode) do
    {:ok, conn} = Mint.HTTP.connect(:http, "127.0.0.1", port, mode: :passive)
    headers = [{"authorization", setup.authorization}, {"content-type", "application/json"}, {"session-id", thread}, {"originator", "codex_cli_rs"}]
    headers = if mode == "lite", do: [{"x-openai-internal-codex-responses-lite", "true"} | headers], else: headers
    {:ok, conn, ref} = Mint.HTTP.request(conn, "POST", @path, headers, CodexPooler.JSON.encode!(payload))
    {conn, ref}
  end

  defp receive_http_prefix!(conn, ref, bytes) do
    {:ok, conn, responses} = Mint.HTTP.recv(conn, 0, @budget)

    bytes =
      Enum.reduce(responses, bytes, fn
        {:data, ^ref, data}, acc -> acc <> data
        _, acc -> acc
      end)

    if String.contains?(bytes, "\n\n") do
      [first | _rest] = String.split(bytes, "\n\n")
      [json] = for "data: " <> json <- String.split(first, "\n"), do: json
      assert %{"type" => "response.output_item.done", "item" => item} = CodexPooler.JSON.decode!(json)
      {conn, item}
    else
      receive_http_prefix!(conn, ref, bytes)
    end
  end

  defp receive_http_terminal!(conn, ref, status, bytes) do
    {:ok, conn, responses} = Mint.HTTP.recv(conn, 0, @budget)

    status =
      Enum.reduce(responses, status, fn
        {:status, ^ref, value}, _ -> value
        _, acc -> acc
      end)

    bytes =
      Enum.reduce(responses, bytes, fn
        {:data, ^ref, data}, acc -> acc <> data
        _, acc -> acc
      end)

    if Enum.any?(responses, &match?({:done, ^ref}, &1)), do: if(status == 200 and String.contains?(bytes, "response.completed"), do: :completed, else: :refused), else: receive_http_terminal!(conn, ref, status, bytes)
  end

  defp headers(mode, window), do: if(mode == "lite", do: [{"x-openai-internal-codex-responses-lite", "true"}, {"x-codex-window-id", window}], else: [{"x-codex-window-id", window}])
  defp event(data), do: "event: #{data["type"]}\ndata: " <> CodexPooler.JSON.encode!(data) <> "\n\n"
  defp completed, do: %{"type" => "response.completed", "response" => %{"id" => "resp_synthetic_lease_complete", "status" => "completed", "output" => [], "usage" => %{"input_tokens" => 3, "output_tokens" => 2, "total_tokens" => 5}}}
  defp payload(setup, thread, window), do: %{"model" => setup.model.exposed_model_id, "input" => native_text_input("synthetic lease"), "stream" => true, "store" => false, "client_metadata" => %{"x-codex-turn-metadata" => CodexPooler.JSON.encode!(%{"thread_id" => thread, "session_id" => thread, "turn_id" => "synthetic_lease", "request_kind" => "turn", "agent_name" => "/root"}), "x-codex-window-id" => window}}
  defp append_mailbox(payload, output), do: Map.update!(payload, "input", &(&1 ++ [Map.put(output, "content", nil), %{"type" => "agent_message", "author" => "/root/worker", "recipient" => "/root", "content" => [%{"type" => "input_text", "text" => "synthetic update"}]}]))
  defp db_now, do: Repo.query!("SELECT clock_timestamp()").rows |> hd() |> hd()
  defp requests(setup), do: Repo.all(from r in Request, where: r.pool_id == ^setup.pool.id, order_by: [asc: r.admitted_at])

  defp await_clock!(session, deadline), do: await_clock!(session, deadline, System.monotonic_time(:millisecond) + @budget)

  defp await_clock!(session, deadline, budget) do
    observation = MailboxLeaseLifecycleSupport.observe!(session)
    assert observation.session_deadline == observation.lease_deadline

    cond do
      DateTime.compare(observation.clock, deadline) == :gt ->
        observation

      System.monotonic_time(:millisecond) >= budget ->
        flunk("PostgreSQL did not cross the original ownership deadline")

      true ->
        receive do
        after
          10 -> await_clock!(session, deadline, budget)
        end
    end
  end

  defp await_settled!(setup, count), do: await_settled!(setup, count, System.monotonic_time(:millisecond) + @budget)

  defp await_settled!(setup, count, budget) do
    rows = requests(setup)

    cond do
      length(rows) == count and Enum.all?(rows, & &1.completed_at) ->
        rows

      System.monotonic_time(:millisecond) >= budget ->
        flunk("mailbox lifecycle requests did not settle")

      true ->
        receive do
        after
          10 -> await_settled!(setup, count, budget)
        end
    end
  end

  defp assert_settled_once!(rows) do
    ids = Enum.map(rows, & &1.id)
    attempts = Repo.all(from a in Attempt, where: a.request_id in ^ids)
    assert Enum.frequencies_by(attempts, & &1.request_id) == Map.new(ids, &{&1, 1})
    assert Enum.all?(attempts, &(&1.completed_at != nil and &1.replay_generation == 0))
    actual = Repo.all(from l in LedgerEntry, where: l.request_id in ^ids, select: {l.request_id, l.entry_kind}) |> Enum.frequencies()
    expected = for id <- ids, kind <- ["reservation", "release", "settlement"], into: %{}, do: {{id, kind}, 1}
    assert actual == expected
  end
end
