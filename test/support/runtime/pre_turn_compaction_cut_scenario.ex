defmodule CodexPoolerWeb.Runtime.PreTurnCompactionCutScenario do
  @moduledoc false

  # The released client's cut, resend and HTTPS fallback of an admitted native
  # compaction (findings#206 rows 206-310, 206-330, 206-332, 206-436, 206-455;
  # the rollout drain's cut, findings#270 row 270-352),
  # shared by `backend_codex_websocket_pre_turn_compaction_cut_test.exs` (one
  # node, owner forwarding on and off) and
  # `backend_codex_websocket_pre_turn_compaction_cut_peer_test.exs` (the
  # session's owner on a peer VM the module boots once, findings#206 row
  # 206-539). Each scenario runs inside the calling test process: its
  # `on_exit` callbacks and assertions belong to that test.

  import Ecto.Query
  import ExUnit.Assertions
  import ExUnit.Callbacks, only: [on_exit: 1, start_supervised!: 1]
  import Phoenix.ConnTest, only: [build_conn: 0, post: 3]
  import Plug.Conn, only: [put_req_header: 3]
  import CodexPoolerWeb.Runtime.BackendCodexTestSupport
  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport, only: [await_socket_connection_state!: 2, model_serving_scope: 0, set_model_serving_mode!: 3, with_info_log: 1]
  import CodexPoolerWeb.Runtime.BackendCodexWebsocketOwnerForwardingSupport, only: [enter_peer_owner_topology!: 0, start_shared_peer_window_owner!: 3, stop_retirable_owner!: 2]

  alias CodexPooler.Accounting.{Attempt, LedgerEntry, Request, RequestClientRetryLink}
  alias CodexPooler.Accounts.Scope
  alias CodexPooler.{ExecutionProofSupport, FakeUpstream, UnboxedFixture}
  alias CodexPooler.Gateway.Payloads.WebsocketTurnIdentity
  alias CodexPooler.Gateway.Persistence.{CodexSession, CodexTurn}
  alias CodexPooler.Gateway.Runtime.Finalization.Interruption
  alias CodexPooler.Gateway.Transports.Streaming.DeferredStreamRegistry
  alias CodexPooler.Gateway.Transports.Websocket.{ActivityDrain, ActivityRegistry, NativeCompactionAdmission, WebsocketOwnerSession}
  alias CodexPooler.Gateway.Transports.Websocket.{RolloutDrain, UpstreamWebsocketSession, WebsocketOwnerAdmissionControlV1}
  alias CodexPooler.Gateway.Transports.Websocket.WebsocketOwnerSession.Persistence, as: OwnerPersistence
  alias CodexPooler.Gateway.Websocket.DeliveryReceipt
  alias CodexPooler.Platform.{ExecutionIdentity, ExecutionRegistry, ExecutionTerminalProof, ExecutionTerminalProofs}
  alias CodexPooler.Repo
  alias CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport
  alias CodexPoolerWeb.Runtime.WebsocketCleanupFence
  alias Ecto.Adapters.SQL.Sandbox

  require Logger

  @endpoint CodexPoolerWeb.Endpoint

  @thread_id "019a0000-0000-7000-8000-00000000f001"
  @window_id "#{@thread_id}:0"
  @resumed_window_id "#{@thread_id}:1"
  @turn_id "019a0000-0000-7000-8000-00000000f002"
  @next_turn_id "019a0000-0000-7000-8000-00000000f006"
  @installation_id "00000000-0000-4000-8000-00000000f003"
  @context_window_id "00000000-0000-4000-8000-00000000f004"
  @resumed_context_window_id "00000000-0000-4000-8000-00000000f005"
  @anchor "resp_preturn_cut_anchor00000001"
  @reconnected_anchor "resp_preturn_cut_anchor00000002"
  @cut_response "resp_preturn_cut_compact_cut001"
  @resend_response "resp_preturn_cut_compact_resend"
  @final_response "resp_preturn_cut_final000000001"
  @lite_marker "ws_request_header_x_openai_internal_codex_responses_lite"
  @compact_endpoint "/backend-api/codex/responses/compact"
  @turn_endpoint "/backend-api/codex/responses"
  @detection_timeout_ms 15_000

  @standalone_turn_id "019a0000-0000-7000-8000-00000000f007"

  # Rows are `{endpoint, transport, status, last_error_code, chained?}` in
  # admission order; `compaction_charges` lists each compaction request's
  # charges in the same order, so a request billed twice shows as a 2.
  def expected(:unobserved_cut, topology), do: expected_unobserved(topology)
  def expected(cut, _topology) when cut in [:observed_cut, :observed_cut_exited], do: expected(:before_output)

  def expected(:observed_cut_claim_held, :direct_committed),
    do: %{expected(:before_output) | retries: [{409, "duplicate_turn"}, :served]}

  # The cut predecessor's settlement is held past the socket's take-over wait
  # (findings#206 row 206-580): the first retry takes the compaction over but
  # cannot see it settle within the bound (`inherited_turn_unsettled`), meets
  # the still-running predecessor and is refused; the released client's next
  # retry is served as the successor, one generation per request.
  def expected(:observed_cut_settlement_held, _topology),
    do: %{expected(:before_output) | retries: [{409, "duplicate_turn", :inherited_turn_unsettled}, :served]}

  # The rollout drain of the socket's node cut the compaction while the
  # provider was still generating it, before any output or after the Pooler
  # collected the compaction item; a native compaction reaches the client only
  # with its terminal. The client read the drain's `owner_drained` error and
  # nothing of the compaction, and resends it as full history on a new
  # connection: the first resend is the cut request's successor (findings#270
  # row 270-352). With the owner on the socket's node its own drain settles
  # the cut request `owner_drained`; with the owner on another node and its
  # proxy stopped before it finalizes the owner's answer, the resend recovers
  # it once the end of that exact executor is proven. The opposite ordering
  # preserves the original executor's `owner_drained` settlement below.
  def expected(cut, topology) when cut in [:drain_before_output, :drain_after_output] do
    code = if topology == :peer, do: "dead_execution_recovered", else: "owner_drained"
    [turn, _cut, successor, resume] = expected(:before_output).rows
    %{expected(:before_output) | rows: [turn, {@compact_endpoint, "websocket", "failed", code, false}, successor, resume]}
  end

  # The provider's stream died after the Pooler collected the compaction item
  # and before its terminal: the client read the Pooler's error and nothing of
  # the compaction, and its first resend is the cut request's successor
  # (findings#270 row 270-365). It used to be refused twice and bought again,
  # unchained, over HTTPS, and the client's session left the websocket.
  def expected(:provider_cut_after_output, _topology) do
    [turn, _cut, successor, resume] = expected(:before_output).rows
    %{expected(:before_output) | rows: [turn, {@compact_endpoint, "websocket", "failed", "upstream_stream_error", false}, successor, resume]}
  end

  # A cut request without a resend waits for the cleanup scan. Its terminal
  # proof makes it eligible at 120 seconds; no usage is charged on recovery.
  def expected(:drain_no_resend, :peer),
    do: %{
      retries: %{before_eligible: {"in_progress", nil, :reservation_held}, eligible: {"failed", "dead_execution_recovered", :reservation_released}},
      rows: [{@turn_endpoint, "websocket", "succeeded", nil, false}, {@compact_endpoint, "websocket", "failed", "dead_execution_recovered", false}],
      compaction_charges: [0],
      upstream_compactions: 1,
      live_rows: 0
    }

  def expected(cut, _topology), do: expected(cut)

  def expected(:drain_no_resend, :peer, :finalized) do
    %{expected(:drain_no_resend, :peer) | rows: [{@turn_endpoint, "websocket", "succeeded", nil, false}, {@compact_endpoint, "websocket", "failed", "owner_drained", false}], retries: %{before_eligible: {"failed", "owner_drained", :reservation_released}, eligible: {"failed", "owner_drained", :reservation_released}}}
  end

  def expected(cut, :peer, :finalized) when cut in [:drain_before_output, :drain_after_output], do: expected(cut, :forwarded)

  defp expected(:no_cut),
    do: %{
      retries: nil,
      rows: [{@turn_endpoint, "websocket", "succeeded", nil, false}, {@compact_endpoint, "websocket", "succeeded", nil, false}, {@turn_endpoint, "websocket", "succeeded", nil, false}],
      compaction_charges: [1],
      upstream_compactions: 1,
      live_rows: 0
    }

  # A cut before the provider produced anything: the first websocket resend is
  # served as the predecessor's successor.
  defp expected(:before_output),
    do: %{
      retries: [:served],
      rows: [
        {@turn_endpoint, "websocket", "succeeded", nil, false},
        {@compact_endpoint, "websocket", "failed", "client_disconnected", false},
        {@compact_endpoint, "websocket", "succeeded", nil, true},
        {@turn_endpoint, "websocket", "succeeded", nil, false}
      ],
      compaction_charges: [0, 1],
      upstream_compactions: 2,
      live_rows: 0
    }

  # The provider pushed the compaction item and the Pooler collected it, but
  # nothing reached the client; the first websocket resend is served as the
  # successor (findings#206 rows 206-330, 206-332). It used to be refused twice
  # and bought again, unchained, by the client's HTTPS fallback.
  defp expected(:after_output),
    do: %{
      retries: [:served],
      rows: [
        {@turn_endpoint, "websocket", "succeeded", nil, false},
        {@compact_endpoint, "websocket", "failed", "client_disconnected", false},
        {@compact_endpoint, "websocket", "succeeded", nil, true},
        {@turn_endpoint, "websocket", "succeeded", nil, false}
      ],
      compaction_charges: [0, 1],
      upstream_compactions: 2,
      live_rows: 0
    }

  # The reply was billed and written and the client never read it: the client
  # resends a compaction only when it did not complete it (its window has not
  # advanced), so the resend is served as the successor, one charge per
  # request, instead of two refusals and an unchained HTTPS purchase.
  defp expected(:after_completion),
    do: %{
      retries: [:served],
      rows: [
        {@turn_endpoint, "websocket", "succeeded", nil, false},
        {@compact_endpoint, "websocket", "succeeded", nil, false},
        {@compact_endpoint, "websocket", "succeeded", nil, true},
        {@turn_endpoint, "websocket", "succeeded", nil, false}
      ],
      compaction_charges: [1, 1],
      upstream_compactions: 2,
      live_rows: 0
    }

  # Without owner forwarding both websocket resends race the live predecessor
  # and are refused; the HTTPS fallback, which arrives after the Pooler settled
  # it, derives the predecessor's claim and is served as its successor, and
  # the turn finishes over HTTPS as the released client does.
  defp expected_unobserved(:direct),
    do: %{
      retries: [{409, "duplicate_turn"}, {409, "duplicate_turn"}, :http_served],
      rows: [
        {@turn_endpoint, "websocket", "succeeded", nil, false},
        {@compact_endpoint, "websocket", "failed", "client_disconnected", false},
        {@compact_endpoint, "http_compact_json", "succeeded", nil, true},
        {@turn_endpoint, "http_sse", "succeeded", nil, false}
      ],
      compaction_charges: [0, 1],
      upstream_compactions: 2,
      live_rows: 0
    }

  # With owner forwarding the first resend's socket takes the owner over and
  # the owner cuts the predecessor, so the second resend is its successor.
  defp expected_unobserved(topology) when topology in [:forwarded, :peer],
    do: %{
      retries: [{409, "duplicate_turn"}, :served],
      rows: [
        {@turn_endpoint, "websocket", "succeeded", nil, false},
        {@compact_endpoint, "websocket", "failed", "client_disconnected", false},
        {@compact_endpoint, "websocket", "succeeded", nil, true},
        {@turn_endpoint, "websocket", "succeeded", nil, false}
      ],
      compaction_charges: [0, 1],
      upstream_compactions: 2,
      live_rows: 0
    }

  # `opts`: `peer_node`, the module's shared peer for the `:peer` topology.
  def run_scenario(mode, shape, topology, cut, dispatch \\ :on_arrival, opts \\ []) do
    put_owner_forwarding!(topology in [:forwarded, :peer])
    release_ref = make_ref()
    ctx = %{mode: mode, shape: shape, topology: topology, before_drain: Keyword.get(opts, :before_drain), after_drain: Keyword.get(opts, :after_drain), proof_cleanup_probe: Keyword.get(opts, :proof_cleanup_probe), execution_order: Keyword.get(opts, :execution_order), execution_order_gate: make_ref()}

    upstream = start_upstream(FakeUpstream.strict_sequence(upstream_sequence(ctx, cut, topology, release_ref)))

    foreign_fixture =
      if ctx.proof_cleanup_probe do
        enter_peer_owner_topology!()
        fixture = gateway_setup(upstream, compact?: true)
        register_unboxed_pool_cleanup!(fixture)
        fixture
      end

    setup = topology_setup!(topology, upstream, opts)
    if mode == "lite", do: set_model_serving_mode!(model_serving_scope(), setup, "lite")
    ctx = ctx |> Map.put(:setup, setup) |> Map.put(:upstream, upstream) |> Map.put(:foreign_fixture, foreign_fixture)
    port = start_public_endpoint!()

    settling = if dispatch == :queued, do: hold_turn_task_after_settlement!()
    first = connect!(port, setup)
    first = ordinary_turn!(first, turn_frame(ctx))
    # The owner arms the admission only once the turn's response task is done:
    # a compaction queued behind that task is admitted at its dequeue.
    if dispatch == :on_arrival, do: await_armed!(topology, setup)
    install_cut_execution_order!(ctx, first)
    # The strict upstream expects this compaction anchored on the admitted
    # response: it is dispatched on its first send.
    first = send_frame!(first, anchored_compaction_frame(ctx))
    if dispatch == :queued, do: dispatch_queued_compaction!(ctx, settling)
    {retries, proof_scope} = cut_and_resend(cut, ctx, first, port, upstream, release_ref) |> cut_result()

    rows = await_settled!(setup.pool.id)
    compactions = Enum.filter(rows, &compaction_row?/1)

    measured = %{
      retries: retries,
      rows: Enum.map(rows, &{&1.endpoint, &1.transport, &1.status, &1.last_error_code, chained?(&1)}),
      compaction_charges: Enum.map(compactions, &charges/1),
      upstream_compactions: upstream |> FakeUpstream.requests() |> Enum.count(&compaction_request?/1),
      live_rows: Enum.count(rows, &(&1.status in ["accepted", "in_progress"]))
    }

    if ctx.execution_order in [:stopped, :finalized] do
      physical = FakeUpstream.physical_counts(upstream)
      assert physical.websocket_generation == length(rows)
      assert physical.http_generation == 0
      CodexPooler.TestDiagnostics.puts(fn -> "compaction_physical_generations " <> CodexPooler.JSON.encode!(%{order: ctx.execution_order, websocket_generations: physical.websocket_generation, request_count: length(rows), http_generations: 0, compaction_charges: measured.compaction_charges}) end)
    end

    CodexPooler.TestDiagnostics.puts(fn -> "206-330 #{mode} #{shape} #{topology} #{cut}: #{inspect(measured)}" end)
    assert :ok = FakeUpstream.verify!(upstream)
    if proof_scope, do: assert_foreign_fixture_after_owned_cleanup!(ctx, proof_scope)
    measured
  end

  defp cut_result({retries, %{owned_execution_ids: _} = proof_scope}), do: {retries, proof_scope}
  defp cut_result(retries), do: {retries, nil}

  # The provider has accepted an incremental mid-turn compaction before the
  # producing owner begins its drain. Release the terminal only after that
  # exact owner reports draining; draining the proxy is a different scenario.
  def run_drain_completion(topology, opts \\ []) do
    put_owner_forwarding!(true)
    release_ref = make_ref()
    mode = Keyword.get(opts, :mode, "full")
    form = Keyword.get(opts, :form, :incremental)
    shape = Keyword.get(opts, :shape, completion_shape(mode, form))
    ctx = %{mode: mode, shape: shape, topology: topology, reconnect_generation: Keyword.get(opts, :reconnect_generation, false)}
    sequence = completion_sequence(ctx, form, release_ref)
    # provenance: synthetic_adversarial; released-client-shaped incremental compact with a held terminal
    upstream = start_upstream(FakeUpstream.strict_sequence(sequence))

    on_exit(fn ->
      try do
        FakeUpstream.release_remaining_frames(upstream, release_ref)
      catch
        :exit, {:noproc, _call} -> :ok
      end
    end)

    setup = topology_setup!(topology, upstream, opts)
    if mode == "lite", do: set_model_serving_mode!(model_serving_scope(), setup, "lite")
    ctx = Map.put(ctx, :setup, setup)
    port = start_public_endpoint!()
    client = connect!(port, setup) |> ordinary_turn!(turn_frame(ctx))
    await_armed!(topology, setup)
    {ctx, client, stale_control} = if ctx.reconnect_generation, do: reconnect_with_issued_capability!(ctx, client), else: {ctx, client, nil}
    delivery_gate = if Keyword.get(opts, :hold_completion, false), do: install_compact_completion_gate!(client.cleanup_socket, topology)
    owner = owner_pid!(setup)
    observed_item = install_collection_item_observer!(owner)
    owner_monitor = Process.monitor(owner)
    initial = :sys.get_state(owner)
    expected_node = topology_node(topology, opts)
    assert node(owner) == expected_node
    assert node(initial.upstream_pid) == expected_node
    assert Repo.get!(CodexSession, initial.codex_session_id).owner_instance_id == initial.owner_instance_id
    assert Repo.get!(CodexSession, initial.codex_session_id).owner_lease_token == initial.owner_lease_token

    if topology == :peer do
      assert {:ok, %{owner_instance_id: owner_instance}} = WebsocketOwnerSession.owner_status(owner)
      assert owner_instance == initial.owner_instance_id
      assert_raise FunctionClauseError, fn -> WebsocketOwnerSession.local_drain_status(owner, %{at: System.monotonic_time(:millisecond) + 80, now_ms: fn -> System.monotonic_time(:millisecond) end}) end
      assert {:ok, %{owner_instance_id: ^owner_instance}} = WebsocketOwnerSession.owner_status(owner)
      CodexPooler.TestDiagnostics.puts(fn -> "owner_status_compatibility " <> CodexPooler.JSON.encode!(%{legacy_remote_atom: true, local_budget_api_refuses_remote_before_send: true, original_peer_owner_alive: true}) end)
    end

    frame = completion_frame(ctx, form)
    client = send_frame!(client, frame)
    await_barrier!(0, release_ref)

    release_collection_prefix!(upstream, release_ref, Keyword.get(opts, :before_output, false))

    admitted = :sys.get_state(owner)
    assert %{active_turn: %{collect?: true}} = admitted

    assert_completion_admission!(admitted, form)

    request_id = admitted.active_turn.admission_request_id
    attempt_id = admitted.active_turn.admission_attempt_id
    assert Repo.get!(Attempt, attempt_id).request_id == request_id
    upstream_pid = initial.upstream_pid
    assert_collection_observed!(Keyword.get(opts, :before_output, false), observed_item, upstream_pid, request_id, attempt_id)

    assert_new_capability!(admitted, stale_control)

    assert :ok = WebsocketOwnerSession.begin_drain(owner)
    if stale_control, do: assert({:error, :owner_drained} == WebsocketOwnerSession.admission_control(owner, stale_control))
    assert %{draining?: true, active_turn: %{admission_request_id: ^request_id}} = :sys.get_state(owner)
    :ok = FakeUpstream.release_remaining_frames(upstream, release_ref)
    await_barrier!(3, release_ref)

    drain_task = if delivery_gate, do: hold_compact_delivery!(delivery_gate, owner, request_id, attempt_id, opts)

    {client, frames} = receive_until_terminal(client, [])
    assert frames == ["response.output_item.done", "response.completed"]
    await_compaction_settled!(setup.pool.id, ["succeeded"])
    assert Repo.get!(Request, request_id).status == "succeeded"
    assert Repo.get!(Attempt, attempt_id).status == "succeeded"
    turn = Repo.one!(from(turn in CodexTurn, where: turn.request_id == ^request_id))
    assert turn.status == "succeeded"
    assert turn.final_attempt_id == attempt_id
    assert charges(Repo.get!(Request, request_id)) == 1

    if drain_task do
      finish_compact_delivery_drain!(drain_task, owner, attempt_id)
      {_conn, _websocket, after_terminal} = BackendCodexWebsocketSupport.receive_frames_until_close!(client.conn, client.websocket, client.ref)
      assert Enum.all?(after_terminal, &match?({:close, _, _}, &1)), "a data frame followed the compact terminal"
      CodexPooler.TestDiagnostics.puts(fn -> "compaction_delivery_causality " <> CodexPooler.JSON.encode!(%{topology: topology, request_id: request_id, attempt_id: attempt_id, held_after_db_settlement: true, receipt_absent_at_hold: true, owner_pending_at_hold: true, exact_delivered_receipt: not drain_task.failure, trailing_data_frames: 0}) end)
    else
      state = :sys.get_state(owner)
      assert state.draining?
      refute match?(%NativeCompactionAdmission{phase: :pending_final}, state.native_compaction_admission)
    end

    assert Enum.count(FakeUpstream.requests(upstream), &compaction_request?/1) == 1
    assert :ok = FakeUpstream.verify!(upstream)
    Mint.HTTP.close(client.conn)
    stop_retirable_owner!(owner, :normal)
    assert_receive {:DOWN, ^owner_monitor, :process, ^owner, _reason}, @detection_timeout_ms
    CodexPooler.TestDiagnostics.puts(fn -> "compaction_drain_causality " <> CodexPooler.JSON.encode!(%{topology: topology, serving_mode: mode, form: form, shape: shape, before_output: Keyword.get(opts, :before_output, false), barrier_ref: inspect(release_ref), owner_pid: inspect(owner), upstream_pid: inspect(initial.upstream_pid), owner_lease_matches_persisted: true, downstream_epoch: admitted.active_turn.downstream.epoch, session_id: initial.codex_session_id, request_id: request_id, attempt_id: attempt_id, owner_node: Atom.to_string(node(owner)), upstream_node: Atom.to_string(node(initial.upstream_pid)), terminal_released_after_owner_draining: true, request_status: "succeeded", attempt_status: "succeeded", turn_status: turn.status, client_events: frames, charges: 1, original_owner_exited: true}) end)
    :ok
  end

  defp topology_node(:peer, opts), do: Keyword.fetch!(opts, :peer_node)
  defp topology_node(_topology, _opts), do: node()

  defp completion_shape("full", :incremental), do: :mid_turn
  defp completion_shape(_mode, _form), do: :pre_turn
  defp completion_frame(ctx, :first), do: full_history_compaction_frame(ctx)
  defp completion_frame(ctx, :incremental), do: anchored_compaction_frame(ctx)
  defp release_collection_prefix!(_upstream, _release_ref, true), do: :ok

  defp release_collection_prefix!(upstream, release_ref, false) do
    for barrier <- [1, 2] do
      :ok = FakeUpstream.release_frame(upstream, release_ref)
      await_barrier!(barrier, release_ref)
    end
  end

  defp assert_completion_admission!(admitted, :first), do: assert(is_tuple(admitted.active_turn.first_compact_request_identity))
  defp assert_completion_admission!(admitted, :incremental), do: assert(%{active_turn: %{admission_phase: :compact}, native_compaction_admission: %NativeCompactionAdmission{phase: :consumed_compact}} = admitted)
  defp assert_new_capability!(_admitted, nil), do: :ok

  defp assert_new_capability!(admitted, control) do
    assert admitted.native_compaction_admission.binding.generation > control.capability.binding.generation
    refute admitted.native_compaction_admission.capability == control.capability
  end

  defp assert_collection_observed!(true, _ref, _upstream, _request, _attempt), do: :ok

  defp assert_collection_observed!(false, ref, upstream, request, attempt) do
    assert_receive {^ref, :collected_item, ^upstream, ^request, ^attempt}, @detection_timeout_ms
  end

  defp completion_sequence(%{reconnect_generation: true} = ctx, :incremental, release_ref) do
    [first | _] = upstream_sequence(ctx, :drain_completion, ctx.topology, release_ref)
    second = FakeUpstream.expect_request(method: "WEBSOCKET", websocket_connection_ordinal: 2, json: [valid: true, forbidden: ["previous_response_id"]], respond: completed_frames(@reconnected_anchor, [function_call()]))
    compact = FakeUpstream.expect_request(method: "WEBSOCKET", websocket_connection_ordinal: 2, json: [valid: true, equals: %{"previous_response_id" => @reconnected_anchor}], respond: FakeUpstream.barrier_websocket_frames(held_compaction_messages(), notify: self(), release_ref: release_ref))
    [first, second, compact]
  end

  defp completion_sequence(ctx, :incremental, release_ref), do: Enum.take(upstream_sequence(ctx, :drain_completion, ctx.topology, release_ref), 2)

  defp completion_sequence(ctx, :first, release_ref) do
    [turn | _] = upstream_sequence(ctx, :drain_completion, ctx.topology, release_ref)
    compact = FakeUpstream.expect_request(method: "WEBSOCKET", websocket_connection_ordinal: 1, json: [valid: true, equals: lite_marker_expectation(%{"type" => "response.create"}, ctx.mode), forbidden: ["previous_response_id"]], respond: FakeUpstream.barrier_websocket_frames(held_compaction_messages(), notify: self(), release_ref: release_ref))
    [turn, compact]
  end

  defp reconnect_with_issued_capability!(ctx, client) do
    owner = owner_pid!(ctx.setup)
    initial = :sys.get_state(owner)
    bound = initial.downstream
    binding = initial.native_compaction_admission.binding
    control_ref = make_ref()
    reserve = owner_control!(:reserve, bound, binding: binding, phase: :compact, control_ref: control_ref, now_ms: System.system_time(:millisecond))
    assert {:ok, capability} = WebsocketOwnerSession.admission_control(owner, reserve)
    cancel = owner_control!(:cancel, bound, capability: capability, disposition: :pre_accounting, now_ms: System.system_time(:millisecond))
    assert {:ok, %NativeCompactionAdmission{phase: :pending_compact}} = WebsocketOwnerSession.admission_control(owner, cancel)
    assert :ok == UpstreamWebsocketSession.invalidate_connection(initial.upstream_pid)
    payload = ctx |> frame([prompt("first"), function_call(), function_call_output()], @turn_id, @window_id) |> put_in(["client_metadata", "x-codex-turn-metadata"], turn_metadata(%{"request_kind" => "turn"})) |> CodexPooler.JSON.encode!()
    client = ordinary_turn!(client, payload)

    await!(
      fn ->
        match?(%{native_compaction_admission: %NativeCompactionAdmission{phase: :pending_compact, binding: %{generation: generation}}} when generation > binding.generation, :sys.get_state(owner))
      end,
      "the real reconnected upstream never armed a newer generation"
    )

    current = :sys.get_state(owner)
    assert current.upstream_pid == initial.upstream_pid
    assert current.native_compaction_admission.binding.lifecycle_id == binding.lifecycle_id
    assert current.native_compaction_admission.binding.generation > binding.generation
    assert {:error, :binding_mismatch} == WebsocketOwnerSession.admission_control(owner, reserve)
    assert :sys.get_state(owner).native_compaction_admission == current.native_compaction_admission
    fresh = owner_control!(:reserve, bound, binding: current.native_compaction_admission.binding, phase: :compact, control_ref: make_ref(), now_ms: System.system_time(:millisecond))
    assert {:ok, fresh_capability} = WebsocketOwnerSession.admission_control(owner, fresh)
    assert {:error, :capability_mismatch} == WebsocketOwnerSession.admission_control(owner, cancel)
    assert :sys.get_state(owner).native_compaction_admission.capability == fresh_capability
    fresh_cancel = owner_control!(:cancel, bound, capability: fresh_capability, disposition: :pre_accounting, now_ms: System.system_time(:millisecond))
    assert {:ok, %NativeCompactionAdmission{phase: :pending_compact}} = WebsocketOwnerSession.admission_control(owner, fresh_cancel)
    CodexPooler.TestDiagnostics.puts(fn -> "compaction_generation_causality " <> CodexPooler.JSON.encode!(%{topology: ctx.topology, session_id: initial.codex_session_id, old_generation: binding.generation, new_generation: current.native_compaction_admission.binding.generation, old_capability_issued_and_cancelled: true, stale_binding_refused_before_drain: true, stale_capability_refused_before_drain: true, fresh_capability_cancel_succeeded: true, real_connection_reopened: true, owner_node: Atom.to_string(node(owner))}) end)
    {Map.put(ctx, :anchor, @reconnected_anchor), client, cancel}
  end

  defp owner_control!(action, bound, attrs) do
    control = struct(WebsocketOwnerAdmissionControlV1, [version: 1, action: action, downstream: Map.take(bound, [:pid, :epoch, :correlation_id])] ++ attrs)
    assert {:ok, ^control} = WebsocketOwnerAdmissionControlV1.new(Map.from_struct(control))
    control
  end

  def run_drain_authority(topology, fault, opts \\ []) do
    put_owner_forwarding!(true)
    release_ref = make_ref()
    ctx = %{mode: "full", shape: :mid_turn, topology: topology}
    sequence = completion_sequence(ctx, :incremental, release_ref)
    sequence = if fault == :before_admission, do: Enum.take(sequence, 1), else: sequence
    upstream = start_upstream(FakeUpstream.strict_sequence(sequence))

    on_exit(fn ->
      try do
        FakeUpstream.release_remaining_frames(upstream, release_ref)
      catch
        :exit, _ -> :ok
      end
    end)

    setup = topology_setup!(topology, upstream, opts)
    ctx = Map.put(ctx, :setup, setup)
    port = start_public_endpoint!()
    client = connect!(port, setup) |> ordinary_turn!(turn_frame(ctx))
    await_armed!(topology, setup)
    owner = owner_pid!(setup)
    observed_item = install_collection_item_observer!(owner)
    owner_monitor = Process.monitor(owner)
    initial = :sys.get_state(owner)
    expected_node = topology_node(topology, opts)
    assert node(owner) == expected_node
    assert node(initial.upstream_pid) == expected_node
    persisted = Repo.get!(CodexSession, initial.codex_session_id)
    assert persisted.owner_lease_token == initial.owner_lease_token
    assert persisted.owner_instance_id == initial.owner_instance_id

    if fault == :before_admission, do: assert(:ok == WebsocketOwnerSession.begin_drain(owner))
    result_gate = if fault in [:cancel_queued, :result_before_cancel], do: install_real_collection_result_gate!(owner)
    client = send_frame!(client, anchored_compaction_frame(ctx))

    if fault == :before_admission do
      {client, terminal} = receive_frame!(client)
      assert terminal["type"] == "error"
      assert terminal["error"]["code"] == "owner_unavailable"
      refute Enum.any?(pool_requests(setup.pool.id), &compaction_row?/1)
      assert FakeUpstream.count(upstream) == 1
      Mint.HTTP.close(client.conn)
      stop_retirable_owner!(owner, :normal)
      assert_receive {:DOWN, ^owner_monitor, :process, ^owner, _}, @detection_timeout_ms
      assert :ok == FakeUpstream.verify!(upstream)
      CodexPooler.TestDiagnostics.puts(fn -> "compaction_authority_causality " <> CodexPooler.JSON.encode!(%{fault: fault, topology: topology, owner_node: Atom.to_string(node(owner)), new_dispatches: 0, new_attempts: 0, client_terminal: "owner_drained"}) end)
    else
      run_admitted_authority!(fault, %{ctx: ctx, client: client, owner: owner, owner_monitor: owner_monitor, initial: initial, upstream: upstream, release_ref: release_ref, result_gate: result_gate, observed_item: observed_item})
    end

    :ok
  end

  defp run_admitted_authority!(fault, %{ctx: ctx, client: client, owner: owner, owner_monitor: owner_monitor, initial: initial, upstream: upstream, release_ref: release_ref, result_gate: result_gate, observed_item: observed_item}) do
    provider_handler = await_barrier!(0, release_ref)

    for barrier <- [1, 2] do
      :ok = FakeUpstream.release_frame(upstream, release_ref)
      await_barrier!(barrier, release_ref)
    end

    admitted = :sys.get_state(owner)
    assert %{active_turn: %{admission_phase: :compact, collect?: true}, native_compaction_admission: %NativeCompactionAdmission{phase: :consumed_compact}} = admitted
    request_id = admitted.active_turn.admission_request_id
    attempt_id = admitted.active_turn.admission_attempt_id
    attempt = Repo.get!(Attempt, attempt_id)
    assert attempt.request_id == request_id
    upstream_pid = initial.upstream_pid
    assert_receive {^observed_item, :collected_item, ^upstream_pid, ^request_id, ^attempt_id}, @detection_timeout_ms
    scenario = %{ctx: ctx, client: client, owner: owner, initial: initial, admitted: admitted, upstream: upstream, provider_handler: provider_handler, release_ref: release_ref, request_id: request_id, attempt_id: attempt_id, result_gate: result_gate}
    apply_collection_fault!(fault, scenario)

    unless fault in [:upstream_closed, :stale_lease] do
      :ok = FakeUpstream.release_remaining_frames(upstream, release_ref)
      await_barrier!(3, release_ref)
    end

    if result_gate, do: resolve_collection_result_order!(fault, scenario)
    finish_collection_fault!(fault, scenario)
    if fault == :stale_lease, do: release_held_compaction!(upstream, release_ref, 3)
    stop_retirable_owner!(owner, :normal)
    assert_receive {:DOWN, ^owner_monitor, :process, ^owner, _}, @detection_timeout_ms
    assert :ok == FakeUpstream.verify!(upstream)
  end

  defp apply_collection_fault!(:generation, s) do
    :sys.replace_state(s.owner, fn state -> put_in(state.native_compaction_admission.binding.generation, state.native_compaction_admission.binding.generation + 1) end)
    assert :ok == WebsocketOwnerSession.begin_drain(s.owner)
  end

  defp apply_collection_fault!(:capability, s) do
    bound = s.admitted.active_turn.downstream
    control = struct(WebsocketOwnerAdmissionControlV1, version: 1, action: :clear, downstream: Map.take(bound, [:pid, :epoch, :correlation_id]))
    assert {:ok, ^control} = WebsocketOwnerAdmissionControlV1.new(Map.from_struct(control))
    assert {:ok, nil} = WebsocketOwnerSession.admission_control(s.owner, control)
    assert :ok == WebsocketOwnerSession.begin_drain(s.owner)
  end

  defp apply_collection_fault!(:stale_lease, s) do
    Repo.update_all(from(session in CodexSession, where: session.id == ^s.initial.codex_session_id), set: [owner_lease_token: Ecto.UUID.generate()])
    send(s.owner, :renew_owner_lease)
  end

  defp apply_collection_fault!(:upstream_closed, s) do
    assert :ok == WebsocketOwnerSession.begin_drain(s.owner)
    monitor = Process.monitor(s.provider_handler)
    Process.exit(s.provider_handler, :kill)
    assert_receive {:DOWN, ^monitor, :process, _, :killed}, @detection_timeout_ms
    assert :ok == FakeUpstream.release_remaining_frames(s.upstream, s.release_ref)
    assert :ok == FakeUpstream.retire_frame_barriers(s.upstream, s.release_ref, s.provider_handler)
  end

  defp apply_collection_fault!(:key_revoked, s) do
    scope = s.ctx.setup.api_key.created_by_user_id |> then(&Repo.get!(CodexPooler.Accounts.User, &1)) |> Scope.for_user(["instance_owner"])
    assert {:ok, paused_key} = CodexPooler.Access.pause_api_key(scope, s.ctx.setup.api_key.id)
    assert paused_key.runtime_revocation_epoch > s.ctx.setup.api_key.runtime_revocation_epoch
    await_socket_connection_state!(s.client.cleanup_socket, fn state -> Map.get(state, :api_key_revoked?, false) end)
    assert :ok == WebsocketOwnerSession.begin_drain(s.owner)
    _client = send_frame!(s.client, resume_frame(s.ctx, "cut"))
  end

  defp apply_collection_fault!(fault, s) when fault in [:cancel_queued, :result_before_cancel], do: assert(:ok == WebsocketOwnerSession.begin_drain(s.owner))

  defp install_real_collection_result_gate!(owner) do
    ref = make_ref()
    parent = self()

    :sys.replace_state(owner, fn state ->
      real = state.callbacks.upstream_sender

      sender = fn upstream, request, writer ->
        result = real.(upstream, request, writer)
        send(parent, {ref, :real_result, self(), match?({:ok, %{terminal: "response.completed"}}, result)})

        receive do
          {^ref, :release} -> result
        after
          @detection_timeout_ms -> result
        end
      end

      put_in(state.callbacks.upstream_sender, sender)
    end)

    on_exit(fn ->
      try do
        case :sys.get_state(owner).active_turn do
          %{task_pid: producer} -> send(producer, {ref, :release})
          _ -> :ok
        end

        :sys.resume(owner)
      catch
        :exit, _ -> :ok
      end
    end)

    ref
  end

  defp install_collection_item_observer!(owner) do
    ref = make_ref()
    parent = self()

    :sys.replace_state(owner, fn state ->
      real = state.callbacks.upstream_sender

      sender = fn upstream, request, writer ->
        observer = collection_item_observer(parent, ref, upstream, request)
        real.(upstream, %{request | frame_observer: observer}, writer)
      end

      put_in(state.callbacks.upstream_sender, sender)
    end)

    ref
  end

  defp collection_item_observer(parent, ref, upstream, request) do
    fn text, decoded ->
      call_original_observer(request.frame_observer, text, decoded)
      if decoded["type"] == "response.output_item.done", do: send(parent, {ref, :collected_item, upstream, request.request_id, request.attempt_id})
      :ok
    end
  end

  defp call_original_observer(observer, text, decoded) when is_function(observer, 2), do: observer.(text, decoded)
  defp call_original_observer(observer, text, _decoded) when is_function(observer, 1), do: observer.(text)
  defp call_original_observer(_observer, _text, _decoded), do: :ok

  defp resolve_collection_result_order!(fault, s) do
    ref = s.result_gate
    assert_receive {^ref, :real_result, producer, true}, @detection_timeout_ms
    producer_monitor = Process.monitor(producer)
    bound = s.admitted.active_turn.downstream

    if fault == :cancel_queued do
      :ok = :sys.suspend(s.owner)
      cancellation = {:cancel_downstream, bound.pid, bound.epoch, bound.correlation_id, bound.owner_turn_id, :owner_drained}
      request = :gen_server.send_request(s.owner, cancellation)
      assert :sys.get_state(s.owner).active_turn.task_pid == producer
      send(producer, {ref, :release})
      assert_receive {:DOWN, ^producer_monitor, :process, ^producer, :normal}, @detection_timeout_ms
      :ok = :sys.resume(s.owner)
      assert {:reply, :ok} = :gen_server.wait_response(request, @detection_timeout_ms)
    else
      send(producer, {ref, :release})
      assert_receive {:DOWN, ^producer_monitor, :process, ^producer, :normal}, @detection_timeout_ms
      await!(fn -> :sys.get_state(s.owner).active_turn == nil end, "real collection result was not consumed")
      assert {:error, :stale_downstream} == WebsocketOwnerSession.cancel_downstream(s.owner, bound, :owner_drained)
    end
  end

  defp finish_collection_fault!(fault, s) do
    if fault == :key_revoked do
      {_conn, _websocket, wire_frames} = BackendCodexWebsocketSupport.receive_frames_until_close!(s.client.conn, s.client.websocket, s.client.ref)
      events = for {:text, text} <- wire_frames, do: CodexPooler.JSON.decode!(text)["type"]
      assert events == ["response.output_item.done", "response.completed"]
    else
      {client, events} = receive_until_terminal(s.client, [])
      if fault == :result_before_cancel, do: assert(events == ["response.output_item.done", "response.completed"]), else: assert(events == ["error"])
      Mint.HTTP.close(client.conn)
    end

    expected = if fault in [:key_revoked, :result_before_cancel], do: "succeeded", else: "failed"
    await_compaction_settled!(s.ctx.setup.pool.id, [expected])
    request = Repo.get!(Request, s.request_id)
    attempt = Repo.get!(Attempt, s.attempt_id)
    assert request.status == expected
    assert attempt.status == expected
    turn = Repo.one!(from(turn in CodexTurn, where: turn.request_id == ^request.id))
    assert turn.final_attempt_id == attempt.id
    refute turn.status == "in_progress"
    await!(fn -> is_map(Repo.get!(Attempt, s.attempt_id).response_metadata["downstream_delivery"]) end, "the exact authority-fenced attempt has no delivery receipt")
    receipt = Repo.get!(Attempt, s.attempt_id).response_metadata["downstream_delivery"]
    assert receipt["outcome"] == "delivered"
    assert receipt["terminal_class"] == if(expected == "succeeded", do: "response.completed", else: "error")
    ledger = Repo.all(from(entry in LedgerEntry, where: entry.request_id == ^request.id, select: entry.entry_kind))
    assert Enum.count(ledger, &(&1 == "reservation")) == 1
    assert Enum.count(ledger, &(&1 == "release")) == 1
    assert Repo.aggregate(from(a in Attempt, where: a.request_id == ^request.id), :count) == 1
    assert Enum.count(pool_requests(s.ctx.setup.pool.id), &compaction_row?/1) == 1
    assert length(pool_requests(s.ctx.setup.pool.id)) == 2
    assert Enum.count(FakeUpstream.requests(s.upstream), &compaction_request?/1) == 1
    assert charges(request) <= 1

    if fault in [:generation, :capability] do
      assert is_map(:sys.get_state(s.owner))
      assert :sys.get_state(s.owner).native_compaction_admission == nil
    end

    CodexPooler.TestDiagnostics.puts(fn -> "compaction_authority_causality " <> CodexPooler.JSON.encode!(%{fault: fault, topology: s.ctx.topology, owner_node: Atom.to_string(node(s.owner)), upstream_node: Atom.to_string(node(s.initial.upstream_pid)), request_id: request.id, attempt_id: attempt.id, execution_id: attempt.owner_execution_id, request_status: request.status, attempt_status: attempt.status, turn_status: turn.status, receipt: receipt, reservation_count: 1, release_count: 1, error_code: request.last_error_code, charges: charges(request), provider_compactions: 1, attempts: 1}) end)
  end

  defp install_compact_completion_gate!(socket, topology) do
    test = self()
    ref = make_ref()

    gate = fn ->
      monitor = Process.monitor(test)
      send(test, {:compact_completion_held, ref, self()})

      receive do
        {:release_compact_completion, ^ref} -> :ok
        {:DOWN, ^monitor, :process, ^test, _reason} -> :ok
      after
        @detection_timeout_ms -> flunk("compact completion handoff was not released")
      end

      Process.demonitor(monitor, [:flush])
    end

    options = if topology == :peer, do: [before_completion_handoff: fn _token, _watcher -> gate.() end], else: [before_local_completion_handoff: gate]

    :sys.replace_state(socket, fn {name, data} ->
      state = Map.put(data.connection.websock_state, :response_task_start_options, options)
      {name, %{data | connection: %{data.connection | websock_state: state}}}
    end)

    ref
  end

  defp hold_compact_delivery!(ref, owner, request_id, attempt_id, opts) do
    assert_receive {:compact_completion_held, ^ref, task}, @detection_timeout_ms
    on_exit(fn -> send(task, {:release_compact_completion, ref}) end)
    assert Repo.get!(Request, request_id).status == "succeeded"
    assert Repo.get!(Attempt, attempt_id).status == "succeeded"
    assert Repo.get!(Attempt, attempt_id).response_metadata["downstream_delivery"] == nil
    assert :sys.get_state(owner).active_turn == nil
    assert_compact_receipt_matrix!(:sys.get_state(owner).termination_cleanup_witness)
    assert {:ok, %{active_turn?: true}} = WebsocketOwnerSession.owner_status(owner)
    budget_waiter = assert_requested_owner_budget!(owner, opts)
    failure = Keyword.get(opts, :receipt_failure, false)
    receipt_outcome = Keyword.get(opts, :receipt_outcome)
    if failure and is_nil(receipt_outcome), do: assert_compact_receipt_read_bound!(:sys.get_state(owner).termination_cleanup_witness, owner)
    fault = if failure, do: install_compact_receipt_failure!(attempt_id, receipt_outcome)
    started = System.monotonic_time(:millisecond)
    coordinator = if Keyword.get(opts, :coordinator, false), do: start_compact_rollout!(owner)
    call = compact_drain_call(coordinator, owner)
    drain = Task.async(call)
    if coordinator, do: await!(fn -> is_map(:sys.get_state(coordinator).active_drain) end, "coordinator did not begin")
    activity_deadline = if coordinator, do: :sys.get_state(coordinator).deadline_ms
    repeated = Task.async(call)
    await!(fn -> is_map(:sys.get_state(owner).drain_settlement) end, "owner did not retain the pending delivery")
    deadline = :sys.get_state(owner).drain_settlement.deadline_ms
    owner_now = if node(owner) == node(), do: System.monotonic_time(:millisecond), else: :erpc.call(node(owner), System, :monotonic_time, [:millisecond])
    local_deadline = System.monotonic_time(:millisecond) + deadline - owner_now

    if coordinator do
      assert :sys.get_state(coordinator).deadline_ms == activity_deadline
      assert length(:sys.get_state(coordinator).active_drain.waiters) == 2
    else
      assert_compact_drain_waiters!(owner, budget_waiter)
    end

    assert :sys.get_state(owner).drain_settlement.deadline_ms == deadline
    send(task, {:release_compact_completion, ref})
    %{task: drain, repeated: repeated, started: started, deadline: local_deadline, failure: failure, fault: fault, coordinator: coordinator, receipt_outcome: receipt_outcome, budget_waiter: budget_waiter}
  end

  defp compact_drain_call(nil, owner), do: fn -> WebsocketOwnerSession.drain_owner(owner) end
  defp compact_drain_call(coordinator, _owner), do: fn -> RolloutDrain.start_drain(name: coordinator, timeout_ms: 11_000) end

  defp assert_compact_drain_waiters!(owner, budget_waiter) do
    expected_waiters = if is_reference(budget_waiter), do: 3, else: 2
    await!(fn -> length(:sys.get_state(owner).drain_settlement.waiters) == expected_waiters end, "second drain caller did not join")
  end

  defp start_compact_rollout!(owner) do
    assert node(owner) == node()
    assert Registry.select(WebsocketOwnerSession.Registry, [{{:"$1", :"$2", :_}, [], [:"$2"]}]) == [owner]
    suffix = System.unique_integer([:positive])
    activities = :"compact_drain_activities_#{suffix}"
    streams = :"compact_drain_streams_#{suffix}"
    name = :"compact_rollout_#{suffix}"
    start_supervised!({ActivityRegistry, name: activities})
    start_supervised!({DeferredStreamRegistry, name: streams})
    start_supervised!({RolloutDrain, name: name, activity_registry: activities, stream_registry: streams})
    name
  end

  defp finish_compact_delivery_drain!(%{failure: false} = drain, _owner, attempt_id) do
    await!(fn -> match?(%{"outcome" => "delivered", "terminal_class" => "response.completed"}, Repo.get!(Attempt, attempt_id).response_metadata["downstream_delivery"]) end, "exact compact receipt never committed")
    assert {:ok, :settled} = Task.await(drain.task, @detection_timeout_ms)
    assert {:ok, :settled} = Task.await(drain.repeated, @detection_timeout_ms)
    if is_reference(drain.budget_waiter), do: assert({:reply, {:ok, :settled}} = :gen_server.wait_response(drain.budget_waiter, @detection_timeout_ms))
  end

  defp finish_compact_delivery_drain!(%{receipt_outcome: outcome} = drain, _owner, attempt_id) when not is_nil(outcome) do
    assert %{result: :ok, owners_drained: 1, turns_aborted: 1, turns_completed: 0} = Task.await(drain.task, @detection_timeout_ms)
    assert %{turns_aborted: 1, turns_completed: 0} = Task.await(drain.repeated, @detection_timeout_ms)
    receipt = Repo.get!(Attempt, attempt_id).response_metadata["downstream_delivery"]
    if outcome == :terminal_error, do: assert(receipt["terminal_class"] == "response.incomplete"), else: assert(receipt["outcome"] == Atom.to_string(outcome))
    assert System.monotonic_time(:millisecond) < drain.deadline
    remove_compact_receipt_failure!(drain.fault)
    CodexPooler.TestDiagnostics.puts(fn -> "compaction_non_success_retirement " <> CodexPooler.JSON.encode!(%{attempt_id: attempt_id, injected_receipt: outcome, turns_aborted: 1, turns_completed: 0, settled_before_deadline: true}) end)
  end

  defp finish_compact_delivery_drain!(%{failure: true} = drain, owner, attempt_id) do
    first = Task.await(drain.task, @detection_timeout_ms)
    second = Task.await(drain.repeated, @detection_timeout_ms)

    if drain.coordinator do
      assert %{result: :ok, owners_drained: 1, turns_aborted: 1, turns_completed: 0} = first
      assert Map.drop(first, [:already_draining?]) == Map.drop(second, [:already_draining?])
    else
      assert first == :ok
      assert second == :ok
    end

    ended = System.monotonic_time(:millisecond)
    assert ended >= drain.deadline
    assert ended - drain.deadline < 1_000
    assert Repo.get!(Attempt, attempt_id).response_metadata["downstream_delivery"] == nil
    assert Repo.get!(Attempt, attempt_id).status == "succeeded"
    remove_compact_receipt_failure!(drain.fault)
    assert :ok = DeliveryReceipt.persist(attempt_id, DeliveryReceipt.build(%{outcome: "delivered", terminal_class: "response.completed", highest_frame_class: "terminal", frames_after_visible: 2}))
    refute if(node(owner) == node(), do: Process.alive?(owner), else: :erpc.call(node(owner), Process, :alive?, [owner]))
    CodexPooler.TestDiagnostics.puts(fn -> "compaction_deadline_causality " <> CodexPooler.JSON.encode!(%{attempt_id: attempt_id, elapsed_ms: ended - drain.started, cutoff_overshoot_ms: ended - drain.deadline, repeated_waiters: 2, coordinator: not is_nil(drain.coordinator), receipt_absent_at_cutoff: true, late_receipt_did_not_revive_owner: true}) end)
  end

  defp install_compact_receipt_failure!(attempt_id, outcome) do
    name = "compact_receipt_" <> String.replace(Ecto.UUID.generate(), "-", "")
    on_exit(fn -> remove_compact_receipt_failure!(name) end)
    # These are deliberate stored-receipt fault controls, not claims about the
    # actual socket's write outcome. The client still proves one terminal.
    action =
      case outcome do
        nil -> "RAISE EXCEPTION 'synthetic receipt persistence failure';"
        :terminal_error -> "NEW.response_metadata = jsonb_set(NEW.response_metadata, '{downstream_delivery,terminal_class}', '\"response.incomplete\"');"
        value when value in [:aborted, :skipped] -> "NEW.response_metadata = jsonb_set(NEW.response_metadata, '{downstream_delivery,outcome}', '\"#{value}\"');"
      end

    Repo.query!("CREATE FUNCTION #{name}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.id = '#{attempt_id}'::uuid AND NEW.response_metadata ? 'downstream_delivery' THEN #{action} END IF; RETURN NEW; END $$")
    Repo.query!("CREATE TRIGGER #{name} BEFORE UPDATE ON attempts FOR EACH ROW EXECUTE FUNCTION #{name}()")
    name
  end

  defp remove_compact_receipt_failure!(name) do
    Repo.query!("DROP TRIGGER IF EXISTS #{name} ON attempts")
    Repo.query!("DROP FUNCTION IF EXISTS #{name}()")
  end

  defp assert_compact_receipt_read_bound!(witness, owner) do
    parent = self()
    ref = make_ref()

    holder =
      Task.async(fn ->
        monitor = Process.monitor(parent)

        Repo.transaction(fn ->
          Repo.query!("LOCK TABLE codex_turns IN ACCESS EXCLUSIVE MODE")
          %{rows: [[backend]]} = Repo.query!("SELECT pg_backend_pid()")
          send(parent, {:compact_read_lock, ref, backend})

          receive do
            {:release_compact_read_lock, ^ref} -> :ok
            {:DOWN, ^monitor, :process, ^parent, _reason} -> :ok
          after
            @detection_timeout_ms -> Repo.rollback(:missing_lock_release)
          end
        end)
      end)

    holder_monitor = Process.monitor(holder.pid)
    on_exit(fn -> send(holder.pid, {:release_compact_read_lock, ref}) end)
    assert_receive {:compact_read_lock, ^ref, holder_backend}, @detection_timeout_ms

    reader =
      Task.async(fn ->
        Repo.checkout(
          fn ->
            %{rows: [[backend]]} = Repo.query!("SELECT pg_backend_pid()")
            send(parent, {:compact_read_backend, ref, backend})
            started = System.monotonic_time(:millisecond)
            result = Interruption.compact_delivery_status(witness, 100)
            {result, System.monotonic_time(:millisecond) - started}
          end,
          timeout: 100
        )
      end)

    reader_monitor = Process.monitor(reader.pid)
    assert_receive {:compact_read_backend, ^ref, reader_backend}, @detection_timeout_ms
    assert reader_backend != holder_backend
    await!(fn -> Repo.query!("SELECT $1 = ANY(pg_blocking_pids($2))", [holder_backend, reader_backend]).rows == [[true]] end, "receipt read never waited on its distinct lock holder")
    assert {:ok, %{active_turn?: true}} = WebsocketOwnerSession.owner_status(owner)
    assert {:pending, elapsed} = Task.await(reader, @detection_timeout_ms)
    assert elapsed < 1_000
    assert_receive {:DOWN, ^reader_monitor, :process, _, :normal}, @detection_timeout_ms

    await!(
      fn ->
        Repo.query!("SELECT pg_stat_clear_snapshot()")
        Repo.query!("SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE pid = $1)", [reader_backend]).rows == [[false]]
      end,
      "the timed-out receipt reader backend remained connected before the lock release"
    )

    send(holder.pid, {:release_compact_read_lock, ref})
    assert {:ok, :ok} = Task.await(holder, @detection_timeout_ms)
    assert_receive {:DOWN, ^holder_monitor, :process, _, :normal}, @detection_timeout_ms

    CodexPooler.TestDiagnostics.puts(fn -> "compaction_read_lock_bound " <> CodexPooler.JSON.encode!(%{attempt_id: witness.attempt_id, distinct_backends: true, blocking_relation_observed: true, query_timeout_ms: 100, observed_ms: elapsed, owner_responsive: true}) end)
  end

  defp assert_compact_receipt_matrix!(witness) do
    original = Repo.get!(Attempt, witness.attempt_id).response_metadata
    delivered = DeliveryReceipt.build(%{outcome: "delivered", terminal_class: "response.completed", highest_frame_class: "terminal", frames_after_visible: 2})

    cases = [
      {:delivered, delivered, :delivered},
      {:missing_terminal, Map.delete(delivered, "terminal_class"), :pending},
      {:malformed, %{delivered | "frames_after_visible" => "two"}, :pending},
      {:malformed_aborted, delivered |> Map.put("outcome", "aborted") |> Map.delete("terminal_class"), :pending},
      {:completed, %{delivered | "outcome" => "completed"}, :pending},
      {:unknown, %{delivered | "outcome" => "unknown"}, :pending},
      {:unknown_terminal, %{delivered | "terminal_class" => "unknown"}, :pending},
      {:write_failed, Map.put(delivered, "write_failure", "closed"), :pending},
      {:error_terminal, %{delivered | "terminal_class" => "response.incomplete"}, :terminal_error},
      {:contradictory_error_terminal, %{delivered | "terminal_class" => "response.failed"}, :pending},
      {:aborted, %{delivered | "outcome" => "aborted"}, :aborted},
      {:skipped, %{delivered | "outcome" => "skipped"}, :skipped}
    ]

    # Classification reads use the test detection budget. The separate
    # blocked-reader and owner-deadline scenarios retain their runtime bounds.
    try do
      assert :pending = Interruption.compact_delivery_status(witness, @detection_timeout_ms)

      for {name, receipt, expected} <- cases do
        assert :ok = DeliveryReceipt.persist(witness.attempt_id, receipt)
        assert Interruption.compact_delivery_status(witness, @detection_timeout_ms) == expected, "receipt classification #{name}"
      end

      assert :ok = DeliveryReceipt.persist(witness.attempt_id, delivered)

      for key <- [:session_id, :request_id, :attempt_id, :owner_lease_token] do
        assert :pending = Interruption.compact_delivery_status(Map.put(witness, key, Ecto.UUID.generate()), @detection_timeout_ms)
      end

      assert :pending = Interruption.compact_delivery_status(%{witness | replay_generation: 2}, @detection_timeout_ms)
      assert :pending = Interruption.compact_delivery_status(%{witness | downstream_epoch: witness.downstream_epoch + 1}, @detection_timeout_ms)

      assert {:error, :matrix_complete} =
               Repo.transaction(fn ->
                 Repo.update_all(from(a in Attempt, where: a.id == ^witness.attempt_id), set: [status: "in_progress"])
                 assert :pending = Interruption.compact_delivery_status(witness, @detection_timeout_ms)
                 Repo.rollback(:matrix_complete)
               end)
    after
      Repo.update_all(from(a in Attempt, where: a.id == ^witness.attempt_id), set: [response_metadata: original])
    end

    CodexPooler.TestDiagnostics.puts(fn -> "compaction_receipt_matrix " <> CodexPooler.JSON.encode!(%{request_id: witness.request_id, attempt_id: witness.attempt_id, receipt_cases: length(cases), missing: true, foreign_binding_cases: 6, nonterminal_database: true}) end)
  end

  # The peer shares the committed database, so its fixture is committed: the
  # sandbox switches to auto mode before anything is written.
  defp topology_setup!(:peer, upstream, opts) do
    enter_peer_owner_topology!()
    setup = gateway_setup(upstream, compact?: true)
    Map.put(setup, :peer_owner, start_shared_peer_window_owner!(setup, @window_id, Keyword.fetch!(opts, :peer_node)))
  end

  # Owner forwarding off on committed rows, like the peer topology's, so a held
  # cleanup transaction holds its own connection only.
  defp topology_setup!(:direct_committed, upstream, _opts) do
    enter_peer_owner_topology!()
    setup = gateway_setup(upstream, compact?: true)
    register_unboxed_pool_cleanup!(setup)
    setup
  end

  defp topology_setup!(_topology, upstream, opts) do
    if Keyword.get(opts, :receipt_failure, false) or Keyword.has_key?(opts, :owner_budget) do
      enter_peer_owner_topology!()
      setup = gateway_setup(upstream, compact?: true)
      register_unboxed_pool_cleanup!(setup)
      setup
    else
      gateway_setup(upstream, compact?: true)
    end
  end

  defp assert_requested_owner_budget!(owner, opts) do
    case Keyword.get(opts, :owner_budget) do
      nil -> nil
      mode -> assert_owner_reader_budget!(owner, mode)
    end
  end

  defp assert_owner_reader_budget!(owner, mode) do
    assert node(owner) == node()
    witness = :sys.get_state(owner).termination_cleanup_witness
    assert :sys.get_state(owner).compact_delivery_read == nil
    parent = self()
    ref = make_ref()
    initial = System.monotonic_time(:millisecond)
    clock_state = start_supervised!({Agent, fn -> %{now: initial, gate?: true} end})
    clock = fn -> owner_reader_clock(clock_state, owner, parent, ref) end
    deadline = %{at: initial + 80, now_ms: clock}
    original = Repo.get!(Attempt, witness.attempt_id).response_metadata
    on_exit(fn -> :erlang.trace_pattern({OwnerPersistence, :compact_delivery_status, 2}, false, [:local]) end)
    lock = if mode in [:sub100, :delayed_start], do: hold_owner_reader_table!(ref)

    if mode in [:delayed_result, :phase_transition], do: DeliveryReceipt.persist(witness.attempt_id, DeliveryReceipt.build(%{outcome: "delivered", terminal_class: "response.completed", highest_frame_class: "terminal", frames_after_visible: 2}))
    assert {:ok, %{active_turn?: true}} = WebsocketOwnerSession.local_drain_status(owner, deadline)
    assert_receive {:owner_reader_clock, ^ref, reader}, @detection_timeout_ms
    assert :sys.get_state(owner).compact_delivery_read.task.pid == reader
    monitor = Process.monitor(reader)
    on_exit(fn -> send(reader, {:release_owner_reader_clock, ref}) end)
    Code.ensure_loaded!(OwnerPersistence)
    assert :erlang.trace_pattern({OwnerPersistence, :compact_delivery_status, 2}, true, [:local]) == 1
    :erlang.trace(reader, true, [:call, {:tracer, self()}])
    assert {:ok, %{active_turn?: true}} = WebsocketOwnerSession.local_drain_status(owner, %{deadline | at: deadline.at + 1_000})
    assert :sys.get_state(owner).compact_activity_deadline.at == deadline.at
    assert {:ok, %{active_turn?: true}} = WebsocketOwnerSession.owner_status(owner)

    result = run_owner_reader_order!(mode, %{owner: owner, reader: reader, monitor: monitor, clock: clock_state, deadline: deadline, ref: ref, lock: lock, witness: witness, original_metadata: original})
    :erlang.trace_pattern({OwnerPersistence, :compact_delivery_status, 2}, false, [:local])
    Repo.update_all(from(a in Attempt, where: a.id == ^witness.attempt_id), set: [response_metadata: original])
    CodexPooler.TestDiagnostics.puts(fn -> "owner_reader_budget_causality " <> CodexPooler.JSON.encode!(%{mode: mode, attempt_id: witness.attempt_id, actual_owner_task: true, activity_budget_ms: 80, repeated_deadline_unchanged: true, legacy_atom_supported: true, pending_after_read: true}) end)
    if is_reference(result), do: result
  end

  defp owner_reader_clock(agent, owner, parent, ref) do
    if self() not in [owner, parent] do
      gate? = Agent.get_and_update(agent, fn state -> {state.gate?, %{state | gate?: false}} end)

      if gate? do
        monitor = Process.monitor(parent)
        send(parent, {:owner_reader_clock, ref, self()})

        receive do
          {:release_owner_reader_clock, ^ref} -> :ok
          {:DOWN, ^monitor, :process, ^parent, _reason} -> :ok
        after
          @detection_timeout_ms -> flunk("owner reader clock gate was not released")
        end

        Process.demonitor(monitor, [:flush])
      end
    end

    Agent.get(agent, & &1.now)
  end

  defp hold_owner_reader_table!(ref) do
    parent = self()

    holder =
      Task.async(fn ->
        monitor = Process.monitor(parent)

        Repo.transaction(fn ->
          Repo.query!("LOCK TABLE codex_turns IN ACCESS EXCLUSIVE MODE")
          %{rows: [[backend]]} = Repo.query!("SELECT pg_backend_pid()")
          send(parent, {:owner_reader_lock, ref, backend})

          receive do
            {:release_owner_reader_lock, ^ref} -> :ok
            {:DOWN, ^monitor, :process, ^parent, _reason} -> :ok
          after
            @detection_timeout_ms -> Repo.rollback(:missing_reader_lock_release)
          end
        end)
      end)

    monitor = Process.monitor(holder.pid)
    on_exit(fn -> send(holder.pid, {:release_owner_reader_lock, ref}) end)
    assert_receive {:owner_reader_lock, ^ref, backend}, @detection_timeout_ms
    %{task: holder, monitor: monitor, backend: backend, ref: ref}
  end

  defp run_owner_reader_order!(:sub100, s) do
    Agent.update(s.clock, &%{&1 | now: s.deadline.at - 40})
    send(s.reader, {:release_owner_reader_clock, s.ref})
    assert_receive {:trace, reader, :call, {OwnerPersistence, :compact_delivery_status, [_witness, timeout]}}, @detection_timeout_ms, "actual owner reader did not enter the persistence boundary"
    assert reader == s.reader
    assert timeout > 0 and timeout <= 40
    await!(fn -> Repo.query!("SELECT count(*) FROM pg_stat_activity WHERE $1 = ANY(pg_blocking_pids(pid))", [s.lock.backend]).rows == [[1]] end, "actual owner reader never blocked on its separate backend")
    assert {:ok, %{active_turn?: true}} = WebsocketOwnerSession.owner_status(s.owner)
    assert_receive {:DOWN, monitor, :process, reader, :normal}, @detection_timeout_ms
    assert {monitor, reader} == {s.monitor, s.reader}
    release_owner_reader_lock!(s.lock)
    await!(fn -> :sys.get_state(s.owner).compact_delivery_read == nil end, "reader did not retire")
    assert :sys.get_state(s.owner).compact_delivery.status == :pending
    CodexPooler.TestDiagnostics.puts(fn -> "owner_reader_query_bound " <> CodexPooler.JSON.encode!(%{mode: :sub100, actual_query_timeout_ms: timeout, residual_at_query_ms: 40, distinct_backend_block_observed: true}) end)
  end

  defp run_owner_reader_order!(:delayed_start, s) do
    Agent.update(s.clock, &%{&1 | now: s.deadline.at + 1})
    send(s.reader, {:release_owner_reader_clock, s.ref})
    assert_receive {:DOWN, monitor, :process, reader, :normal}, @detection_timeout_ms
    assert {monitor, reader} == {s.monitor, s.reader}
    trace_fence = :erlang.trace_delivered(:all)
    assert_receive {:trace_delivered, :all, ^trace_fence}, @detection_timeout_ms
    refute_received {:trace, ^reader, :call, {OwnerPersistence, :compact_delivery_status, _args}}
    release_owner_reader_lock!(s.lock)
    await!(fn -> :sys.get_state(s.owner).compact_delivery_read == nil end, "expired reader did not retire")
    assert :sys.get_state(s.owner).compact_delivery.status == :pending
    CodexPooler.TestDiagnostics.puts(fn -> "owner_reader_query_bound " <> CodexPooler.JSON.encode!(%{mode: :delayed_start, query_started: false, actual_owner_task_exited: true}) end)
  end

  defp run_owner_reader_order!(:delayed_result, s) do
    :ok = :sys.suspend(s.owner)

    on_exit(fn ->
      try do
        :sys.resume(s.owner)
      catch
        :exit, _ -> :ok
      end
    end)

    send(s.reader, {:release_owner_reader_clock, s.ref})
    assert_receive {:trace, reader, :call, {OwnerPersistence, :compact_delivery_status, [_witness, timeout]}}, @detection_timeout_ms, "actual owner reader did not enter the persistence boundary"
    assert reader == s.reader and timeout <= 80
    assert_receive {:DOWN, monitor, :process, reader, :normal}, @detection_timeout_ms
    assert {monitor, reader} == {s.monitor, s.reader}
    Agent.update(s.clock, &%{&1 | now: s.deadline.at + 1})
    :ok = :sys.resume(s.owner)
    await!(fn -> :sys.get_state(s.owner).compact_delivery_read == nil end, "stale result was not consumed")
    assert :sys.get_state(s.owner).compact_delivery.status == :pending
    CodexPooler.TestDiagnostics.puts(fn -> "owner_reader_query_bound " <> CodexPooler.JSON.encode!(%{mode: :delayed_result, actual_query_timeout_ms: timeout, stale_completed_result_ignored: true}) end)
  end

  defp run_owner_reader_order!(:phase_transition, s) do
    :ok = :sys.suspend(s.owner)

    on_exit(fn ->
      try do
        :sys.resume(s.owner)
      catch
        :exit, _ -> :ok
      end
    end)

    waiter = :gen_server.send_request(s.owner, :drain)
    assert :sys.get_state(s.owner).drain_settlement == nil
    send(s.reader, {:release_owner_reader_clock, s.ref})
    assert_receive {:trace, reader, :call, {OwnerPersistence, :compact_delivery_status, [_witness, timeout]}}, @detection_timeout_ms, "actual owner phase reader did not enter persistence"
    assert reader == s.reader and timeout <= 80
    assert_receive {:DOWN, monitor, :process, reader, :normal}, @detection_timeout_ms
    assert {monitor, reader} == {s.monitor, s.reader}
    Repo.update_all(from(a in Attempt, where: a.id == ^s.witness.attempt_id), set: [response_metadata: s.original_metadata])
    :ok = :sys.resume(s.owner)

    await!(
      fn ->
        state = :sys.get_state(s.owner)
        is_map(state.drain_settlement) and (is_nil(state.compact_delivery_read) or state.compact_delivery_read.task.pid != s.reader)
      end,
      "old activity result did not leave the settlement phase"
    )

    assert :sys.get_state(s.owner).compact_delivery.status == :pending
    CodexPooler.TestDiagnostics.puts(fn -> "owner_reader_query_bound " <> CodexPooler.JSON.encode!(%{mode: :phase_transition, old_activity_result_ignored_during_settlement: true, actual_query_timeout_ms: timeout}) end)
    waiter
  end

  defp release_owner_reader_lock!(lock) do
    send(lock.task.pid, {:release_owner_reader_lock, lock.ref})
    assert {:ok, :ok} = Task.await(lock.task, @detection_timeout_ms)
    assert_receive {:DOWN, monitor, :process, holder, :normal}, @detection_timeout_ms
    assert {monitor, holder} == {lock.monitor, lock.task.pid}
  end

  defp cut_and_resend(:no_cut, ctx, client, _port, _upstream, _release_ref) do
    {client, frames} = receive_until_terminal(client, [])
    assert frames == ["response.output_item.done", "response.completed"]

    try do
      client |> send_frame!(resume_frame(ctx, "served")) |> ordinary_turn!()
    after
      Mint.HTTP.close(client.conn)
    end

    nil
  end

  defp cut_and_resend(cut, ctx, client, port, upstream, release_ref) when cut in [:drain_before_output, :drain_after_output] do
    await_barrier!(0, release_ref)

    # `response.created` and the compaction item reach the Pooler, which
    # collects a native compaction before it shows the client anything.
    if cut == :drain_after_output do
      for barrier <- [1, 2] do
        :ok = FakeUpstream.release_frame(upstream, release_ref)
        await_barrier!(barrier, release_ref)
      end
    end

    publisher = if ctx.topology == :peer, do: prepare_cut_execution_proof!(ctx)
    drains = drain_socket_node!(ctx, client)
    {client, frame} = receive_frame!(client)
    assert %{"type" => "error", "status" => 503, "error" => %{"code" => "owner_drained"}} = frame
    await_cut_finalization!(ctx)
    # The released client drops the connection its compaction failed on.
    Mint.HTTP.close(client.conn)
    Enum.each(drains, &Task.await(&1, @detection_timeout_ms))
    assert_proxy_drain_separation!(ctx, client.cleanup_socket, :after_proxy_drain)
    assert_cut_execution_order!(ctx, publisher)
    if is_function(ctx.after_drain, 0), do: ctx.after_drain.()
    if publisher, do: await_cut_execution_proof!(ctx, publisher)
    retries = released_client_retries!(ctx, port, fn -> :ok end)
    release_held_compaction!(upstream, release_ref, if(cut == :drain_after_output, do: 3, else: 1))
    retries
  end

  # The provider sends `response.created` and the compaction item, which the
  # Pooler collects, and its connection dies before the terminal. The client
  # reads the error the Pooler writes for the cut and retries on a new
  # connection.
  defp cut_and_resend(:provider_cut_after_output, ctx, client, port, _upstream, _release_ref) do
    {client, frames} = receive_until_terminal(client, [])
    assert frames == ["error"]
    Mint.HTTP.close(client.conn)
    await_compaction_settled!(ctx.setup.pool.id, ["failed"])
    released_client_retries!(ctx, port, fn -> :ok end)
  end

  defp cut_and_resend(:drain_no_resend, ctx, client, _port, upstream, release_ref) do
    await_barrier!(0, release_ref)
    publisher = prepare_cut_execution_proof!(ctx)
    drains = drain_socket_node!(ctx, client)
    {client, frame} = receive_frame!(client)
    assert %{"type" => "error", "status" => 503, "error" => %{"code" => "owner_drained"}} = frame
    await_cut_finalization!(ctx)
    Mint.HTTP.close(client.conn)
    Enum.each(drains, &Task.await(&1, @detection_timeout_ms))
    assert_proxy_drain_separation!(ctx, client.cleanup_socket, :after_proxy_drain)
    assert_cut_execution_order!(ctx, publisher)
    publisher = await_cut_execution_proof!(ctx, publisher)
    attempt = publisher.attempt

    {:ok, _summary} = CodexPooler.Accounting.recover_dead_execution_attempts(DateTime.add(attempt.started_at, 119, :second))
    before_eligible = cut_request_state(attempt.request_id)
    {:ok, _summary} = CodexPooler.Accounting.recover_dead_execution_attempts(DateTime.add(attempt.started_at, 121, :second))
    eligible = cut_request_state(attempt.request_id)
    release_held_compaction!(upstream, release_ref, 1)
    result = %{before_eligible: before_eligible, eligible: eligible}

    if ctx.proof_cleanup_probe do
      assert_owned_proof_cleanup!(publisher)
      {result, publisher}
    else
      result
    end
  end

  defp cut_and_resend(:unobserved_cut, ctx, client, port, upstream, release_ref) do
    await_barrier!(0, release_ref)
    # The client gave up on the connection but the Pooler has not seen it close
    # yet: the predecessor is still live when both websocket resends arrive,
    # and the Pooler settles it before the HTTPS fallback.
    settle = fn ->
      Mint.HTTP.close(client.conn)
      await_compaction_settled!(ctx.setup.pool.id, ["succeeded", "failed"])
      release_held_compaction!(upstream, release_ref, 1)
    end

    retries = released_client_retries!(ctx, port, settle)
    if List.last(retries) == :served, do: settle.()
    retries
  end

  defp cut_and_resend(cut, ctx, client, port, upstream, release_ref) when cut in [:observed_cut, :observed_cut_exited, :observed_cut_settlement_held, :observed_cut_claim_held] do
    await_barrier!(0, release_ref)
    closing_socket = if cut == :observed_cut_settlement_held, do: attached_socket!(ctx)
    # The connection closes before the provider produced anything and the
    # Pooler sees it close; its session cleanup is held.
    hold = hold_session_cleanups!(if(cut == :observed_cut_exited, do: 2, else: 1), ctx.topology)
    Mint.HTTP.close(client.conn)
    assert_receive {^hold, :held, cleanup}, @detection_timeout_ms
    # The retries meet a predecessor that is still live.
    assert Enum.any?(pool_requests(ctx.setup.pool.id), &(&1.endpoint == @compact_endpoint and &1.status == "in_progress"))
    cleanups = [cleanup | if(cut == :observed_cut_exited, do: [drop_inheriting_connection!(ctx, port, hold)], else: [])]

    release = fn ->
      release_session_cleanups!(hold, cleanups)
      await_compaction_settled!(ctx.setup.pool.id, ["succeeded", "failed"])
    end

    retries = resend_after_observed_cut(cut, ctx, port, release, closing_socket)
    release_held_compaction!(upstream, release_ref, 1)
    retries
  end

  defp cut_and_resend(cut, ctx, client, port, upstream, release_ref) do
    await_barrier!(0, release_ref)

    next_barrier =
      case cut do
        :before_output ->
          1

        :after_output ->
          # `response.created` and the compaction item reach the Pooler, which
          # collects a native compaction before it shows the client anything.
          for barrier <- [1, 2] do
            :ok = FakeUpstream.release_frame(upstream, release_ref)
            await_barrier!(barrier, release_ref)
          end

          3

        :after_completion ->
          # The provider completes and the Pooler bills and writes the reply,
          # which the client never reads (the reply is lost with the connection).
          :ok = FakeUpstream.release_remaining_frames(upstream, release_ref)
          for barrier <- 1..3, do: await_barrier!(barrier, release_ref)
          await_compaction_settled!(ctx.setup.pool.id, ["succeeded"])
          nil
      end

    Mint.HTTP.close(client.conn)
    await_compaction_settled!(ctx.setup.pool.id, ["succeeded", "failed"])
    retries = released_client_retries!(ctx, port, fn -> :ok end)
    if next_barrier, do: release_held_compaction!(upstream, release_ref, next_barrier)
    retries
  end

  # The socket node's rollout drain past its deadline, as `RolloutDrain` runs
  # it: it cancels the compaction's response task, whose terminal has not gone
  # out, and with the session's owner on this node it drains the owner too.
  # The owner on another node is left running, as when only the socket's pod
  # is replaced. `ActivityDrain.drain/4` without a drain snapshot leaves the
  # application's registry open for later tests.
  defp drain_socket_node!(ctx, client) do
    state = await_socket_connection_state!(client.cleanup_socket, &(MapSet.size(Map.get(&1, :tasks, MapSet.new())) == 1))
    [task] = MapSet.to_list(state.tasks)
    entry = Enum.find(ActivityRegistry.activities(), &(&1.pid == task))
    refute Map.get(entry, :terminal_delivered?, false), "the compaction's terminal went out before the drain"
    assert_proxy_drain_separation!(ctx, client.cleanup_socket, :before_proxy_drain)

    if ctx.execution_order == :stopped do
      on_exit(fn ->
        try do
          :erlang.resume_process(task)
        rescue
          ArgumentError -> :ok
        end
      end)

      # The actual provider barrier proves admission and DB work finished.
      # Hold only the proxy awaiting its remote owner's answer, so the real
      # watcher wins the stop before this executor can finalize that answer.
      assert :erlang.suspend_process(task)
    end

    if is_function(ctx.before_drain, 2), do: ctx.before_drain.(state, entry)
    policy = %{now_ms: fn -> System.monotonic_time(:millisecond) end, schedule_wait: &schedule_drain_wait/3, cancel_wait: &cancel_drain_wait/2, owner_post_deadline_call_budget_ms: 5_000}
    task_drain = Task.async(fn -> ActivityDrain.drain(entry, System.monotonic_time(:millisecond) - 1, policy, ActivityRegistry) end)

    if ctx.topology == :forwarded do
      owner = owner_pid!(ctx.setup)
      [task_drain, Task.async(fn -> WebsocketOwnerSession.drain_owner(owner) end)]
    else
      [task_drain]
    end
  end

  defp install_cut_execution_order!(%{execution_order: order} = ctx, client) when order in [:stopped, :finalized] do
    parent = self()
    gate = ctx.execution_order_gate
    pool_id = ctx.setup.pool.id

    # Both orders observe the actual watcher after receipt COMMIT and before
    # the original executor ends. The finalized arm additionally lets the
    # original executor settle the owner's answer first.
    options = [
      before_cancelled_coordinator_termination: fn _token, task ->
        monitor = Process.monitor(parent)
        if order == :finalized, do: await_compaction_settled!(pool_id, ["failed"])
        [request] = Enum.filter(pool_requests(pool_id), &compaction_row?/1)
        attempt = Repo.get_by!(Attempt, request_id: request.id)
        receipt = attempt.response_metadata["downstream_delivery"]
        facts = %{original_actor_matches: attempt.owner_process_id == List.to_string(:erlang.pid_to_list(task)), original_alive: ExecutionIdentity.status(attempt) == :alive, proof_absent: not ExecutionTerminalProofs.terminal?(attempt), receipt: Map.take(receipt || %{}, ["outcome", "terminal_class"]), outside_transaction: not Repo.in_transaction?(), request_status: request.status, error_code: request.last_error_code}
        send(parent, {:compaction_cut_receipt_committed, gate, self(), facts})

        receive do
          {:release_compaction_cut, ^gate} -> :ok
          {:DOWN, ^monitor, :process, ^parent, _reason} -> :ok
        after
          @detection_timeout_ms -> flunk("compaction cut receipt observation was not released")
        end

        Process.demonitor(monitor, [:flush])
      end
    ]

    :sys.replace_state(client.cleanup_socket, fn {name, data} ->
      state = Map.put(data.connection.websock_state, :response_task_start_options, options)
      {name, %{data | connection: %{data.connection | websock_state: state}}}
    end)
  end

  defp install_cut_execution_order!(_ctx, _client), do: :ok

  defp await_cut_finalization!(%{execution_order: order, execution_order_gate: gate}) when order in [:stopped, :finalized] do
    assert_receive {:compaction_cut_receipt_committed, ^gate, watcher, facts}, @detection_timeout_ms

    try do
      assert facts.original_actor_matches and facts.original_alive and facts.proof_absent and facts.outside_transaction
      assert facts.receipt == %{"outcome" => "aborted", "terminal_class" => "error"}
      assert {facts.request_status, facts.error_code} == if(order == :stopped, do: {"in_progress", nil}, else: {"failed", "owner_drained"})
      CodexPooler.TestDiagnostics.puts(fn -> "compaction_receipt_before_executor_end " <> CodexPooler.JSON.encode!(Map.put(facts, :order, order)) end)
    after
      send(watcher, {:release_compaction_cut, gate})
    end
  end

  defp await_cut_finalization!(_ctx), do: :ok

  defp assert_cut_execution_order!(%{execution_order: order}, %{attempt: attempt}) when order in [:stopped, :finalized] do
    request = Repo.get!(Request, attempt.request_id)
    attempt = Repo.reload!(attempt)
    turn = Repo.get_by!(CodexTurn, request_id: request.id)
    receipt = attempt.response_metadata["downstream_delivery"]
    assert receipt["outcome"] == "aborted"
    assert receipt["terminal_class"] == "error"
    assert charges(request) == 0
    assert ExecutionIdentity.status(attempt) == :dead
    refute ExecutionTerminalProofs.terminal?(attempt)

    if order == :stopped do
      assert request.status == "in_progress"
      assert request.last_error_code == nil
      assert attempt.status == "in_progress"
    else
      assert {request.status, request.last_error_code, turn.status} == {"failed", "owner_drained", "interrupted"}
      assert request.usage_status == "usage_unknown"
      assert attempt.status == "failed"
    end

    CodexPooler.TestDiagnostics.puts(fn -> "compaction_executor_order " <> CodexPooler.JSON.encode!(%{order: order, request_id: request.id, attempt_id: attempt.id, execution_id: attempt.owner_execution_id, request_status: request.status, attempt_status: attempt.status, error_code: request.last_error_code, original_executor_dead: true, receipt_committed_before_proof: true, receipt_terminal: receipt["terminal_class"], proof_not_yet_published: true, charges: 0}) end)
  end

  defp assert_cut_execution_order!(_ctx, _publisher), do: :ok

  defp assert_proxy_drain_separation!(%{topology: :peer} = ctx, socket, phase) do
    owner = owner_pid!(ctx.setup)
    state = :sys.get_state(owner)
    refute state.draining?
    assert node(owner) == node(state.upstream_pid)
    refute node(owner) == node(socket)
    assert Repo.get!(CodexSession, state.codex_session_id).owner_instance_id == state.owner_instance_id
    CodexPooler.TestDiagnostics.puts(fn -> "compaction_proxy_drain " <> CodexPooler.JSON.encode!(%{phase: phase, session_id: state.codex_session_id, owner_node: Atom.to_string(node(owner)), upstream_node: Atom.to_string(node(state.upstream_pid)), proxy_node: Atom.to_string(node(socket)), owner_draining: false, owner_pid: inspect(owner), socket_pid: inspect(socket)}) end)
  end

  defp assert_proxy_drain_separation!(_ctx, _socket, _phase), do: :ok

  defp schedule_drain_wait(recipient, token, wait_ms), do: Process.send_after(recipient, {:rollout_drain_wait_elapsed, token}, wait_ms)

  defp cancel_drain_wait(timer, _token) do
    _remaining = Process.cancel_timer(timer)
    :ok
  end

  # Publish only this scenario's exact executor proof before a resend. Starting
  # the global publisher here also commits unrelated retained executions from
  # earlier tests. Capture ownership before the cut, and use the real registry
  # and proof writer after the executor ends; never delete a snapshot complement.
  defp prepare_cut_execution_proof!(ctx) do
    [cut] = Enum.filter(pool_requests(ctx.setup.pool.id), &compaction_row?/1)
    attempt = Repo.one!(from(attempt in Attempt, where: attempt.request_id == ^cut.id))
    owned_execution_ids = Repo.all(from(attempt in Attempt, join: request in Request, on: request.id == attempt.request_id, where: request.pool_id == ^ctx.setup.pool.id, select: attempt.owner_execution_id))
    assert attempt.owner_execution_id in owned_execution_ids
    cleanup = fn -> Repo.delete_all(from(proof in ExecutionTerminalProof, where: proof.execution_id in ^owned_execution_ids)) end
    UnboxedFixture.register_unboxed_cleanup!(cleanup)
    foreign = if ctx.proof_cleanup_probe == :before_publication, do: committed_foreign_proof!(ctx)
    %{attempt: attempt, cleanup: cleanup, owned_execution_ids: owned_execution_ids, foreign: foreign}
  end

  defp await_cut_execution_proof!(ctx, %{attempt: attempt} = proof_scope) do
    :ok = ExecutionProofSupport.publish_terminal!(attempt)
    foreign = if ctx.proof_cleanup_probe == :after_publication, do: committed_foreign_proof!(ctx), else: proof_scope.foreign
    # The socket node flushes this proof before exiting; the child-VM test in
    # execution_proof_shutdown_test.exs covers that shutdown boundary.
    %{proof_scope | foreign: foreign}
  end

  defp committed_foreign_proof!(ctx) do
    foreign_id = Ecto.UUID.generate()
    UnboxedFixture.register_unboxed_cleanup!(fn -> Repo.delete_all(from(proof in ExecutionTerminalProof, where: proof.execution_id == ^foreign_id)) end)
    fixture = ctx.foreign_fixture
    assert fixture.model.exposed_model_id == ctx.setup.model.exposed_model_id
    refute fixture.pool.id == ctx.setup.pool.id
    parent = self()
    ref = make_ref()

    inserter = Task.async(fn -> Sandbox.unboxed_run(Repo, fn -> insert_foreign_proof!(parent, ref, foreign_id) end) end)

    monitor = Process.monitor(inserter.pid)
    on_exit(fn -> send(inserter.pid, {ref, :insert}) end)
    assert_receive {^ref, :checked_out, foreign_backend}, @detection_timeout_ms
    [[observer_backend]] = Repo.query!("SELECT pg_backend_pid()").rows
    refute observer_backend == foreign_backend
    send(inserter.pid, {ref, :insert})
    assert Task.await(inserter, @detection_timeout_ms) == foreign_backend
    assert_receive {:DOWN, ^monitor, :process, _, :normal}, @detection_timeout_ms
    assert Repo.get!(ExecutionTerminalProof, foreign_id).execution_id == foreign_id
    %{id: foreign_id, fixture: fixture, foreign_backend: foreign_backend, observer_backend: observer_backend}
  end

  defp insert_foreign_proof!(parent, ref, foreign_id) do
    Repo.checkout(fn ->
      [[backend]] = Repo.query!("SELECT pg_backend_pid()").rows
      send(parent, {ref, :checked_out, backend})

      receive do
        {^ref, :insert} -> :ok
      after
        @detection_timeout_ms -> flunk("foreign proof insertion barrier was not released")
      end

      assert :ok == ExecutionRegistry.register(foreign_id)
      assert :ok == ExecutionRegistry.complete(foreign_id)
      [proof] = ExecutionRegistry.pending_proofs([foreign_id])
      assert {:ok, 1} == ExecutionTerminalProofs.publish([proof])
      assert :ok == ExecutionRegistry.acknowledge([foreign_id])
      backend
    end)
  end

  defp assert_owned_proof_cleanup!(publisher) do
    assert Repo.get!(ExecutionTerminalProof, publisher.attempt.owner_execution_id)
    publisher.cleanup.()
    refute Repo.exists?(from(proof in ExecutionTerminalProof, where: proof.execution_id in ^publisher.owned_execution_ids))
    assert Repo.get!(ExecutionTerminalProof, publisher.foreign.id)
    assert Repo.get!(CodexPooler.Pools.Pool, publisher.foreign.fixture.pool.id)
    assert Repo.get!(publisher.foreign.fixture.model.__struct__, publisher.foreign.fixture.model.id)
    assert Repo.get!(publisher.foreign.fixture.pricing.__struct__, publisher.foreign.fixture.pricing.id)
    CodexPooler.TestDiagnostics.puts(fn -> "compaction_owned_cleanup " <> CodexPooler.JSON.encode!(%{owned_execution_ids: publisher.owned_execution_ids, exact_attempt_id: publisher.attempt.id, foreign_execution_id: publisher.foreign.id, foreign_backend: publisher.foreign.foreign_backend, observer_backend: publisher.foreign.observer_backend, owned_proofs_absent: true, foreign_proof_preserved: true, same_model_fixture_preserved: true}) end)
  end

  defp assert_foreign_fixture_after_owned_cleanup!(ctx, proof_scope) do
    owner = owner_pid!(ctx.setup)
    monitor = Process.monitor(owner)
    stop_retirable_owner!(owner, :normal)
    assert_receive {:DOWN, ^monitor, :process, ^owner, _}, @detection_timeout_ms
    :ok = WebsocketCleanupFence.await_session_cleanups!()
    cleanup_unboxed_pool!(ctx.setup)
    refute Repo.get(CodexPooler.Pools.Pool, ctx.setup.pool.id)
    assert Repo.get!(CodexPooler.Pools.Pool, proof_scope.foreign.fixture.pool.id)
    assert Repo.get!(proof_scope.foreign.fixture.model.__struct__, proof_scope.foreign.fixture.model.id)
    assert Repo.get!(proof_scope.foreign.fixture.pricing.__struct__, proof_scope.foreign.fixture.pricing.id)
    assert Repo.get!(ExecutionTerminalProof, proof_scope.foreign.id)
    CodexPooler.TestDiagnostics.puts(fn -> "compaction_fixture_cleanup " <> CodexPooler.JSON.encode!(%{removed_pool_id: ctx.setup.pool.id, preserved_pool_id: proof_scope.foreign.fixture.pool.id, preserved_model_id: proof_scope.foreign.fixture.model.id, preserved_pricing_id: proof_scope.foreign.fixture.pricing.id, preserved_execution_id: proof_scope.foreign.id, original_owner_exited: true}) end)
  end

  defp cut_request_state(request_id) do
    request = Repo.get!(Request, request_id)
    kinds = Repo.all(from(entry in LedgerEntry, where: entry.request_id == ^request_id, select: entry.entry_kind))
    reservation = if "release" in kinds, do: :reservation_released, else: if("reservation" in kinds, do: :reservation_held, else: :no_reservation)
    {request.status, request.last_error_code, reservation}
  end

  # The socket the owner streams the running compaction to, before its client
  # closes it.
  defp attached_socket!(ctx) do
    owner = owner_pid!(ctx.setup)
    %{downstream: %{pid: socket}} = :sys.get_state(owner)
    socket
  end

  # The first retry is served when the predecessor it took over settles within
  # the socket's take-over wait (two seconds). Only the cut request's own
  # settlement decides that, and on a starved machine it can come later: the
  # first retry is then refused with the take-over logged
  # `inherited_turn_unsettled`, and the next one is served, which is the bound
  # the product sets. `:observed_cut_settlement_held` pins the refusal followed
  # by a served retry and separately asserts the first retry waited the full budget. First-send
  # success after take-over is covered by InheritedTurnTakeOverTest; shortening
  # the wait can fail that contract while preserving this allowed outcome.
  # The settlement is held (findings#206 row 206-580: once in about 120 runs at a load
  # average of 15-27). Any other refusal, or that one followed by anything but
  # a served retry, stays in the result.
  defp within_settlement_bound([{409, "duplicate_turn", :inherited_turn_unsettled}, :served], ctx) do
    CodexPooler.TestDiagnostics.puts(fn -> "206-580 #{ctx.shape} #{ctx.topology}: the predecessor settled after the take-over wait" end)
    [:served]
  end

  defp within_settlement_bound(retries, _ctx), do: retries

  defp resend_after_observed_cut(:observed_cut, %{topology: :direct_committed} = ctx, port, release, _socket) do
    # A direct retry cannot take over an owner. First-service success requires
    # its actual closing cleanup and all original terminal rows to be ready.
    release.()
    await_cut_compaction_settled!(ctx.setup.pool.id)
    released_client_retries!(ctx, port, release)
  end

  defp resend_after_observed_cut(:observed_cut_claim_held, ctx, port, release, _socket) do
    ctx |> hold_direct_claim_settlement!(release) |> released_client_retries!(port, release)
  end

  defp resend_after_observed_cut(cut, ctx, port, release, closing_socket) do
    ctx = if cut == :observed_cut_settlement_held, do: hold_predecessor_settlement!(ctx, closing_socket), else: ctx
    retries = released_client_retries!(ctx, port, release)
    if List.last(retries) == :served, do: release.()
    if cut == :observed_cut_settlement_held, do: retries, else: within_settlement_bound(retries, ctx)
  end

  defp hold_direct_claim_settlement!(ctx, release) do
    [predecessor] = Enum.filter(pool_requests(ctx.setup.pool.id), &compaction_row?/1)

    Map.put(ctx, :after_first_retry, fn started_at, finished_at ->
      assert finished_at - started_at >= 1_000, "the live compact claim did not receive its original one-second wait"
      assert Repo.get!(Request, predecessor.id).status == "in_progress"
      assert Repo.get_by!(CodexTurn, request_id: predecessor.id).status == "in_progress"
      assert length(pool_requests(ctx.setup.pool.id)) == 2
      assert FakeUpstream.physical_counts(ctx.upstream).websocket_generation == 2
      assert charges(predecessor) == 0
      release.()
      await_cut_compaction_settled!(ctx.setup.pool.id)
    end)
  end

  # Holds the cut request's settlement: the first query the closing socket's
  # own processes (its response task, which settles the request the owner
  # cancelled at the take-over) make once the retries start, other than its
  # session cleanup, which `hold_session_cleanups!/2` holds. Released when the
  # first retry has its answer, which comes only after the take-over wait. The
  # held query keeps its connection, so only the peer topology's committed rows
  # (a pooled connection each) can run it; on the single-node sandbox the held
  # connection is everyone's.
  defp hold_predecessor_settlement!(ctx, closing_socket) do
    hold = make_ref()
    handler_id = {__MODULE__, :settlement_hold, hold}
    on_exit(fn -> :telemetry.detach(handler_id) end)
    config = %{hold: hold, test: self(), socket: closing_socket, claimed: :atomics.new(1, [])}
    :ok = :telemetry.attach(handler_id, [:codex_pooler, :repo, :query], &__MODULE__.hold_settlement_query/4, config)

    Map.put(ctx, :after_first_retry, fn retry_started_at, retry_finished_at ->
      :telemetry.detach(handler_id)
      assert_received {^hold, :held, settler, _settlement_started_at}, "the cut request's settlement never started while the first retry waited"
      send(settler, {hold, :release})
      await_cut_compaction_settled!(ctx.setup.pool.id)
      elapsed_ms = retry_finished_at - retry_started_at
      assert elapsed_ms >= 2_000, "the first retry must receive the 2s take-over budget, observed #{elapsed_ms}ms"
    end)
  end

  # The held query can be any step of the cut request's settlement, which writes
  # the request and attempt and then the turn. The next retry is sent once the
  # whole settlement is visible, as the released client's retry comes about
  # 400 ms after the refusal: sent at once, it could land between those writes,
  # where the replay preflight closes the still-open turn as orphaned
  # (`orphaned_turn_closed`) and the predecessor is no longer a resendable cut
  # (findings#206 row 206-607, seen on a CI runner).
  defp await_cut_compaction_settled!(pool_id) do
    await!(
      fn ->
        Repo.all(
          from(request in Request,
            join: turn in CodexTurn,
            on: turn.request_id == request.id,
            join: attempt in Attempt,
            on: attempt.id == turn.final_attempt_id and attempt.request_id == request.id,
            where: request.pool_id == ^pool_id and request.endpoint == @compact_endpoint,
            select: {request.status, attempt.status, turn.status}
          )
        )
        |> case do
          [{request_status, attempt_status, turn_status}] -> request_status not in ["accepted", "in_progress"] and attempt_status not in ["queued", "in_progress"] and turn_status != "in_progress"
          _missing_or_ambiguous -> false
        end
      end,
      "the cut compaction's settlement never finished after its hold was released"
    )
  end

  @doc false
  def hold_settlement_query(_event, _measurements, _metadata, %{hold: hold, test: test, socket: socket, claimed: claimed}) do
    if socket in Process.get(:"$callers", []) and not match?({CodexPoolerWeb.WebsocketControlPath, _function, _arity}, Process.get(:"$initial_call")) and
         :atomics.add_get(claimed, 1, 1) == 1 do
      send(test, {hold, :held, self(), System.monotonic_time(:millisecond)})

      receive do
        {^hold, :release} -> :ok
      after
        @detection_timeout_ms -> :ok
      end
    end

    :ok
  end

  # The released client's retry of a remote compaction it did not complete
  # (`compact_remote_v2.rs`, measured on the wire in P69 with Codex 0.156.1):
  # two websocket retries, each on a new connection with the full history,
  # then `POST /responses` over SSE with the same body (no `type`, no
  # websocket start timestamp, the Lite marker moved to a header) and two
  # more HTTP retries; after those the turn fails and the compaction is lost.
  defp released_client_retries!(ctx, port, before_https) do
    case websocket_retries!(ctx, port, 2, []) do
      {:served, outcomes} ->
        outcomes

      {:refused, outcomes} ->
        before_https.()
        outcomes ++ https_retries!(ctx, 3, [])
    end
  end

  defp websocket_retries!(_ctx, _port, 0, outcomes), do: {:refused, Enum.reverse(outcomes)}

  defp websocket_retries!(ctx, port, remaining, outcomes) do
    retry_started_at = System.monotonic_time(:millisecond)
    outcome = full_history_resend!(ctx, port)
    retry_finished_at = System.monotonic_time(:millisecond)

    ctx =
      case Map.pop(ctx, :after_first_retry) do
        {nil, ctx} ->
          ctx

        {after_first_retry, ctx} ->
          after_first_retry.(retry_started_at, retry_finished_at)
          ctx
      end

    case outcome do
      :served -> {:served, Enum.reverse([:served | outcomes])}
      refused -> websocket_retries!(ctx, port, remaining - 1, [refused | outcomes])
    end
  end

  # One websocket retry: a new connection and the same compaction as full
  # history; when it is served the turn continues on that connection. The
  # released client waits about 200 ms before its next retry, and what that
  # retry meets depends on the closed connection's session cleanup (its owner
  # detach): the next retry is sent once that cleanup finished, not the moment
  # the connection closed. Sent at once, it met the live predecessor whenever
  # the cleanup outlasted the socket's 100 ms yield (findings#206 row 206-425).
  #
  # A refused retry's own refusal lines (the replay preflight's or the claim's
  # bounded reason codes, info level) are logged again as a warning, so a
  # failing arm's captured log names why the retry was refused (findings#206
  # row 206-580: a peer arm's first retry refused once in about 120 runs
  # under a busy machine, with nothing but `cleanup_deferred` in its log).
  defp full_history_resend!(ctx, port) do
    client = connect!(port, ctx.setup)

    {outcome, log} =
      with_info_log(fn ->
        try do
          client = send_frame!(client, full_history_compaction_frame(ctx))

          case receive_frame!(client) do
            {_client, %{"type" => "error", "status" => status, "error" => %{"code" => code}}} ->
              {status, code, nil}

            {client, %{"type" => "response.output_item.done"}} ->
              {client, ["response.completed"]} = receive_until_terminal(client, [])
              client |> send_frame!(resume_frame(ctx, "resend")) |> ordinary_turn!()
              :served
          end
        after
          Mint.HTTP.close(client.conn)
        end
      end)

    if outcome != :served do
      for line <- String.split(log, "\n"), line =~ ~r/replay rejection|live predecessor|duplicate|reconnect disposition/ do
        Logger.warning("refused compaction retry: " <> String.trim(line))
      end
    end

    :ok = WebsocketCleanupFence.await_listener_socket_cleanup!(client.cleanup_socket)
    refused_retry(outcome, log)
  end

  # A refused retry names its status and code, and the take-over disposition
  # when the socket took the predecessor over but could not see it settle.
  defp refused_retry({status, code, nil}, log) do
    if log =~ "reconnect_disposition=inherited_turn_unsettled", do: {status, code, :inherited_turn_unsettled}, else: {status, code}
  end

  defp refused_retry(outcome, _log), do: outcome

  defp https_retries!(_ctx, 0, outcomes), do: Enum.reverse(outcomes)

  defp https_retries!(ctx, remaining, outcomes) do
    compaction = post_native!(ctx, https_body(full_history_compaction_payload(ctx)), compaction_metadata(ctx.shape), @window_id)

    if compaction.status == 200 do
      assert compaction.resp_body =~ "response.completed"
      {payload, metadata} = resume_payload(ctx, "https")
      resume = post_native!(ctx, https_body(payload), metadata, @resumed_window_id)
      assert resume.status == 200 and resume.resp_body =~ "response.completed", inspect({resume.status, resume.resp_body})
      Enum.reverse([:http_served | outcomes])
    else
      code = get_in(CodexPooler.JSON.decode!(compaction.resp_body), ["error", "code"])
      https_retries!(ctx, remaining - 1, [{:http, compaction.status, code} | outcomes])
    end
  end

  # The HTTP request the released client builds from the websocket one: the
  # same body without the websocket-only keys, the turn metadata echoed as a
  # header, and in Lite the marker as `x-openai-internal-codex-responses-lite`.
  defp post_native!(ctx, body, metadata, window_id) do
    conn =
      build_conn()
      |> put_req_header("authorization", ctx.setup.authorization)
      |> put_req_header("content-type", "application/json")
      |> put_req_header("accept", "text/event-stream")
      |> put_req_header("session-id", @thread_id)
      |> put_req_header("thread-id", @thread_id)
      |> put_req_header("x-client-request-id", @thread_id)
      |> put_req_header("x-codex-window-id", window_id)
      |> put_req_header("x-codex-turn-metadata", metadata)
      |> put_req_header("originator", "codex_cli_rs")

    conn = if ctx.mode == "lite", do: put_req_header(conn, "x-openai-internal-codex-responses-lite", "true"), else: conn
    post(conn, @turn_endpoint, CodexPooler.JSON.encode!(body))
  end

  defp https_body(payload) do
    payload
    |> Map.delete("type")
    |> Map.update!("client_metadata", &Map.drop(&1, ["x-codex-ws-stream-request-start-ms", @lite_marker]))
  end

  # The provider finishes the cut generation, as a live provider would; the
  # Pooler has already settled it. The Pooler may have closed the provider
  # connection when it abandoned the generation, and then the rest of the reply
  # is never pushed: wait until either the last frame went out or that
  # connection is gone, and only then acknowledge the barriers the closed
  # connection never reached.
  defp release_held_compaction!(upstream, release_ref, next_barrier) do
    connection = held_connection(upstream)
    _released = FakeUpstream.release_remaining_frames(upstream, release_ref)

    await!(
      fn ->
        receive do
          {:fake_upstream_frame_barrier, 3, _handler, ^release_ref} -> true
        after
          0 -> not FakeUpstream.websocket_connection_alive?(upstream, connection)
        end
      end,
      "the held provider reply neither finished nor lost its connection"
    )

    for barrier <- next_barrier..3//1, do: FakeUpstream.acknowledge(upstream, {:frame_barrier, release_ref, barrier})
    :ok
  end

  defp held_connection(upstream) do
    %{websocket_connection_id: connection} = Enum.find(FakeUpstream.requests(upstream), &(&1.json["previous_response_id"] == @anchor))
    connection
  end

  defp await_barrier!(barrier, release_ref) do
    receive do
      {:fake_upstream_frame_barrier, ^barrier, handler, ^release_ref} -> handler
    after
      @detection_timeout_ms -> flunk("the upstream never reached frame barrier #{barrier}")
    end
  end

  defp upstream_sequence(ctx, cut, topology, release_ref) do
    turn = FakeUpstream.expect_request(method: "WEBSOCKET", json: [valid: true, equals: %{"type" => "response.create"}, forbidden: ["previous_response_id"]], respond: completed_frames(@anchor, t1_output(ctx.shape)))

    anchored =
      FakeUpstream.expect_request(
        method: "WEBSOCKET",
        websocket_connection_ordinal: 1,
        json: [valid: true, equals: lite_marker_expectation(%{"type" => "response.create", "previous_response_id" => @anchor}, ctx.mode)],
        respond:
          case cut do
            :no_cut -> compaction_frames(compaction_item("served"), @cut_response)
            :provider_cut_after_output -> FakeUpstream.websocket_text_frames_then_abrupt_close(Enum.take(held_compaction_messages(), 2))
            _held -> FakeUpstream.barrier_websocket_frames(held_compaction_messages(), notify: self(), release_ref: release_ref)
          end
      )

    resume = FakeUpstream.expect_request(method: "WEBSOCKET", json: [valid: true, equals: %{"type" => "response.create"}, forbidden: ["previous_response_id"]], respond: completed_frames(@final_response, []))

    full_history =
      FakeUpstream.expect_request(
        method: "WEBSOCKET",
        json: [valid: true, equals: lite_marker_expectation(%{"type" => "response.create"}, ctx.mode), forbidden: ["previous_response_id"]],
        respond: compaction_frames(compaction_item("resend"), @resend_response)
      )

    case {cut, topology} do
      {:no_cut, _topology} ->
        [turn, anchored, resume]

      {:unobserved_cut, :direct} ->
        https_compaction =
          FakeUpstream.expect_request(
            method: "POST",
            path: @turn_endpoint,
            json: [valid: true, forbidden: ["previous_response_id", "type"]],
            respond: FakeUpstream.sse_stream(compaction_events(compaction_item("https"), @resend_response))
          )

        https_resume =
          FakeUpstream.expect_request(method: "POST", path: @turn_endpoint, json: [valid: true, forbidden: ["previous_response_id"]], respond: FakeUpstream.sse_stream(completed_events(@final_response)))

        [turn, anchored, https_compaction, https_resume]

      {:drain_no_resend, _topology} ->
        [turn, anchored]

      _websocket_resend ->
        [turn, anchored, full_history, resume]
    end
  end

  defp lite_marker_expectation(expected, "lite"), do: Map.put(expected, "client_metadata.#{@lite_marker}", "true")
  defp lite_marker_expectation(expected, "full"), do: expected

  defp compaction_request?(%{json: %{"input" => input}}) when is_list(input), do: match?(%{"type" => "compaction_trigger"}, List.last(input))
  defp compaction_request?(_request), do: false

  # A charge is a settlement that billed known usage.
  defp charges(%Request{id: request_id}) do
    Repo.aggregate(
      from(entry in LedgerEntry, where: entry.request_id == ^request_id and entry.entry_kind == "settlement" and entry.usage_status == "usage_known" and entry.settled_cost_micros > 0),
      :count
    )
  end

  # Both a websocket compaction and its HTTPS fallback (which the Pooler
  # bridges to a compact request) are recorded on the compact endpoint.
  defp compaction_row?(%Request{endpoint: endpoint}), do: endpoint == @compact_endpoint

  # Chained to its predecessor: the owner's client-retry link, or the resend
  # policy's `client_resend` marker when owner forwarding is off.
  defp chained?(%Request{request_metadata: %{"client_resend" => %{"predecessor_request_id" => predecessor}}}) when is_binary(predecessor), do: true
  defp chained?(%Request{id: request_id}), do: Repo.exists?(from(link in RequestClientRetryLink, where: link.successor_request_id == ^request_id))

  defp pool_requests(pool_id), do: Repo.all(from(request in Request, where: request.pool_id == ^pool_id, order_by: [asc: request.admitted_at, asc: request.id]))

  # Every response task settles its request after the terminal frame, and a
  # cut request after the closing socket's drain; no completion signal reaches
  # the test, so poll the rows within a bounded detection budget.
  defp await_compaction_settled!(pool_id, statuses) do
    await!(fn -> Enum.any?(pool_requests(pool_id), &(&1.endpoint == @compact_endpoint and &1.status in statuses)) end, "the compaction never settled")
  end

  defp await_settled!(pool_id) do
    await!(fn -> Enum.all?(pool_requests(pool_id), &(&1.status not in ["accepted", "in_progress"])) end, "requests did not settle")
    pool_requests(pool_id)
  end

  # A new connection attaches and receives the running compaction, then drops
  # before it sends anything; its cleanup is held as well, so the owner handles
  # its exit (no downstream, the compaction still bound to it) before the retry
  # attaches.
  defp drop_inheriting_connection!(ctx, port, hold) do
    owner = owner_pid!(ctx.setup)
    inheriting = connect!(port, ctx.setup)
    await!(fn -> match?(%{downstream: %{epoch: 2, active_turn_reconnect?: true}, active_turn: %{downstream: %{epoch: 2}}}, :sys.get_state(owner)) end, "the new connection never received the running compaction")
    Mint.HTTP.close(inheriting.conn)
    assert_receive {^hold, :held, cleanup}, @detection_timeout_ms
    await!(fn -> match?(%{downstream: nil, active_turn: %{downstream: %{epoch: 2}}}, :sys.get_state(owner)) end, "the owner never handled the exit of the connection that received the compaction")
    cleanup
  end

  defp owner_pid!(%{peer_owner: %{owner_pid: owner}}), do: owner

  defp owner_pid!(setup) do
    [session_id] = Repo.all(from(session in CodexSession, where: session.pool_id == ^setup.pool.id, select: session.id))
    assert {:ok, owner} = WebsocketOwnerSession.lookup(session_id)
    owner
  end

  # Holds the next `count` socket session cleanups that start from here on (the
  # closed connections'; nothing else closes meanwhile) right after their first
  # query made outside a transaction, whose connection is already back in the
  # pool.
  #
  # With owner forwarding off the cleanup's first query opens the transaction
  # that stops the direct task, so there it is held at that query (committed
  # rows: the held connection stalls nobody else). Each scenario releases it
  # explicitly before its positive retry or after its held-live refusal.
  defp hold_session_cleanups!(count, topology) do
    hold = make_ref()
    handler_id = {__MODULE__, :session_cleanup_hold, hold}
    on_exit(fn -> :telemetry.detach(handler_id) end)
    config = %{hold: hold, test: self(), count: count, claimed: :atomics.new(1, []), any_query?: topology == :direct_committed}
    :ok = :telemetry.attach(handler_id, [:codex_pooler, :repo, :query], &__MODULE__.hold_session_cleanup_query/4, config)

    hold
  end

  @doc false
  def hold_session_cleanup_query(_event, _measurements, metadata, %{hold: hold, test: test, count: count, claimed: claimed, any_query?: any_query?}) do
    if match?({CodexPoolerWeb.WebsocketControlPath, _function, _arity}, Process.get(:"$initial_call")) and (any_query? or (metadata[:query] not in ["begin", "commit"] and not Repo.in_transaction?())) and
         is_nil(Process.get({__MODULE__, hold})) and :atomics.add_get(claimed, 1, 1) <= count do
      Process.put({__MODULE__, hold}, :held)
      send(test, {hold, :held, self()})

      receive do
        {^hold, :release} -> :ok
      after
        @detection_timeout_ms -> :ok
      end
    end

    :ok
  end

  defp release_session_cleanups!(hold, cleanups) do
    :telemetry.detach({__MODULE__, :session_cleanup_hold, hold})

    for cleanup <- cleanups do
      monitor = Process.monitor(cleanup)
      send(cleanup, {hold, :release})
      assert_receive {:DOWN, ^monitor, :process, ^cleanup, _reason}, @detection_timeout_ms
    end

    :ok
  end

  defp await!(condition, message) do
    deadline = System.monotonic_time(:millisecond) + @detection_timeout_ms

    Stream.repeatedly(condition)
    |> Enum.reduce_while(nil, fn
      true, _acc ->
        {:halt, :ok}

      false, _acc ->
        if System.monotonic_time(:millisecond) >= deadline, do: flunk(message)
        Process.sleep(10)
        {:cont, nil}
    end)
  end

  defp connect!(port, setup) do
    sockets = WebsocketCleanupFence.listener_sockets()
    {:ok, conn} = Mint.HTTP.connect(:http, "127.0.0.1", port, protocols: [:http1])

    headers = [
      {"authorization", setup.authorization},
      {"session-id", @thread_id},
      {"thread-id", @thread_id},
      {"x-client-request-id", @thread_id},
      {"x-codex-window-id", @window_id}
    ]

    {:ok, conn, ref} = Mint.WebSocket.upgrade(:ws, conn, "/backend-api/codex/responses", headers)
    {:ok, conn, status, response_headers} = await_public_websocket_upgrade(conn, ref)
    {conn, websocket} = mint_websocket_new!(conn, ref, status, response_headers)
    %{conn: conn, websocket: websocket, ref: ref, cleanup_socket: WebsocketCleanupFence.await_new_listener_socket!(sockets)}
  end

  defp ordinary_turn!(client, frame), do: client |> send_frame!(frame) |> ordinary_turn!()

  defp ordinary_turn!(client) do
    {client, frames} = receive_until_terminal(client, [])
    assert List.last(frames) == "response.completed", inspect(frames)
    client
  end

  defp receive_until_terminal(client, seen) do
    {client, frame} = receive_frame!(client)
    seen = [frame["type"] | seen]

    if frame["type"] in ["response.completed", "error", "response.failed"],
      do: {client, Enum.reverse(seen)},
      else: receive_until_terminal(client, seen)
  end

  defp send_frame!(client, text) do
    {conn, websocket} = public_websocket_send_text!(client.conn, client.websocket, client.ref, text)
    %{client | conn: conn, websocket: websocket}
  end

  defp receive_frame!(client) do
    {conn, websocket, text} = public_websocket_receive_text!(client.conn, client.websocket, client.ref)
    {%{client | conn: conn, websocket: websocket}, CodexPooler.JSON.decode!(text)}
  end

  # The owner arms a forwarded admission after the terminal frame left it;
  # poll its authoritative state until it is armed for the attached socket.
  # The direct upstream session arms before its turn settles.
  defp await_armed!(topology, setup) when topology in [:direct, :direct_committed], do: await!(fn -> match?([%Request{status: "succeeded"}], pool_requests(setup.pool.id)) end, "the first turn never settled")

  defp await_armed!(:peer, setup), do: await_owner_armed!(setup.peer_owner.owner_pid)

  defp await_armed!(:forwarded, setup) do
    [session_id] = Repo.all(from(session in CodexSession, where: session.pool_id == ^setup.pool.id, select: session.id))
    assert {:ok, owner} = WebsocketOwnerSession.lookup(session_id)
    await_owner_armed!(owner)
  end

  defp await_owner_armed!(owner) do
    await!(
      fn ->
        match?(
          %{native_compaction_admission: %NativeCompactionAdmission{phase: :pending_compact}, native_compaction_admission_downstream: %{pid: pid}, downstream: %{pid: pid}},
          :sys.get_state(owner)
        )
      end,
      "the owner never armed the native compaction admission for the attached socket"
    )
  end

  # Holds the first response task that settles a websocket turn from here on
  # (the previous turn's) right after its settlement, outside any transaction,
  # so its socket still tracks it when the compaction arrives.
  defp hold_turn_task_after_settlement! do
    hold = make_ref()
    handler_id = {__MODULE__, :turn_settlement_hold, hold}
    on_exit(fn -> :telemetry.detach(handler_id) end)
    config = %{hold: hold, test: self(), claimed: :atomics.new(1, [])}
    :ok = :telemetry.attach(handler_id, [:codex_pooler, :gateway, :stream, :outcome], &__MODULE__.hold_settled_turn_task/4, config)
    hold
  end

  @doc false
  def hold_settled_turn_task(_event, _measurements, %{outcome: "succeeded", downstream_transport: "websocket"}, %{hold: hold, test: test, claimed: claimed}) do
    if not Repo.in_transaction?() and :atomics.add_get(claimed, 1, 1) == 1 do
      send(test, {hold, :held, self(), Process.get(:"$callers", [])})

      receive do
        {^hold, :release} -> :ok
      after
        @detection_timeout_ms -> :ok
      end
    end

    :ok
  end

  def hold_settled_turn_task(_event, _measurements, _metadata, _config), do: :ok

  # The compaction is queued behind the held task on the socket that started
  # it; the task is then released, and the socket dispatches the compaction
  # when the task ends. With owner forwarding the owner runs it as a turn
  # keyed like the one the owner preflight records for an unqueued frame.
  defp dispatch_queued_compaction!(ctx, hold) do
    assert_receive {^hold, :held, task, callers}, @detection_timeout_ms
    :telemetry.detach({__MODULE__, :turn_settlement_hold, hold})
    await!(fn -> Enum.any?(callers, &compaction_queued?/1) end, "the compaction was never queued behind the settling turn")
    send(task, {hold, :release})

    if ctx.topology in [:forwarded, :peer] do
      owner = owner_pid!(ctx.setup)
      await!(fn -> match?(%{active_turn: %{admission_phase: phase}} when not is_nil(phase), :sys.get_state(owner)) end, "the queued compaction never reached the owner")
      assert %{active_turn: %{descriptor: %{kind: :native, semantic_turn_key: key}}} = :sys.get_state(owner)
      assert key == compaction_turn_key(ctx)
    end

    :ok
  end

  defp compaction_queued?(pid) do
    pid |> :sys.get_state(1_000) |> queued_frames(6) |> Enum.any?(&match?(%{endpoint: @compact_endpoint}, &1))
  catch
    :exit, _not_a_socket -> false
  end

  defp queued_frames(%{queued_response_payloads: queue}, _depth), do: :queue.to_list(queue)
  defp queued_frames(_term, 0), do: []
  defp queued_frames(%_{} = struct, depth), do: struct |> Map.from_struct() |> queued_frames(depth)
  defp queued_frames(map, depth) when is_map(map), do: Enum.flat_map(Map.values(map), &queued_frames(&1, depth - 1))
  defp queued_frames(tuple, depth) when is_tuple(tuple), do: tuple |> Tuple.to_list() |> Enum.flat_map(&queued_frames(&1, depth - 1))
  defp queued_frames(_term, _depth), do: []

  # The compaction turn's semantic key under the socket's claim scope (the
  # client thread, bound to the session's Pool and key).
  defp compaction_turn_key(ctx) do
    [session] = Repo.all(from(session in CodexSession, where: session.pool_id == ^ctx.setup.pool.id))
    scope = WebsocketTurnIdentity.claim_scope(session, @thread_id)
    {:ok, %{semantic_turn_key: key}} = WebsocketTurnIdentity.resolve(%{"client_metadata" => %{"turn_id" => compaction_turn(ctx.shape)}}, scope)
    key
  end

  defp put_owner_forwarding!(enabled?) do
    previous = Application.fetch_env(:codex_pooler, :websocket_owner_forwarding_enabled)

    on_exit(fn ->
      case previous do
        :error -> Application.delete_env(:codex_pooler, :websocket_owner_forwarding_enabled)
        {:ok, value} -> Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, value)
      end
    end)

    Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, enabled?)
  end

  defp compaction_item(label), do: %{"type" => "compaction", "encrypted_content" => "synthetic-preturn-cut-#{label}"}

  defp prompt(label), do: %{"type" => "message", "role" => "user", "content" => [%{"type" => "input_text", "text" => "synthetic #{label} prompt"}]}

  defp answer, do: %{"type" => "message", "role" => "assistant", "content" => [%{"type" => "output_text", "text" => "synthetic answer"}]}

  defp function_call, do: %{"type" => "function_call", "call_id" => "call_preturn_cut", "name" => "shell", "arguments" => "{}"}

  defp function_call_output, do: %{"type" => "function_call_output", "call_id" => "call_preturn_cut", "output" => "synthetic output"}

  defp t1_output(shape) when shape in [:pre_turn, :standalone_turn], do: [answer()]
  defp t1_output(:mid_turn), do: [function_call()]

  # What the compaction adds after the anchor: the pre-turn compaction only the
  # trigger, a mid-turn one the tool round's outputs and the trigger.
  defp compaction_delta(shape) when shape in [:pre_turn, :standalone_turn], do: [%{"type" => "compaction_trigger"}]
  defp compaction_delta(:mid_turn), do: [function_call_output(), %{"type" => "compaction_trigger"}]

  # The pre-turn compaction runs inside the next turn, a mid-turn one inside
  # its own, a manual `/compact` in a standalone turn of its own.
  defp compaction_turn(:pre_turn), do: @next_turn_id
  defp compaction_turn(:mid_turn), do: @turn_id
  defp compaction_turn(:standalone_turn), do: @standalone_turn_id

  # The turn that continues on the compacted history: the pre-turn and
  # mid-turn compaction's own turn, the next user turn after a standalone one.
  defp resume_turn(:standalone_turn), do: @next_turn_id
  defp resume_turn(shape), do: compaction_turn(shape)

  # After a pre-turn or manual compaction the next user turn opens on the
  # compacted history; after a mid-turn one the same turn continues on it with
  # no new user message (one carrying a user message after the compaction item
  # would open a new turn, which a reused turn id is refused over HTTPS).
  defp resume_delta(:mid_turn), do: []
  defp resume_delta(_shape), do: [prompt("next")]

  # The released Lite client opens a provider context with its tool manifest.
  defp context_prefix("lite"), do: [%{"type" => "additional_tools", "role" => "developer", "tools" => []}]
  defp context_prefix("full"), do: []

  defp turn_frame(ctx) do
    ctx
    |> frame(context_prefix(ctx.mode) ++ [prompt("first")], @turn_id, @window_id)
    |> put_in(["client_metadata", "x-codex-turn-metadata"], turn_metadata(%{"request_kind" => "turn"}))
    |> CodexPooler.JSON.encode!()
  end

  defp anchored_compaction_frame(ctx) do
    ctx
    |> frame(compaction_delta(ctx.shape), compaction_turn(ctx.shape), @window_id)
    |> Map.put("previous_response_id", Map.get(ctx, :anchor, @anchor))
    |> put_in(["client_metadata", "x-codex-turn-metadata"], compaction_metadata(ctx.shape))
    |> CodexPooler.JSON.encode!()
  end

  defp full_history_compaction_frame(ctx), do: ctx |> full_history_compaction_payload() |> CodexPooler.JSON.encode!()

  defp full_history_compaction_payload(ctx) do
    ctx
    |> frame(context_prefix(ctx.mode) ++ [prompt("first") | t1_output(ctx.shape)] ++ compaction_delta(ctx.shape), compaction_turn(ctx.shape), @window_id)
    |> put_in(["client_metadata", "x-codex-turn-metadata"], compaction_metadata(ctx.shape))
  end

  defp resume_frame(ctx, label) do
    {payload, _metadata} = resume_payload(ctx, label)
    CodexPooler.JSON.encode!(payload)
  end

  defp resume_payload(ctx, label) do
    turn_id = resume_turn(ctx.shape)
    metadata = turn_metadata(%{"request_kind" => "turn", "turn_id" => turn_id, "root_turn_id" => turn_id, "window_id" => @resumed_window_id, "window_number" => 1, "context_window_id" => @resumed_context_window_id})

    payload =
      ctx
      |> frame(context_prefix(ctx.mode) ++ [compaction_item(label) | resume_delta(ctx.shape)], turn_id, @resumed_window_id)
      |> put_in(["client_metadata", "x-codex-turn-metadata"], metadata)

    {payload, metadata}
  end

  defp compaction_metadata(shape) do
    turn_id = compaction_turn(shape)
    compaction = %{"trigger" => "auto", "reason" => "context_limit", "implementation" => "responses_compaction_v2", "phase" => Atom.to_string(shape), "strategy" => "memento"}
    compaction = if shape == :standalone_turn, do: %{compaction | "trigger" => "manual", "reason" => "user_requested"}, else: compaction
    turn_metadata(%{"request_kind" => "compaction", "compaction" => compaction, "turn_id" => turn_id, "root_turn_id" => turn_id})
  end

  # Full: the released client's top-level `instructions` and `tools`, parallel
  # tool calls on. Lite (`use_responses_lite` in the catalog): neither
  # top-level key, parallel tool calls off, and the Lite marker in
  # `client_metadata` (P63 wire probe of Codex 0.156.1 on a Lite model).
  defp frame(ctx, input, turn_id, window_id) do
    client_metadata = %{
      "session_id" => @thread_id,
      "thread_id" => @thread_id,
      "turn_id" => turn_id,
      "root_turn_id" => turn_id,
      "x-codex-installation-id" => @installation_id,
      "x-codex-window-id" => window_id,
      "x-codex-ws-stream-request-start-ms" => Integer.to_string(System.system_time(:millisecond))
    }

    base = %{
      "type" => "response.create",
      "model" => ctx.setup.model.exposed_model_id,
      "input" => input,
      "tool_choice" => "auto",
      "reasoning" => %{"effort" => "low"},
      "store" => false,
      "stream" => true,
      "include" => ["reasoning.encrypted_content"],
      "text" => %{"verbosity" => "low"},
      "prompt_cache_key" => @thread_id
    }

    case ctx.mode do
      "full" -> Map.merge(base, %{"instructions" => "synthetic instructions", "tools" => [], "parallel_tool_calls" => true, "client_metadata" => client_metadata})
      "lite" -> Map.merge(base, %{"parallel_tool_calls" => false, "client_metadata" => Map.put(client_metadata, @lite_marker, "true")})
    end
  end

  defp turn_metadata(extra) do
    %{
      "agent_name" => "/root",
      "analytics_enabled" => true,
      "auto_review_enabled" => false,
      "context_window_id" => @context_window_id,
      "installation_id" => @installation_id,
      "root_turn_id" => @turn_id,
      "sandbox" => "seatbelt",
      "sandbox_mode" => "read-only",
      "session_id" => @thread_id,
      "thread_id" => @thread_id,
      "turn_id" => @turn_id,
      "turn_started_at_unix_ms" => 1_790_000_000_000,
      "window_id" => @window_id,
      "window_number" => 0,
      "model" => "gpt-test-model",
      "reasoning_effort" => "low"
    }
    |> Map.merge(extra)
    |> CodexPooler.JSON.encode!()
  end

  defp usage, do: %{"input_tokens" => 20_000, "output_tokens" => 10, "total_tokens" => 20_010}

  defp completed_frames(response_id, output) do
    FakeUpstream.websocket_text_frames(
      [CodexPooler.JSON.encode!(%{"type" => "response.created", "response" => %{"id" => response_id, "status" => "in_progress"}})] ++
        Enum.map(output, &CodexPooler.JSON.encode!(%{"type" => "response.output_item.done", "item" => &1})) ++
        [CodexPooler.JSON.encode!(%{"type" => "response.completed", "response" => %{"id" => response_id, "status" => "completed", "output" => output, "usage" => usage()}})]
    )
  end

  defp held_compaction_messages do
    item = compaction_item("cut")

    [
      CodexPooler.JSON.encode!(%{"type" => "response.created", "response" => %{"id" => @cut_response, "status" => "in_progress"}}),
      CodexPooler.JSON.encode!(%{"type" => "response.output_item.done", "item" => item}),
      CodexPooler.JSON.encode!(%{"type" => "response.completed", "response" => %{"id" => @cut_response, "status" => "completed", "output" => [item], "usage" => usage()}})
    ]
  end

  defp compaction_frames(item, response_id) do
    FakeUpstream.websocket_text_frames([
      CodexPooler.JSON.encode!(%{"type" => "response.output_item.done", "item" => item}),
      CodexPooler.JSON.encode!(%{"type" => "response.completed", "response" => %{"id" => response_id, "status" => "completed", "output" => [item], "usage" => usage()}})
    ])
  end

  defp compaction_events(item, response_id) do
    [
      %{"type" => "response.created", "response" => %{"id" => response_id, "status" => "in_progress"}},
      %{"type" => "response.output_item.done", "item" => item},
      %{"type" => "response.completed", "response" => %{"id" => response_id, "status" => "completed", "output" => [item], "usage" => usage()}}
    ]
  end

  defp completed_events(response_id) do
    [
      %{"type" => "response.created", "response" => %{"id" => response_id, "status" => "in_progress"}},
      %{"type" => "response.completed", "response" => %{"id" => response_id, "status" => "completed", "output" => [], "usage" => usage()}}
    ]
  end
end
