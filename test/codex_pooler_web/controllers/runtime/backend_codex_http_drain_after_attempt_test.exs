defmodule CodexPoolerWeb.Runtime.BackendCodexHttpDrainAfterAttemptTest do
  # A native HTTP turn stopped by a rollout drain at a candidate's drain
  # checkpoint (`Dispatch.drain_checkpoint/1`) is told to retry, and the client
  # resends the turn on another pod. When an earlier candidate of the request
  # already failed before any output, the release follows that attempt: by the
  # findings#221 rule it carries the attempt, no pre-attempt phase and unknown
  # usage, and the pre-attempt release metric does not count it (findings#321
  # row 321-2). Only the writer changed: the native HTTP claim walk, which
  # never reads the release, still steps over the drained request and serves
  # the resend.
  #
  # One BEAM node, two accounts of one Pool with deterministic rotation,
  # FakeUpstream, a test-scoped deferred stream registry wired in through the
  # application env (as `backend_codex_http_drain_resend_test.exs` does).
  use CodexPoolerWeb.ConnCase, async: false

  import Ecto.Query
  import CodexPoolerWeb.Runtime.BackendCodexTestSupport, only: [auth: 2, deterministic_rotation_seed: 2, native_text_input: 1, stream_retry_setup: 2, stream_success_sse: 0]

  alias CodexPooler.Access
  alias CodexPooler.Accounting.{Attempt, LedgerEntry, PreAttemptRelease, Request, RequestClientRetryLink}
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway.Payloads.RequestOptions
  alias CodexPooler.Gateway.Persistence.{CodexTurn, SessionContinuity}
  alias CodexPooler.Gateway.Routing.{BridgeRing, RoutePlanInput}
  alias CodexPooler.Gateway.Runtime.Dispatch.PreDispatch
  alias CodexPooler.Gateway.Runtime.Service
  alias CodexPooler.Gateway.Transports.Streaming.DeferredStreamRegistry
  alias CodexPooler.Repo
  alias Ecto.Adapters.SQL.Sandbox

  @moduletag capture_log: true
  @path "/backend-api/codex/responses"
  @turn_id "turn_321_drain_after_attempt"
  @budget 15_000

  setup do
    stream_registry = :"deferred-stream-registry-#{System.unique_integer([:positive])}"
    start_supervised!({DeferredStreamRegistry, name: stream_registry})
    previous = Application.get_env(:codex_pooler, DeferredStreamRegistry)
    Application.put_env(:codex_pooler, DeferredStreamRegistry, server_name: stream_registry)

    restore = fn ->
      if previous,
        do: Application.put_env(:codex_pooler, DeferredStreamRegistry, previous),
        else: Application.delete_env(:codex_pooler, DeferredStreamRegistry)
    end

    on_exit(restore)
    {:ok, stream_registry: stream_registry, leave_drained_pod: restore}
  end

  # The first candidate's 503 is held while the drain starts, so the turn
  # reaches the second candidate's drain checkpoint after the first attempt.
  test "a drain before the next candidate after a retryable failure releases after that attempt, and the resend is served", %{conn: conn, stream_registry: stream_registry, leave_drained_pod: leave_drained_pod} do
    ref = make_ref()
    refusal = FakeUpstream.barrier_json_response(%{"error" => %{"code" => "server_error", "message" => "synthetic pre-output refusal"}}, status: 503, notify: self(), release_ref: ref)
    {setup, first_upstream, second_upstream} = stream_retry_setup(FakeUpstream.strict_sequence([refusal, stream_success_sse()]), stream_success_sse())
    session = "codex-session-drain-after-attempt-#{System.unique_integer([:positive])}"
    setup = Map.put(setup, :request_id, assert_session_order!(setup, session))
    parent = self()

    request =
      Task.async(fn ->
        Sandbox.allow(Repo, parent, self())
        post_turn(conn, setup, session)
      end)

    assert_receive {:fake_upstream_timeout_barrier, :before_headers, provider, ^ref}, @budget
    {_epoch, _entries} = DeferredStreamRegistry.begin_drain(name: stream_registry)
    releases = attach_pre_attempt_release_telemetry!()
    send(provider, {:fake_upstream_release_timeout, ref})
    drained = Task.await(request, @budget)

    assert drained.status == 503
    assert %{"error" => %{"code" => "owner_drained"}} = CodexPooler.JSON.decode!(drained.resp_body)
    assert {FakeUpstream.count(first_upstream), FakeUpstream.count(second_upstream)} == {1, 0}
    assert [%Request{status: "failed", response_status_code: 499, last_error_code: "owner_drained", usage_status: "usage_unknown"} = predecessor] = Repo.all(from(r in Request, where: r.pool_id == ^setup.pool.id))
    assert [%Attempt{id: attempt_id, status: "retryable_failed", pool_upstream_assignment_id: assignment_id}] = Repo.all(from(a in Attempt, where: a.request_id == ^predecessor.id))
    assert assignment_id == setup.assignment.id
    assert [{"release", ^attempt_id, "usage_unknown", nil}, {"reservation", nil, _usage, nil}] = ledger_entries(predecessor)
    refute_received {^releases, _measurements, %{phase: "turn_interrupted"}}
    assert %CodexTurn{first_visible_output_at: nil} = Repo.one!(from(t in CodexTurn, where: t.request_id == ^predecessor.id))

    # The client retries on a pod that is not draining; the native HTTP claim
    # walk steps over the drained request, which delivered nothing.
    leave_drained_pod.()
    resend = post_turn(conn, setup, session)
    assert resend.status == 200
    assert [%Request{status: "succeeded"} = served] = Repo.all(from(r in Request, where: r.pool_id == ^setup.pool.id and r.id != ^predecessor.id))
    assert served.correlation_id != predecessor.correlation_id
    refute Map.has_key?(served.request_metadata, "client_resend")
    refute Repo.exists?(from(l in RequestClientRetryLink, where: l.predecessor_request_id == ^predecessor.id or l.successor_request_id == ^served.id))
  end

  # The control: a drain that stops the first candidate, before any attempt,
  # still writes the pre-attempt shape and counts it. The request is held after
  # admission and before its reservation while the drain starts.
  test "a drain before the first candidate still releases before any attempt", %{conn: conn, stream_registry: stream_registry} do
    {setup, first_upstream, second_upstream} = stream_retry_setup(stream_success_sse(), stream_success_sse())
    session = "codex-session-drain-first-candidate-#{System.unique_integer([:positive])}"
    ref = make_ref()
    setup = Map.put(setup, :request_id, assert_session_order!(setup, session))
    parent = self()

    request =
      Task.async(fn ->
        Sandbox.allow(Repo, parent, self())
        Process.put({Service, :runtime_authorization_barrier}, {parent, ref, {:reservation_lock, :before}})
        post_turn(conn, setup, session)
      end)

    assert_receive {:runtime_authorization_barrier, ^ref, :reservation_lock, :before, request_pid}, @budget
    {_epoch, _entries} = DeferredStreamRegistry.begin_drain(name: stream_registry)
    releases = attach_pre_attempt_release_telemetry!()
    send(request_pid, {:runtime_authorization_release, ref})
    drained = Task.await(request, @budget)

    assert drained.status == 503
    assert %{"error" => %{"code" => "owner_drained"}} = CodexPooler.JSON.decode!(drained.resp_body)
    assert {FakeUpstream.count(first_upstream), FakeUpstream.count(second_upstream)} == {0, 0}
    assert [%Request{status: "failed", response_status_code: 499, last_error_code: "owner_drained", usage_status: "not_applicable"} = row] = Repo.all(from(r in Request, where: r.pool_id == ^setup.pool.id))
    refute Repo.exists?(from(a in Attempt, where: a.request_id == ^row.id))
    assert [{"release", nil, "not_applicable", "turn_interrupted"}, {"reservation", nil, _usage, nil}] = ledger_entries(row)
    assert_received {^releases, %{count: 1}, %{phase: "turn_interrupted", release_reason: "owner_drained"}}
  end

  # Resolve the real session before dispatch, then construct creation order
  # so its automatic rotation seed selects the refusal account. The request
  # id deliberately points at the competing account on that same ordering.
  defp assert_session_order!(setup, session_header) do
    {:ok, auth} = Access.authenticate_authorization_header(setup.authorization)
    payload = %{"model" => setup.model.exposed_model_id, "stream" => true}
    options = RequestOptions.build(%{api_key_policy: auth.api_key, session_header: session_header, session_header_source: "session-id"}, @path, payload)
    assert {:ok, session} = SessionContinuity.start_codex_session(auth, options)
    offset = :erlang.phash2(session.id, 2)
    first = if offset == 0, do: setup.assignment, else: setup.fallback_assignment
    last = if offset == 0, do: setup.fallback_assignment, else: setup.assignment
    now = DateTime.utc_now() |> DateTime.truncate(:microsecond)
    Repo.update!(Ecto.Changeset.change(first, created_at: DateTime.add(now, -2, :second)))
    Repo.update!(Ecto.Changeset.change(last, created_at: DateTime.add(now, -1, :second)))

    assert {:ok, %{request_options: options, candidates: candidates}} = PreDispatch.prepare(auth, @path, payload, options, setup.model)
    assert options.continuity.codex_session.id == session.id
    request_id = deterministic_rotation_seed(2, 1 - offset)
    input = %{auth: auth, model: setup.model, candidates: candidates, route_plan_input: %RoutePlanInput{correlation_id: request_id}, request_options: options}
    plan = BridgeRing.plan_route(input)
    assert plan.affinity.kind == "codex_session"
    assert plan.affinity.seed == session.id
    assert plan.selected_assignment_id == setup.assignment.id

    competing_options = RequestOptions.build(%{request_id: request_id}, @path, payload)
    assert BridgeRing.plan_route(%{input | request_options: competing_options}).selected_assignment_id == setup.fallback_assignment.id
    request_id
  end

  defp post_turn(conn, setup, session) do
    conn
    |> Phoenix.ConnTest.recycle()
    |> auth(setup)
    |> put_req_header("session-id", session)
    |> put_req_header("x-request-id", setup.request_id)
    |> post(@path, %{"model" => setup.model.exposed_model_id, "input" => native_text_input("drain after attempt turn"), "stream" => true, "client_metadata" => %{"session_id" => "client-metadata-session", "x-codex-turn-metadata" => CodexPooler.JSON.encode!(%{"turn_id" => @turn_id, "request_kind" => "turn"})}})
  end

  # The marker is emitted in the request's process after its commit; this
  # module is synchronous, so no other test emits one meanwhile.
  defp attach_pre_attempt_release_telemetry! do
    handler_id = {__MODULE__, System.unique_integer([:positive])}
    test_pid = self()
    on_exit(fn -> :telemetry.detach(handler_id) end)
    :ok = :telemetry.attach(handler_id, PreAttemptRelease.telemetry_event(), fn _event, measurements, metadata, _config -> send(test_pid, {handler_id, measurements, metadata}) end, nil)
    handler_id
  end

  defp ledger_entries(request),
    do: Enum.sort(Repo.all(from(l in LedgerEntry, where: l.request_id == ^request.id, select: {l.entry_kind, l.attempt_id, l.usage_status, fragment("?->>'pre_attempt_phase'", l.details)})))
end
