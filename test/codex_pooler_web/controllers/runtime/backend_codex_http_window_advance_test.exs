defmodule CodexPoolerWeb.Runtime.BackendCodexHttpWindowAdvanceTest do
  # The released Codex client (0.159.0, `codex-rs/core/src/session/mod.rs`
  # `current_window/0`) names its context window `<thread>:<n>` in
  # `x-codex-window-id` and in the turn metadata, and moves to `<thread>:<n + 1>`
  # after every compaction it completes, local (`compact.rs`) or remote
  # (`compact_remote_v2.rs`). Over HTTP the request that resumes the turn after
  # the compaction keeps `session-id`, `thread-id`, the turn ids and everything
  # else, and names only the next window, which no session knew: it opened a
  # second Pooler session for the same thread, while the websocket keeps the
  # thread in the socket's session (findings#206 P115). The resume now continues
  # the live session of its thread's previous window, found through that
  # window's alias under the request's own Pool and API key (findings#289).
  #
  # Requests: the released client's HTTP shape (a JSON body carrying the
  # canonical turn document, `stream: true`, the document echoed as
  # `x-codex-turn-metadata`, the thread as `session-id` and `thread-id`, the
  # current window as `x-codex-window-id`, the Lite marker as a header), with
  # synthetic text and ids. Topology: the real HTTP route, one node, owner
  # forwarding not involved (HTTP is never forwarded), FakeUpstream; a second
  # replica is the test-only owner override naming another node.
  use CodexPoolerWeb.ConnCase, async: false

  import Ecto.Query
  import CodexPooler.PoolerFixtures, only: [active_api_key_fixture: 1]
  import CodexPoolerWeb.Runtime.BackendCodexTestSupport
  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport, only: [model_serving_scope: 0, set_model_serving_mode!: 3]

  alias CodexPooler.Access
  alias CodexPooler.Accounting.Request
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway.Payloads.RequestOptions
  alias CodexPooler.Gateway.Persistence.{BridgeOwnerLease, BridgeSessionAlias, CodexSession}
  alias CodexPooler.Gateway.Routing.{BridgeRing, RoutePlanInput}
  alias CodexPooler.Repo
  alias CodexPoolerWeb.GatewayControllerHelpers

  @moduletag capture_log: true

  @metadata_key "x-codex-turn-metadata"
  @lite_header "x-openai-internal-codex-responses-lite"
  @compaction_prompt "You are performing a CONTEXT CHECKPOINT COMPACTION. Create a handoff summary (synthetic)."

  for mode <- ["full", "lite"], compaction <- [:local, :remote] do
    @mode mode
    @compaction compaction

    test "a #{mode} #{compaction} compaction's resume stays in the thread's session up to the third window", %{conn: conn} do
      upstream = start_upstream(FakeUpstream.strict_sequence(upstream_responses(window_advance_requests(@compaction))))
      setup = setup!(upstream, @mode)
      ids = ids()

      for {{kind, input, window}, step} <- Enum.with_index(window_advance_requests(@compaction), 1) do
        conn = post_window(conn, setup, ids, kind, input, window)
        assert conn.status == 200, "step #{step} (#{kind} on window #{window}) answered #{conn.status}: #{conn.resp_body}"
      end

      assert FakeUpstream.count(upstream) == 5
      assert [session] = pool_sessions(setup)
      assert session.session_key == window_session_key(ids.thread, 0)

      requests = pool_requests(setup)
      assert length(requests) == 5
      assert Enum.all?(requests, &(&1.status == "succeeded"))
      assert Enum.map(requests, & &1.request_metadata["codex_session_id"]) == List.duplicate(session.id, 5)

      for window <- 0..2, do: assert(window_alias_session_ids(setup, "#{ids.thread}:#{window}") == [session.id])

      # Nothing the provider receives changes: the client's own session and
      # window headers go upstream as they came.
      for {captured, {_kind, _input, window}} <- Enum.zip(FakeUpstream.requests(upstream), window_advance_requests(@compaction)) do
        headers = Map.new(captured.headers)
        assert headers["session-id"] == ids.thread
        assert headers["x-codex-window-id"] == "#{ids.thread}:#{window}"
      end
    end
  end

  test "the resume landing on another replica continues the session and leaves its owner alone", %{conn: conn} do
    upstream = start_upstream(FakeUpstream.strict_sequence([turn_sse("resp_replica_open"), turn_sse("resp_replica_summary"), turn_sse("resp_replica_resume")]))
    setup = setup!(upstream, "full")
    ids = ids()
    [{_kind, open, _window}, summary, resume] = window_advance_requests(:local) |> Enum.take(3)

    assert response(post_window(conn, setup, ids, "turn", open, 0), 200)
    {kind, input, window} = summary
    assert response(post_window(conn, setup, ids, kind, input, window), 200)
    assert [session] = pool_sessions(setup)
    lease = active_lease!(session.id)

    Process.put({GatewayControllerHelpers, :owner_liveness_test_options}, %{owner_instance_id: "sample-replica-b@remote"})
    {kind, input, window} = resume
    assert response(post_window(conn, setup, ids, kind, input, window), 200)
    Process.delete({GatewayControllerHelpers, :owner_liveness_test_options})

    assert Enum.map(pool_sessions(setup), & &1.id) == [session.id]
    assert Enum.map(pool_requests(setup), & &1.request_metadata["codex_session_id"]) == List.duplicate(session.id, 3)
    assert active_lease!(session.id).lease_token == lease.lease_token
    assert active_lease!(session.id).owner_instance_id == lease.owner_instance_id
    assert Repo.get!(CodexSession, session.id).owner_instance_id == session.owner_instance_id
  end

  @tag :cross_key_window_session
  test "another key of the Pool or another Pool sending the same resume opens its own session", %{conn: conn} do
    upstream = start_upstream(FakeUpstream.strict_sequence(Enum.map(1..4, &turn_sse("resp_cross_key_#{&1}"))))
    setup = setup!(upstream, "full")
    other_upstream = start_upstream(FakeUpstream.strict_sequence([turn_sse("resp_cross_pool")]))
    other_pool = setup!(other_upstream, "full")
    same_pool_key = Map.merge(active_api_key_fixture(setup.pool), Map.take(setup, [:identity, :assignment, :model, :pricing]))
    ids = ids()
    [{_kind, open, _window}, _summary, {kind, resume, window} | _rest] = window_advance_requests(:remote)

    assert response(post_window(conn, setup, ids, "turn", open, 0), 200)
    assert [session] = pool_sessions(setup)

    for other <- [same_pool_key, other_pool] do
      assert response(post_window(conn, other, ids, kind, resume, window), 200)
      assert [other_request] = Repo.all(from(r in Request, where: r.api_key_id == ^other.api_key.id))
      refute other_request.request_metadata["codex_session_id"] == session.id
      other_session = Repo.get!(CodexSession, other_request.request_metadata["codex_session_id"])
      assert other_session.api_key_id == other.api_key.id
      assert other_session.session_key == window_session_key(ids.thread, window)
    end

    assert window_alias_session_ids(setup, "#{ids.thread}:#{window}") == []

    assert response(post_window(conn, setup, ids, kind, resume, window), 200)
    assert window_alias_session_ids(setup, "#{ids.thread}:#{window}") == [session.id]
  end

  test "a resume whose previous window's session has lapsed opens its own session and closes the lapsed one", %{conn: conn} do
    upstream = start_upstream(FakeUpstream.strict_sequence([turn_sse("resp_lapsed_open"), turn_sse("resp_lapsed_resume")]))
    setup = setup!(upstream, "full")
    ids = ids()
    [{_kind, open, _window}, _summary, {kind, resume, window} | _rest] = window_advance_requests(:remote)

    assert response(post_window(conn, setup, ids, "turn", open, 0), 200)
    assert [session] = pool_sessions(setup)
    expire_owner_lease!(session.id)

    assert response(post_window(conn, setup, ids, kind, resume, window), 200)
    assert [_open, resumed] = pool_requests(setup)
    refute resumed.request_metadata["codex_session_id"] == session.id
    assert resumed.request_metadata["codex_session_key"] == window_session_key(ids.thread, 1)
    assert %CodexSession{status: "closed"} = Repo.get!(CodexSession, session.id)
    assert window_alias_session_ids(setup, "#{ids.thread}:0") == []
  end

  # The thread's session lapses after a compaction (no request for longer than
  # the owner lease), and the next turn names the window the session reached
  # only through its alias: the replacement session prefers the assignment the
  # thread served on, as a lapse on the same window does. Least recent
  # success ranks the unused competing account ahead of the served account,
  # regardless of the random replacement session id. Only recreation's
  # preference can override that independently observed order.
  test "after the thread's session lapses, the next window's new session prefers its assignment", %{conn: conn} do
    upstream = start_upstream(FakeUpstream.strict_sequence(upstream_responses(Enum.take(window_advance_requests(:remote), 3)) ++ [turn_sse("resp_lapse_next_turn")]))
    other_upstream = start_upstream(FakeUpstream.json_response(%{"id" => "resp_lapse_other"}))
    setup = setup!(upstream, "full")
    other = gateway_upstream(setup.pool, other_upstream, "synthetic-lapse-other-token", compact?: true)
    prime_routing_quota!(other.identity)
    ids = ids()

    for {kind, input, window} <- Enum.take(window_advance_requests(:remote), 3) do
      assert response(post_window(conn, setup, ids, kind, input, window, prompt_cache_key: false), 200)
    end

    assert [session] = pool_sessions(setup)
    assert session.pool_upstream_assignment_id == setup.assignment.id
    expire_owner_lease!(session.id)

    setup = %{setup | model: put_model_source_assignments!(setup.model, [setup.assignment, other.assignment])}
    use_routing_strategy!(setup.pool, "least_recent_success", 2)
    assert_unused_candidate_wins!(setup, other)
    next_turn = %{ids | turn: "turn-" <> unique_suffix()}
    input = history() ++ [compaction_item("one"), assistant("done"), user("next task")]

    assert response(post_window(conn, setup, next_turn, "turn", input, 1, prompt_cache_key: false, request_id: "new-window-request"), 200)

    assert FakeUpstream.count(upstream) == 4
    assert FakeUpstream.count(other_upstream) == 0
    assert %CodexSession{status: "closed"} = Repo.get!(CodexSession, session.id)
    assert [_old, replacement] = Enum.sort_by(pool_sessions(setup), & &1.created_at, DateTime)
    assert replacement.session_key == window_session_key(ids.thread, 1)
    assert %Request{request_metadata: %{"codex_session_id" => replacement_id, "routing" => routing}} = List.last(pool_requests(setup))
    assert replacement_id == replacement.id
    assert routing["affinity_kind"] == "codex_session"
    assert routing["strategy"] == "least_recent_success"
    assert {routing["session_preference_kind"], routing["session_preference_status"]} == {"recreated", "applied"}
  end

  # A file whose affinity names another assignment than the thread session's
  # is refused before routing, as the same request on the previous window is,
  # and not left to routing as a pin no candidate can meet.
  test "a resume referencing a file held by another assignment gets the previous window's file conflict", %{conn: conn} do
    upstream = start_upstream(FakeUpstream.strict_sequence([turn_sse("resp_file_open")]))
    file_upstream = start_upstream(FakeUpstream.json_response(%{"id" => "resp_file_holder_unused"}))
    setup = setup!(upstream, "full")
    holder = gateway_upstream(setup.pool, file_upstream, "synthetic-file-holder-token", compact?: true)
    prime_routing_quota!(holder.identity)
    ids = ids()
    [{_kind, open, _window} | _rest] = window_advance_requests(:remote)

    assert response(post_window(conn, setup, ids, "turn", open, 0), 200)
    assert [session] = pool_sessions(setup)
    assert session.pool_upstream_assignment_id == setup.assignment.id

    setup = %{setup | model: put_model_source_assignments!(setup.model, [setup.assignment, holder.assignment])}

    file =
      response_affinity_file_fixture(setup, holder.assignment, holder.identity,
        file_id: "file-window-advance-#{System.unique_integer([:positive])}",
        status: "uploaded",
        finalize_status: "succeeded"
      )

    with_file = open ++ [%{"type" => "message", "role" => "user", "content" => [%{"type" => "input_file", "file_id" => file.file_id}]}]

    for window <- [0, 1] do
      conn = post_window(conn, setup, ids, "turn", with_file, window)

      assert %{"error" => %{"code" => "file_assignment_conflict", "param" => "file_id", "message" => "referenced file conflicts with existing session routing assignment"}} =
               json_response(conn, 409)
    end

    assert FakeUpstream.count(upstream) == 1
    assert FakeUpstream.count(file_upstream) == 0
  end

  defp setup!(upstream, mode) do
    setup = gateway_setup(upstream, compact?: true)
    _revision = set_model_serving_mode!(model_serving_scope(), setup, mode)
    Map.put(setup, :serving_mode, mode)
  end

  defp ids, do: %{turn: "turn-" <> unique_suffix(), thread: Ecto.UUID.generate()}

  # Turn, compaction and resume on window 0 and 1, then the resume on window 2.
  # A local compaction's resume is the turn's retained user messages plus the
  # summary as one more user message, so the second resume has no more user
  # messages than the first: its window tells it apart (findings#282).
  defp window_advance_requests(:local) do
    open = history()

    [
      {"turn", open, 0},
      {"compaction", open ++ [assistant("working"), user(@compaction_prompt)], 0},
      {"turn", open ++ [user("synthetic summary one")], 1},
      {"compaction", open ++ [user("synthetic summary one"), assistant("more work"), user(@compaction_prompt)], 1},
      {"turn", open ++ [user("synthetic summary two")], 2}
    ]
  end

  defp window_advance_requests(:remote) do
    open = history()

    [
      {"turn", open, 0},
      {"compaction", open ++ [assistant("working"), %{"type" => "compaction_trigger"}], 0},
      {"turn", open ++ [compaction_item("one")], 1},
      {"compaction", open ++ [compaction_item("one"), assistant("more work"), %{"type" => "compaction_trigger"}], 1},
      {"turn", open ++ [compaction_item("two")], 2}
    ]
  end

  defp post_window(conn, setup, ids, kind, input, window, opts \\ []) do
    document =
      %{
        "session_id" => ids.thread,
        "thread_id" => ids.thread,
        "turn_id" => ids.turn,
        "root_turn_id" => ids.turn,
        "window_id" => "#{ids.thread}:#{window}",
        "window_number" => window,
        "request_kind" => kind
      }
      |> then(&if kind == "compaction", do: Map.put(&1, "compaction", compaction_metadata(input)), else: &1)
      |> CodexPooler.JSON.encode!()

    payload =
      %{
        "model" => setup.model.exposed_model_id,
        "input" => input,
        "stream" => true,
        "client_metadata" => %{@metadata_key => document}
      }
      |> then(&if Keyword.get(opts, :prompt_cache_key, true), do: Map.put(&1, "prompt_cache_key", ids.thread), else: &1)

    conn
    |> recycle()
    |> auth(setup)
    |> put_req_header("content-type", "application/json")
    |> put_req_header("accept", "text/event-stream")
    |> put_req_header("session-id", ids.thread)
    |> put_req_header("thread-id", ids.thread)
    |> put_req_header("x-codex-window-id", "#{ids.thread}:#{window}")
    |> put_req_header(@metadata_key, document)
    |> put_req_header("originator", "codex_exec")
    |> then(&if request_id = Keyword.get(opts, :request_id), do: put_req_header(&1, "x-request-id", request_id), else: &1)
    |> then(&if Map.get(setup, :serving_mode) == "lite", do: put_req_header(&1, @lite_header, "true"), else: &1)
    |> post("/backend-api/codex/responses", CodexPooler.JSON.encode!(payload))
  end

  defp compaction_metadata(input) do
    implementation = if Enum.any?(input, &match?(%{"type" => "compaction_trigger"}, &1)), do: "responses_compaction_v2", else: "responses"
    %{"trigger" => "auto", "reason" => "context_limit", "implementation" => implementation, "phase" => "mid_turn", "strategy" => "memento"}
  end

  # A remote compaction is answered with the compaction stream the collector
  # reads; every other request with a plain completed stream.
  defp upstream_responses(requests) do
    for {{kind, input, _window}, step} <- Enum.with_index(requests, 1) do
      if kind == "compaction" and Enum.any?(input, &match?(%{"type" => "compaction_trigger"}, &1)),
        do: compaction_sse("resp_window_advance_#{step}"),
        else: turn_sse("resp_window_advance_#{step}")
    end
  end

  defp compaction_sse(id) do
    FakeUpstream.compaction_stream(%{
      "id" => id,
      "output" => [compaction_item(id)],
      "usage" => %{"input_tokens" => 5, "output_tokens" => 2, "total_tokens" => 7}
    })
  end

  defp turn_sse(id) do
    FakeUpstream.sse_stream([
      {"response.completed", %{"type" => "response.completed", "response" => %{"id" => id, "usage" => %{"input_tokens" => 4, "output_tokens" => 3, "total_tokens" => 7}}}}
    ])
  end

  defp history, do: [developer("synthetic developer instructions"), user("synthetic environment context"), user("window advance sample: work, then answer")]

  defp developer(text), do: %{"type" => "message", "role" => "developer", "content" => [%{"type" => "input_text", "text" => text}]}
  defp user(text), do: %{"type" => "message", "role" => "user", "content" => [%{"type" => "input_text", "text" => text}]}
  defp assistant(text), do: %{"type" => "message", "role" => "assistant", "content" => [%{"type" => "output_text", "text" => text}]}
  defp compaction_item(label), do: %{"type" => "compaction", "encrypted_content" => "synthetic-compaction-" <> label}

  defp window_session_key(thread, window), do: "x-codex-window-id:" <> Base.encode16(:crypto.hash(:sha256, "#{thread}:#{window}"), case: :lower)

  defp window_alias_session_ids(setup, window) do
    Repo.all(
      from alias_record in BridgeSessionAlias,
        where:
          alias_record.pool_id == ^setup.pool.id and alias_record.api_key_id == ^setup.api_key.id and alias_record.alias_kind == "session_header" and
            alias_record.alias_hash == ^:crypto.hash(:sha256, window) and alias_record.status == "active",
        select: alias_record.codex_session_id
    )
  end

  defp pool_sessions(setup), do: Repo.all(from(s in CodexSession, where: s.pool_id == ^setup.pool.id and s.api_key_id == ^setup.api_key.id))

  defp pool_requests(setup), do: Repo.all(from(r in Request, where: r.pool_id == ^setup.pool.id and r.api_key_id == ^setup.api_key.id, order_by: r.admitted_at))

  defp active_lease!(session_id), do: Repo.one!(from(l in BridgeOwnerLease, where: l.codex_session_id == ^session_id and l.status == "active"))

  defp assert_unused_candidate_wins!(setup, other) do
    successes = CodexPooler.Accounting.latest_success_by_assignment_ids([setup.assignment.id, other.assignment.id])
    assert %DateTime{} = Map.fetch!(successes, setup.assignment.id)
    refute Map.has_key?(successes, other.assignment.id)
    {:ok, auth} = Access.authenticate_authorization_header(setup.authorization)

    options =
      RequestOptions.build(%{request_id: "window-base-order"}, "/backend-api/codex/responses", %{"stream" => true})

    plan =
      BridgeRing.plan_route(%{
        auth: auth,
        model: setup.model,
        candidates: [{setup.assignment, setup.identity}, {other.assignment, other.identity}],
        route_plan_input: %RoutePlanInput{correlation_id: "window-base-order"},
        request_options: options
      })

    assert plan.affinity.kind == "request_correlation"
    assert plan.selected_assignment_id == other.assignment.id
    refute plan.selected_assignment_id == setup.assignment.id
  end

  defp expire_owner_lease!(session_id) do
    expired_at = DateTime.utc_now() |> DateTime.add(-1, :second) |> DateTime.truncate(:microsecond)
    Repo.update_all(from(s in CodexSession, where: s.id == ^session_id), set: [owner_lease_expires_at: expired_at])
    Repo.update_all(from(l in BridgeOwnerLease, where: l.codex_session_id == ^session_id and l.status == "active"), set: [expires_at: expired_at])
  end
end
