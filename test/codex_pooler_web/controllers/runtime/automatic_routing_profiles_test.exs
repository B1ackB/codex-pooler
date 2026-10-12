defmodule CodexPoolerWeb.Runtime.AutomaticRoutingProfilesTest do
  use CodexPoolerWeb.ConnCase, async: false

  import Ecto.Query
  import CodexPoolerWeb.Runtime.BackendCodexTestSupport
  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport, only: [model_serving_scope: 0, set_model_serving_mode!: 3]

  alias CodexPooler.Access
  alias CodexPooler.Accounting.{Attempt, LedgerEntry, Request}
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway.Payloads.RequestOptions
  alias CodexPooler.Gateway.Persistence.{BridgeAffinity, BridgeDemotion, CodexSession}
  alias CodexPooler.Gateway.Routing.{BridgeRing, RoutePlanInput}
  alias CodexPooler.Repo

  @response_path "/backend-api/codex/responses"
  @profiles ~w(bridge_ring deterministic_rotation least_recent_success quota_first)

  for profile <- @profiles, transport <- [:json, :sse], mode <- ["full", "lite"] do
    test "#{profile} #{transport} #{mode} dispatches the competing profile winner", do: independent(unquote(profile), unquote(transport), unquote(mode))
  end

  for profile <- @profiles, mode <- ["full", "lite"] do
    test "#{profile} #{mode} keeps the first serving session assignment across new request and cache keys", do: session_preference(unquote(profile), unquote(mode))
  end

  for profile <- @profiles do
    test "#{profile} preserves malformed cache diagnosis before profile policy", do: malformed_cache(unquote(profile))
  end

  for profile <- @profiles, safeguard <- [:ineligible, :demoted] do
    test "#{profile} excludes #{safeguard} preferred candidates before a one-slot ring", do: safeguard(unquote(profile), unquote(safeguard))
  end

  test "reused request id has durable affinity without inventing a session" do
    fixture = fixture!(:json, "full")
    {seed, cache, expected} = opposing_winners!(fixture, "bridge_ring")
    assert post_profile(fixture, :json, seed, cache).status == 200
    other = Enum.find(fixture.candidates, &(elem(&1, 0).id != expected)) |> elem(0)
    cache = cache_for!(fixture, other.id)
    assert post_profile(fixture, :json, seed, cache).status == 200
    assert_selected!(fixture, expected, 2)
    assert_accounting!(fixture, 2)
    assert sessions(fixture) == []
    [_first, second] = requests(fixture)
    assert second.request_metadata["routing"]["affinity_status"] == "hit"
    assert second.request_metadata["routing"]["routing_locality_unhonored_reason"] == "durable_affinity_hit"
  end

  test "idempotency identity outranks cache and survives changed request ids" do
    fixture = fixture!(:json, "full")
    use_routing_strategy!(fixture.pool, "bridge_ring", 2)
    idempotency_key = "synthetic-idempotency-#{System.unique_integer([:positive])}"
    opts = RequestOptions.build(%{request_id: "synthetic-first-request", idempotency_key: idempotency_key, request_method: "POST"}, @response_path, payload(fixture, :json, nil))
    base = BridgeRing.plan_route(%{auth: fixture.auth, model: fixture.model, candidates: fixture.candidates, route_plan_input: %RoutePlanInput{correlation_id: "synthetic-first-request"}, request_options: opts})
    expected = base.selected_assignment_id
    other = Enum.find(fixture.candidates, &(elem(&1, 0).id != expected)) |> elem(0)
    cache = cache_for!(fixture, other.id)

    for _ <- 1..2 do
      conn = build_conn() |> auth(fixture) |> put_req_header("content-type", "application/json") |> put_req_header("idempotency-key", idempotency_key) |> put_req_header("x-request-id", Ecto.UUID.generate()) |> post(@response_path, payload(fixture, :json, cache))
      assert conn.status == 200
    end

    assert_selected!(fixture, expected, 2)
    assert_accounting!(fixture, 2)
    assert sessions(fixture) == []
    [first, second] = requests(fixture)
    assert first.request_metadata["routing"]["affinity_kind"] == "idempotency_key"
    assert first.request_metadata["routing"]["routing_locality_unhonored_reason"] == "idempotency_key_continuity"
    assert second.request_metadata["routing"]["affinity_status"] == "hit"
  end

  test "generated request identities create separate affinities without a shared session" do
    fixture = fixture!(:json, "full")
    use_routing_strategy!(fixture.pool, "quota_first", 2)

    for _ <- 1..2 do
      conn = post_profile(fixture, :json, nil, "shared-synthetic-cache")
      assert conn.status == 200
    end

    assert sessions(fixture) == []
    assert [first, second] = requests(fixture)
    refute first.correlation_id == second.correlation_id
    assert Repo.aggregate(from(a in BridgeAffinity, where: a.pool_id == ^fixture.pool.id and a.affinity_kind == "request_correlation"), :count) == 2
    assert_accounting!(fixture, 2)
  end

  @tag :routing_profiles_curl
  test "curl crosses the actual Phoenix listener and both upstream/accounting boundaries" do
    for profile <- @profiles do
      fixture = fixture!(:json, "full")
      {request_id, cache, expected} = opposing_winners!(fixture, profile)
      {server, port} = start_public_endpoint_with_server!()
      ownership = capture_public_endpoint_ownership!(server)
      payload = payload(fixture, :json, cache)
      {headers, body} = curl_request!(port, fixture.authorization, request_id, payload)
      assert headers =~ "HTTP/1.1 200"
      assert headers =~ "content-type: application/json"
      assert CodexPooler.JSON.decode!(body)["status"] == "completed"
      assert_selected!(fixture, expected, 1)
      assert_accounting!(fixture, 1)
      assert sessions(fixture) == []
      [{request, attempt}] = request_attempts(fixture)
      assert attempt.pool_upstream_assignment_id == expected
      emit_receipt(%{scenario: "curl", profile: profile, port: port, status: 200, content_type: "application/json", request_status: request.status, attempt_status: attempt.status, attempts: 1, settlements: 1, upstream_calls: 1, eligible_candidates: 2, selection_matches: true, expected_assignment: expected, observed_assignment: attempt.pool_upstream_assignment_id, response_status: "completed"})
      ThousandIsland.stop(server)

      for {pid, monitor} <- ownership.processes do
        assert_receive {:DOWN, ^monitor, :process, ^pid, _}, 15_000
      end

      for {socket, monitor} <- ownership.sockets do
        assert_receive {:DOWN, ^monitor, :port, ^socket, _}, 15_000
      end

      for {_assignment, upstream} <- fixture.upstreams do
        monitor = Process.monitor(upstream.supervisor)
        FakeUpstream.stop(upstream)
        assert_receive {:DOWN, ^monitor, :process, _, _}, 15_000
      end

      emit_receipt(%{scenario: "curl_cleanup", profile: profile, listener_stopped: true, sockets_closed: true, fake_upstreams_stopped: 2})
    end
  end

  defp independent(profile, transport, mode) do
    fixture = fixture!(transport, mode)
    {request_id, cache, expected} = opposing_winners!(fixture, profile)
    conn = post_profile(fixture, transport, request_id, cache)
    assert conn.status == 200
    assert_response!(conn, transport)
    assert_selected!(fixture, expected, 1)
    assert_accounting!(fixture, 1)
    assert sessions(fixture) == []
    [{request, attempt}] = request_attempts(fixture)
    routing = request.request_metadata["routing"]
    assert routing["strategy"] == profile
    assert routing["routing_locality_eligible_candidate_count"] == 2
    assert routing["routing_locality_status"] == if(profile == "bridge_ring", do: "applied", else: "disabled")
    if profile != "bridge_ring", do: assert(routing["routing_locality_unhonored_reason"] == "routing_strategy_policy")
    assert attempt.response_metadata["routing"]["model_serving_mode"] == mode
    assert routing["affinity_kind"] == "request_correlation"
    assert routing["affinity_status"] == "miss"
    emit_receipt(%{scenario: "independent", profile: profile, transport: transport, mode: mode, eligible_candidates: 2, opposing_winners: true, selected_matches: true, requests: 1, attempts: 1, settlements: 1})
  end

  defp session_preference(profile, mode) do
    fixture = fixture!(:sse, mode)
    use_routing_strategy!(fixture.pool, profile, 2)
    session_id = Ecto.UUID.generate()
    assert post_profile(fixture, :sse, Ecto.UUID.generate(), "initial-cache", session_id).status == 200
    assert [session] = sessions(fixture)
    assert session.pool_upstream_assignment_id in Enum.map(fixture.candidates, &elem(&1, 0).id)
    pinned = session.pool_upstream_assignment_id
    other = Enum.find(fixture.candidates, &(elem(&1, 0).id != pinned))
    # Pose an independent request that really prefers the competitor. The
    # pinned session must still prevail on the subsequent controller request.
    for {assignment, identity} <- fixture.candidates do
      prime_routing_quota!(identity, %{used_percent: Decimal.new(if(assignment.id == pinned, do: 90, else: 1))})
    end

    seed = seed_for!(fixture, profile, elem(other, 0).id)
    cache = cache_for!(fixture, elem(other, 0).id)
    use_routing_strategy!(fixture.pool, profile, 2)
    assert plan(fixture, seed, cache).selected_assignment_id == elem(other, 0).id
    assert post_profile(fixture, :sse, seed, cache, session_id).status == 200
    assert_selected!(fixture, pinned, 2)
    assert_accounting!(fixture, 2)
    assert [persisted] = sessions(fixture)
    assert persisted.id == session.id
    assert persisted.pool_upstream_assignment_id == pinned
    [_first, last] = requests(fixture)
    assert last.request_metadata["routing"]["session_preference_kind"] == "pinned"
    assert last.request_metadata["routing"]["session_preference_status"] == "applied"
    emit_receipt(%{scenario: "session", profile: profile, mode: mode, competing_base_winner: true, stable_assignment: true, sessions: 1, requests: 2, attempts: 2, settlements: 2})
  end

  defp safeguard(profile, safeguard) do
    fixture = fixture!(:json, "full")
    {seed, cache, preferred} = opposing_winners!(fixture, profile)
    {assignment, identity} = Enum.find(fixture.candidates, &(elem(&1, 0).id == preferred))
    other = Enum.find(fixture.candidates, &(elem(&1, 0).id != preferred)) |> elem(0)
    assert plan(fixture, seed, cache).selected_assignment_id == preferred
    now = DateTime.utc_now() |> DateTime.truncate(:microsecond)

    case safeguard do
      :ineligible -> Repo.update!(Ecto.Changeset.change(assignment, eligibility_status: "ineligible"))
      :demoted -> Repo.insert!(%BridgeDemotion{pool_id: fixture.pool.id, api_key_id: fixture.api_key.id, model_identifier: fixture.model.exposed_model_id, pool_upstream_assignment_id: assignment.id, upstream_identity_id: identity.id, status: "active", reason_code: "provider_overloaded", demoted_until: DateTime.add(now, 60, :second), attempt_count: 1, metadata: %{}, created_at: now, updated_at: now})
    end

    use_routing_strategy!(fixture.pool, profile, 1)
    assert post_profile(fixture, :json, seed, cache).status == 200
    assert_selected!(fixture, other.id, 1)
    assert_accounting!(fixture, 1)
    [request] = requests(fixture)
    assert request.request_metadata["routing"]["bridge_ring_size"] == 1
    emit_receipt(%{scenario: "safeguard", profile: profile, safeguard: safeguard, precondition_competing_winner: true, ring_size: 1, rejected_candidate_calls: 0, attempts: 1, settlements: 1})
  end

  defp malformed_cache(profile) do
    fixture = fixture!(:json, "full")
    use_routing_strategy!(fixture.pool, profile, 2)
    assert post_profile(fixture, :json, Ecto.UUID.generate(), String.duplicate("x", 257)).status == 200
    [request] = requests(fixture)
    assert request.request_metadata["routing"]["routing_locality_status"] == "unavailable"
    assert request.request_metadata["routing"]["routing_locality_unhonored_reason"] == "prompt_cache_key_oversized"
    assert_accounting!(fixture, 1)
  end

  defp fixture!(transport, mode) do
    first = start_upstream(upstream_mode(transport))
    second = start_upstream(upstream_mode(transport))
    setup = gateway_setup(first)
    sibling = gateway_upstream(setup.pool, second, "synthetic-profile-token", [])
    prime_routing_quota!(setup.identity, %{used_percent: Decimal.new(80)})
    prime_routing_quota!(sibling.identity, %{used_percent: Decimal.new(10)})
    model = put_model_source_assignments!(setup.model, [setup.assignment, sibling.assignment])
    setup = %{setup | model: model}
    set_model_serving_mode!(model_serving_scope(), setup, mode)
    {:ok, auth} = Access.authenticate_authorization_header(setup.authorization)
    Map.merge(setup, %{auth: auth, candidates: [{setup.assignment, setup.identity}, {sibling.assignment, sibling.identity}], upstreams: %{setup.assignment.id => first, sibling.assignment.id => second}})
  end

  # Seed search is bounded and happens before any dispatch; it constructs the
  # adversarial fixture through the production planner, never retries traffic.
  defp opposing_winners!(fixture, profile) do
    seed = Ecto.UUID.generate()
    use_routing_strategy!(fixture.pool, profile, 2)
    base = plan(fixture, seed, nil).selected_assignment_id
    other = Enum.find(fixture.candidates, &(elem(&1, 0).id != base)) |> elem(0) |> Map.fetch!(:id)
    cache = cache_for!(fixture, other)
    assert plan(fixture, seed, cache).selected_assignment_id == other
    use_routing_strategy!(fixture.pool, profile, 2)
    expected = if profile == "bridge_ring", do: other, else: base
    {seed, cache, expected}
  end

  defp cache_for!(fixture, desired) do
    use_routing_strategy!(fixture.pool, "bridge_ring", 2)

    Enum.find_value(1..200, fn i ->
      key = "synthetic-cache-#{i}"
      if plan(fixture, "cache-precondition", key).selected_assignment_id == desired, do: key
    end) || flunk("no opposing cache winner found before dispatch")
  end

  defp seed_for!(fixture, profile, desired) do
    use_routing_strategy!(fixture.pool, profile, 2)

    Enum.find_value(1..200, fn i ->
      seed = "synthetic-request-#{i}"
      if plan(fixture, seed, nil).selected_assignment_id == desired, do: seed
    end) || flunk("no competing independent winner found before dispatch")
  end

  defp plan(fixture, seed, cache) do
    opts = RequestOptions.build(%{request_id: seed, request_method: "POST"}, @response_path, payload(fixture, :json, cache))
    BridgeRing.plan_route(%{auth: fixture.auth, model: fixture.model, candidates: fixture.candidates, route_plan_input: %RoutePlanInput{correlation_id: seed}, request_options: opts})
  end

  defp post_profile(fixture, transport, request_id, cache, session_id \\ nil) do
    conn = build_conn() |> auth(fixture) |> put_req_header("content-type", "application/json")
    conn = if request_id, do: put_req_header(conn, "x-request-id", request_id), else: conn
    conn = if session_id, do: put_req_header(conn, "session-id", session_id) |> put_req_header("thread-id", session_id), else: conn
    post(conn, @response_path, payload(fixture, transport, cache))
  end

  defp payload(fixture, transport, cache) do
    %{"model" => fixture.model.exposed_model_id, "input" => [], "stream" => transport == :sse}
    |> then(fn body -> if cache, do: Map.put(body, "prompt_cache_key", cache), else: body end)
  end

  defp upstream_mode(:json), do: FakeUpstream.json_response(completion())
  defp upstream_mode(:sse), do: FakeUpstream.sse_stream([%{"type" => "response.completed", "response" => completion()}], done: false)
  defp completion, do: %{"id" => "resp_synthetic_routing", "object" => "response", "status" => "completed", "output" => [], "usage" => %{"input_tokens" => 2, "output_tokens" => 1, "total_tokens" => 3}}
  defp assert_response!(conn, :json), do: assert(json_response(conn, 200)["status"] == "completed")
  defp assert_response!(conn, :sse), do: assert(response(conn, 200) =~ "response.completed")
  defp requests(fixture), do: Repo.all(from(r in Request, where: r.pool_id == ^fixture.pool.id, order_by: [asc: r.admitted_at]))
  defp sessions(fixture), do: Repo.all(from(s in CodexSession, where: s.pool_id == ^fixture.pool.id))
  defp request_attempts(fixture), do: Repo.all(from(r in Request, join: a in Attempt, on: a.request_id == r.id, where: r.pool_id == ^fixture.pool.id, select: {r, a}, order_by: [asc: r.admitted_at]))

  defp assert_selected!(fixture, expected, count) do
    for {assignment, upstream} <- fixture.upstreams do
      assert FakeUpstream.count(upstream) == if(assignment == expected, do: count, else: 0)
    end

    assert Enum.all?(request_attempts(fixture), fn {_r, a} -> a.pool_upstream_assignment_id == expected end)
  end

  defp assert_accounting!(fixture, count) do
    rows = request_attempts(fixture)
    assert length(requests(fixture)) == count
    assert length(rows) == count

    for {request, attempt} <- rows do
      assert request.status == "succeeded"
      assert attempt.status == "succeeded"
      assert [settlement] = Repo.all(from(l in LedgerEntry, where: l.request_id == ^request.id and l.entry_kind == "settlement"))
      assert settlement.attempt_id == attempt.id
    end
  end

  defp curl_request!(port, authorization, request_id, body) do
    root = Path.join(System.tmp_dir!(), "automatic-routing-curl-#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf!(root) end)
    File.mkdir!(root)
    File.chmod!(root, 0o700)
    config = Path.join(root, "curl.conf")
    data = Path.join(root, "request.json")
    File.write!(data, CodexPooler.JSON.encode!(body))
    File.chmod!(data, 0o600)
    File.write!(config, "url = \"http://127.0.0.1:#{port}#{@response_path}\"\nrequest = \"POST\"\nheader = \"authorization: #{authorization}\"\nheader = \"content-type: application/json\"\nheader = \"x-request-id: #{request_id}\"\ndata-binary = \"@#{data}\"\n")
    File.chmod!(config, 0o600)
    {output, rc} = System.cmd("curl", ["-i", "--silent", "--show-error", "--max-time", "15", "--config", config], stderr_to_stdout: true)
    File.rm_rf!(root)
    refute File.exists?(root)
    assert rc == 0
    assert [headers, response] = String.split(output, "\r\n\r\n", parts: 2)
    emit_receipt(%{scenario: "curl_process", exit_code: rc, credentials_removed: true, command: "curl -i --silent --show-error --max-time 15 --config <owned-0600-config>"})
    {headers, response}
  end

  defp emit_receipt(receipt) do
    if System.get_env("CODEX_POOLER_ROUTING_RECEIPTS") == "1", do: IO.puts("ROUTING_RECEIPT " <> CodexPooler.JSON.encode!(receipt))
  end
end
