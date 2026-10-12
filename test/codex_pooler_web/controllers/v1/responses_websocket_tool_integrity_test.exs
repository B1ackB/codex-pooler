defmodule CodexPoolerWeb.V1.ResponsesWebsocketToolIntegrityTest do
  use CodexPoolerWeb.ConnCase, async: false

  import Ecto.Query

  import CodexPoolerWeb.Runtime.BackendCodexTestSupport,
    only: [
      await_public_websocket_upgrade: 2,
      auth: 2,
      gateway_setup: 1,
      mint_websocket_new!: 4,
      public_websocket_receive_text!: 3,
      public_websocket_send_text!: 4,
      start_public_endpoint!: 0,
      start_upstream: 1
    ]

  alias CodexPooler.Accounting.{Attempt, LedgerEntry, Request}
  alias CodexPooler.Events
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway.Persistence.{BridgeSessionAlias, CodexTurn}
  alias CodexPooler.Repo
  alias CodexPooler.TestDiagnostics

  import CodexPoolerWeb.Runtime.BackendCodexWebsocketOwnerForwardingSupport,
    only: [enter_peer_owner_topology!: 0, start_peer_session_owner!: 2]

  @timeout 15_000

  # A comprehension expands and compiles a test's body once per generated test, so a loop that generates more than a few tests keeps
  # the scenario in a private function below it and each generated test is one call.
  for topology <- [:direct, :local_owner, :remote_owner], kind <- ["function_call", "custom_tool_call"], defect <- [:missing_done, :missing_index, :string_index, :wrong_index, :wrong_id, :one_pending], terminal_shape <- [:typed, :legacy] do
    if topology == :remote_owner, do: @tag(slow: "actual remote owner on a distinct BEAM node")
    @tag topology: topology
    @tag kind: kind
    @tag defect: defect
    @tag terminal_shape: terminal_shape
    test "#{topology} rejects #{defect} #{kind} #{terminal_shape} and serves a fresh same-socket turn", %{topology: topology, kind: kind, defect: defect, terminal_shape: terminal_shape} do
      assert_rejection_and_fresh_turn!(topology, kind, defect, terminal_shape)
    end
  end

  # Reason: the body of a generated test; its branches select the matrix case.
  # credo:disable-for-next-line Credo.Check.Refactor.CyclomaticComplexity
  defp assert_rejection_and_fresh_turn!(topology, kind, defect, terminal_shape) do
    CodexPooler.TestAppEnv.restore_on_exit(:websocket_owner_forwarding_enabled)
    Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, topology != :direct)
    if topology == :remote_owner, do: enter_peer_owner_topology!()
    # provenance: synthetic_adversarial; healthy opener and recovery are invented controls.
    upstream =
      start_upstream(
        FakeUpstream.strict_sequence([
          FakeUpstream.websocket_text_frames([CodexPooler.JSON.encode!(put_in(healthy_terminal(), ["response", "id"], "resp_prior_tool_fixture"))]),
          FakeUpstream.websocket_text_frames(Enum.map(source_events(kind, defect, terminal_shape), &CodexPooler.JSON.encode!/1)),
          FakeUpstream.websocket_text_frames([CodexPooler.JSON.encode!(terminal_shape(healthy_terminal(), terminal_shape))])
        ])
      )

    setup = gateway_setup(upstream)
    assert :ok = Events.subscribe_pool(setup.pool)
    registration = {__MODULE__, make_ref()}
    registration_gate = :atomics.new(1, [])
    :atomics.put(registration_gate, 1, 1)
    :ok = :telemetry.attach(registration, [:codex_pooler, :repo, :query], &__MODULE__.registration_query/4, {self(), registration, registration_gate})
    on_exit(fn -> :telemetry.detach(registration) end)
    turn_state = "tool-integrity-#{System.unique_integer([:positive])}"
    peer = if topology == :remote_owner, do: start_peer_session_owner!(setup, %{accepted_turn_state: turn_state})
    if peer, do: assert(peer.node != node() and node(peer.owner_pid) == peer.node)
    port = start_public_endpoint!()
    {conn, websocket, ref} = connect!(port, setup, turn_state)

    try do
      payload = %{"type" => "response.create", "model" => setup.model.exposed_model_id, "input" => [%{"type" => "message", "role" => "user", "content" => [%{"type" => "input_text", "text" => "synthetic tool integrity turn"}]}], "stream" => true}
      {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, CodexPooler.JSON.encode!(payload))
      {conn, websocket, ["response.completed"], _prior} = receive_terminal!(conn, websocket, ref, [])
      assert await_request_finalized!(setup, []) == "succeeded"
      await_registration!(registration)
      prior_alias = Repo.one!(response_alias_query(setup, "resp_prior_tool_fixture"))
      assert prior_alias.status == "active"
      prior_attempt = Repo.one!(from a in Attempt, join: r in Request, on: r.id == a.request_id, where: r.pool_id == ^setup.pool.id)
      assert %{"lifecycle_id" => prior_lifecycle, "generation" => prior_generation} = prior_attempt.response_metadata["upstream_websocket_connection"]
      {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, CodexPooler.JSON.encode!(payload))
      {conn, websocket, types, terminal} = receive_terminal!(conn, websocket, ref, [])
      status = await_request_finalized!(setup, [prior_attempt.request_id])
      observed = Repo.one!(from r in Request, where: r.pool_id == ^setup.pool.id and r.id != ^prior_attempt.request_id)
      observed_settlement = Repo.one!(from l in LedgerEntry, where: l.request_id == ^observed.id and l.entry_kind == "settlement")
      TestDiagnostics.puts("F4_LEGACY topology=#{topology} kind=#{kind} defect=#{defect} shape=#{terminal_shape} terminal=#{List.last(types)} status=#{status} request_id=#{observed.id} input=#{observed_settlement.input_tokens} output=#{observed_settlement.output_tokens} cache_write=#{observed_settlement.cache_write_tokens} provider_requests=#{length(FakeUpstream.requests(upstream))} invalid_alias=#{Repo.exists?(response_alias_query(setup, "resp_invalid_tool_fixture"))}")
      assert status == "failed"
      assert List.last(types) == "error"
      assert terminal["error"]["code"] == "server_error"
      refute "response.completed" in types
      if defect == :missing_done, do: refute("response.output_item.done" in types)
      assert "response.output_item.added" in types
      assert Enum.any?(types, &String.ends_with?(&1, ".delta"))
      assert [request] = Repo.all(from r in Request, where: r.pool_id == ^setup.pool.id and r.status == "failed")
      assert request.status == "failed"
      assert request.last_error_code == "upstream_stream_error"
      assert [attempt] = Repo.all(from a in Attempt, where: a.request_id == ^request.id)
      assert attempt.status == "failed"
      assert attempt.network_error_code == "upstream_stream_error"
      assert [settlement] = Repo.all(from l in LedgerEntry, where: l.request_id == ^request.id and l.entry_kind == "settlement")
      assert settlement.input_tokens == 5457
      assert settlement.output_tokens == 2
      assert settlement.cache_write_tokens == 5454
      assert Repo.aggregate(from(l in LedgerEntry, where: l.request_id == ^request.id and l.entry_kind == "reservation"), :count) == 1
      assert Repo.aggregate(from(l in LedgerEntry, where: l.request_id == ^request.id and l.entry_kind == "release"), :count) == 1
      assert request.usage_status == "usage_known"
      assert [%{status: "failed"}] = Repo.all(from t in CodexTurn, where: t.request_id == ^request.id)
      refute Repo.exists?(response_alias_query(setup, "resp_invalid_tool_fixture"))
      assert Repo.get!(BridgeSessionAlias, prior_alias.id) == prior_alias
      assert %{"lifecycle_id" => ^prior_lifecycle, "generation" => ^prior_generation} = attempt.response_metadata["upstream_websocket_connection"]
      assert length(FakeUpstream.requests(upstream)) == 2
      assert Enum.all?(FakeUpstream.requests(upstream), &(&1.method == "WEBSOCKET"))
      anchored = payload |> Map.put("previous_response_id", "resp_invalid_tool_fixture") |> Map.put("input", [%{"type" => "function_call_output", "call_id" => "call_fixture", "output" => "synthetic result"}])
      {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, CodexPooler.JSON.encode!(anchored))
      {conn, websocket, ["error"], refused} = receive_terminal!(conn, websocket, ref, [])
      assert refused["error"]["code"] == "previous_response_not_found"
      assert length(FakeUpstream.requests(upstream)) == 2
      prior_anchored = Map.put(anchored, "previous_response_id", "resp_prior_tool_fixture")
      {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, CodexPooler.JSON.encode!(prior_anchored))
      {conn, websocket, retired_types, retired} = receive_terminal!(conn, websocket, ref, [])
      assert retired_types == ["error"]
      assert retired["error"]["code"] == "previous_response_not_found"
      assert length(FakeUpstream.requests(upstream)) == 2
      prior_after_lookup = Repo.get!(BridgeSessionAlias, prior_alias.id)
      assert Map.drop(Map.from_struct(prior_after_lookup), [:last_seen_at, :expires_at, :updated_at]) == Map.drop(Map.from_struct(prior_alias), [:last_seen_at, :expires_at, :updated_at])
      prior_request_ids = Repo.all(from request in Request, where: request.pool_id == ^setup.pool.id, select: request.id)
      :atomics.put(registration_gate, 1, 1)
      {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, CodexPooler.JSON.encode!(payload))
      {conn, _websocket, ["response.completed"], _healthy} = receive_terminal!(conn, websocket, ref, [])
      assert await_request_finalized!(setup, prior_request_ids) == "succeeded"
      await_registration!(registration)
      assert length(FakeUpstream.requests(upstream)) == 3
      healthy_alias = Repo.one!(response_alias_query(setup, "resp_healthy_tool_fixture"))
      assert healthy_alias.status == "active"
      refute Repo.exists?(response_alias_query(setup, "resp_invalid_tool_fixture"))
      recovery_attempt = Repo.one!(from a in Attempt, join: r in Request, on: r.id == a.request_id, where: r.pool_id == ^setup.pool.id and r.id != ^prior_attempt.request_id and r.status == "succeeded")
      assert %{"lifecycle_id" => recovery_lifecycle, "generation" => recovery_generation} = recovery_attempt.response_metadata["upstream_websocket_connection"]
      refute {recovery_lifecycle, recovery_generation} == {prior_lifecycle, prior_generation}
      assert :ok = FakeUpstream.verify!(upstream)

      if peer do
        assert node(peer.owner_pid) != node()
        assert %{upstream_pid: pid} = :sys.get_state(peer.owner_pid)
        assert node(pid) == peer.node
        TestDiagnostics.puts("TASK3_REMOTE shape=#{terminal_shape} kind=#{kind} defect=#{defect} request_id=#{request.id} socket_node=#{node()} owner_node=#{peer.node} upstream_node=#{node(pid)} usage_input=5457 usage_output=2 cache_write=5454 provider_requests=3")
      end

      conn
    after
      Mint.HTTP.close(conn)
    end
  end

  # A done status other than `completed` leaves the call unfinished: a following `response.completed` becomes the
  # sanitized `server_error` event, while the provider's own `response.incomplete` keeps its outcome.
  for topology <- [:direct, :local_owner], kind <- ["function_call", "custom_tool_call"], {status, terminal_type} <- [{"failed", "response.completed"}, {"incomplete", "response.completed"}, {"incomplete", "response.incomplete"}] do
    @tag topology: topology
    @tag kind: kind
    test "#{topology} #{kind} done status #{status} before #{terminal_type}", %{topology: topology, kind: kind} do
      assert_done_status_outcome!(topology, kind, unquote(status), unquote(terminal_type))
    end
  end

  defp assert_done_status_outcome!(topology, kind, status, terminal_type) do
    CodexPooler.TestAppEnv.restore_on_exit(:websocket_owner_forwarding_enabled)
    Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, topology != :direct)
    frames = Enum.map(status_events(kind, status, terminal_type), &CodexPooler.JSON.encode!/1)
    # provenance: synthetic_adversarial for the failed status, which the provider schema does not give a function or custom tool call; the incomplete status before an incomplete terminal follows the openai-node ResponseFunctionToolCall status enum, with invented ids and usage.
    upstream = start_upstream(FakeUpstream.strict_sequence([FakeUpstream.websocket_text_frames(frames)]))
    setup = gateway_setup(upstream)
    assert :ok = Events.subscribe_pool(setup.pool)
    port = start_public_endpoint!()
    {conn, websocket, ref} = connect!(port, setup, "tool-status-#{System.unique_integer([:positive])}")

    try do
      payload = %{"type" => "response.create", "model" => setup.model.exposed_model_id, "input" => [%{"type" => "message", "role" => "user", "content" => [%{"type" => "input_text", "text" => "synthetic tool status turn"}]}], "stream" => true}
      {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, CodexPooler.JSON.encode!(payload))
      {conn, _websocket, types, terminal} = receive_terminal!(conn, websocket, ref, [])
      finalized = await_request_finalized!(setup, [])
      assert "response.output_item.done" in types
      request = Repo.one!(from r in Request, where: r.pool_id == ^setup.pool.id)

      if terminal_type == "response.completed" do
        assert List.last(types) == "error"
        assert terminal["error"]["code"] == "server_error"
        refute "response.completed" in types
        assert {finalized, request.status, request.last_error_code} == {"failed", "failed", "upstream_stream_error"}
      else
        assert List.last(types) == "response.incomplete"
        assert terminal["response"]["incomplete_details"] == %{"reason" => "max_output_tokens"}
        refute "error" in types
        assert {finalized, request.status, request.last_error_code} == {"succeeded", "succeeded", nil}
      end

      assert :ok = FakeUpstream.verify!(upstream)
      conn
    after
      Mint.HTTP.close(conn)
    end
  end

  for kind <- ["function_call", "custom_tool_call"], defect <- [:missing_done, :wrong_id] do
    test "bridged SSE rejects #{kind} #{defect} legacy completion and preserves healthy legacy", %{conn: conn} do
      assert_bridged_rejection_and_healthy_legacy!(conn, unquote(kind), unquote(defect))
    end
  end

  defp assert_bridged_rejection_and_healthy_legacy!(conn, kind, defect) do
    CodexPooler.TestAppEnv.restore_on_exit(:websocket_owner_forwarding_enabled)
    Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, true)

    upstream =
      start_upstream(
        FakeUpstream.strict_sequence([
          FakeUpstream.websocket_text_frames(Enum.map(source_events(kind, defect, :legacy), &CodexPooler.JSON.encode!/1)),
          FakeUpstream.websocket_text_frames([CodexPooler.JSON.encode!(terminal_shape(healthy_terminal(), :legacy))])
        ])
      )

    setup = gateway_setup(upstream)
    assert :ok = Events.subscribe_pool(setup.pool)
    session = "legacy-bridge-#{System.unique_integer([:positive])}"
    handler = {__MODULE__, make_ref()}
    on_exit(fn -> :telemetry.detach(handler) end)
    :ok = :telemetry.attach(handler, [:codex_pooler, :gateway, :stream, :outcome], fn _event, _measurements, metadata, target -> if self() == target, do: send(target, {:bridge_settled, metadata.outcome}) end, self())
    payload = %{"model" => setup.model.exposed_model_id, "input" => "synthetic legacy bridge", "stream" => true}
    response = conn |> auth(setup) |> put_req_header("x-session-id", session) |> post("/v1/responses", payload)
    types = response.resp_body |> String.split("\n") |> Enum.filter(&String.starts_with?(&1, "event: ")) |> Enum.map(&String.replace_prefix(&1, "event: ", ""))
    assert types == Enum.map(source_events(kind, defect, :legacy) |> Enum.drop(-1), & &1["type"]) ++ ["response.created", "error"]
    assert response.resp_body =~ ~s("server_error")
    refute response.resp_body =~ "incomplete_tool_item"
    assert_received {:bridge_settled, "failed"}
    request = Repo.one!(from r in Request, where: r.pool_id == ^setup.pool.id)
    assert request.status == "failed"
    assert request.transport == "http_sse"
    attempt = Repo.one!(from a in Attempt, where: a.request_id == ^request.id)
    assert attempt.status == "failed"
    assert attempt.transport == "websocket"
    assert attempt.network_error_code == "upstream_stream_error"
    settlement = Repo.one!(from l in LedgerEntry, where: l.request_id == ^request.id and l.entry_kind == "settlement")
    assert {settlement.input_tokens, settlement.output_tokens, settlement.cache_write_tokens} == {5457, 2, 5454}
    assert Repo.aggregate(from(l in LedgerEntry, where: l.request_id == ^request.id and l.entry_kind == "release"), :count) == 1
    refute Repo.exists?(response_alias_query(setup, "resp_invalid_tool_fixture"))
    healthy = build_conn() |> auth(setup) |> put_req_header("x-session-id", session) |> post("/v1/responses", payload)
    assert healthy.resp_body =~ "event: response.completed"
    assert_received {:bridge_settled, "succeeded"}
    assert length(FakeUpstream.requests(upstream)) == 2
    assert Enum.all?(FakeUpstream.requests(upstream), &(&1.method == "WEBSOCKET"))
    assert :ok = FakeUpstream.verify!(upstream)
    TestDiagnostics.puts("F4_BRIDGE request_id=#{request.id} terminal=error settlement=failed input=5457 output=2 cache_write=5454 release=1 provider_requests=2 healthy_legacy=completed")
  end

  defp await_request_finalized!(setup, prior_request_ids) do
    request_id = Repo.one!(from request in Request, where: request.pool_id == ^setup.pool.id and request.id not in ^prior_request_ids, select: request.id)
    assert_receive {Events, %{reason: "request_finalized", payload: %{"request_id" => ^request_id, "status" => status}}}, @timeout
    status
  end

  defp response_alias_query(setup, response_id) do
    hash = :crypto.hash(:sha256, response_id)
    from a in BridgeSessionAlias, where: a.pool_id == ^setup.pool.id and a.api_key_id == ^setup.api_key.id and a.alias_kind == "previous_response_id" and a.alias_hash == ^hash
  end

  def registration_query(_event, _measurements, metadata, {parent, registration, gate}) do
    query = metadata[:query] || ""

    if String.starts_with?(query, "INSERT") and String.contains?(query, "bridge_session_aliases") and "previous_response_id" in (metadata[:params] || []) and :atomics.compare_exchange(gate, 1, 1, 0) == :ok do
      send(parent, {registration, :aliases_written, self()})

      receive do
        {^registration, :observed} -> :ok
      after
        @timeout -> raise "alias registration observer did not acknowledge"
      end
    end
  end

  defp await_registration!(registration) do
    assert_receive {^registration, :aliases_written, executor}, @timeout
    monitor = Process.monitor(executor)
    send(executor, {registration, :observed})
    assert_receive {:DOWN, ^monitor, :process, ^executor, :normal}, @timeout
  end

  defp source_events(kind, defect, terminal_shape) do
    item = %{"type" => kind, "id" => "tool_fixture", "call_id" => "call_fixture", "name" => "fixture", "status" => "in_progress"}
    added = %{"type" => "response.output_item.added", "output_index" => 0, "item" => item}
    delta_type = if kind == "function_call", do: "response.function_call_arguments.delta", else: "response.custom_tool_call_input.delta"
    delta = %{"type" => delta_type, "item_id" => "tool_fixture", "output_index" => 0, "delta" => "synthetic"}
    done = %{"type" => "response.output_item.done", "output_index" => 0, "item" => Map.put(item, "status", "completed")}

    events =
      case defect do
        :missing_done -> [added, delta]
        :missing_index -> [Map.delete(added, "output_index"), delta, done]
        :string_index -> [Map.put(added, "output_index", "0"), delta, done]
        :wrong_index -> [added, delta, Map.put(done, "output_index", 1)]
        :wrong_id -> [added, delta, put_in(done, ["item", "id"], "other_fixture")]
        :one_pending -> [added, delta, done, %{added | "output_index" => 1, "item" => %{item | "id" => "second_fixture", "call_id" => "second_call"}}]
      end

    events ++ [terminal_shape(%{"type" => "response.completed", "response" => %{"id" => "resp_invalid_tool_fixture", "status" => "completed", "output" => [], "usage" => %{"input_tokens" => 5457, "input_tokens_details" => %{"cached_tokens" => 0, "cache_write_tokens" => 5454}, "output_tokens" => 2, "total_tokens" => 5459}}}, terminal_shape)]
  end

  defp status_events(kind, status, terminal_type) do
    item = %{"type" => kind, "id" => "tool_fixture", "call_id" => "call_fixture", "name" => "fixture", "status" => "in_progress"}
    delta_type = if kind == "function_call", do: "response.function_call_arguments.delta", else: "response.custom_tool_call_input.delta"
    response = %{"id" => "resp_status_tool_fixture", "status" => String.replace_prefix(terminal_type, "response.", ""), "output" => [], "usage" => %{"input_tokens" => 5457, "input_tokens_details" => %{"cached_tokens" => 0, "cache_write_tokens" => 5454}, "output_tokens" => 2, "total_tokens" => 5459}}
    response = if terminal_type == "response.incomplete", do: Map.put(response, "incomplete_details", %{"reason" => "max_output_tokens"}), else: response

    [
      %{"type" => "response.output_item.added", "output_index" => 0, "item" => item},
      %{"type" => delta_type, "item_id" => "tool_fixture", "output_index" => 0, "delta" => "synthetic"},
      %{"type" => "response.output_item.done", "output_index" => 0, "item" => %{item | "status" => status}},
      %{"type" => terminal_type, "response" => response}
    ]
  end

  defp terminal_shape(%{"response" => response}, :legacy), do: Map.drop(response, ["status", "output"])
  defp terminal_shape(event, :typed), do: event

  defp healthy_terminal, do: %{"type" => "response.completed", "response" => %{"id" => "resp_healthy_tool_fixture", "status" => "completed", "output" => [], "usage" => %{"input_tokens" => 6, "output_tokens" => 2, "total_tokens" => 8}}}

  defp connect!(port, setup, turn_state) do
    {:ok, conn} = Mint.HTTP.connect(:http, "127.0.0.1", port, protocols: [:http1])
    headers = [{"authorization", setup.authorization}, {"x-codex-turn-state", turn_state}, {"openai-beta", "responses_websockets=2026-02-06"}]
    {:ok, conn, ref} = Mint.WebSocket.upgrade(:ws, conn, "/v1/responses", headers)
    {:ok, conn, status, headers} = await_public_websocket_upgrade(conn, ref)
    {conn, websocket} = mint_websocket_new!(conn, ref, status, headers)
    {conn, websocket, ref}
  end

  defp receive_terminal!(conn, websocket, ref, types) do
    {conn, websocket, frame} = public_websocket_receive_text!(conn, websocket, ref)
    decoded = CodexPooler.JSON.decode!(frame)
    type = decoded["type"]
    types = types ++ [type]

    if type in ["response.completed", "response.failed", "response.incomplete", "error"],
      do: {conn, websocket, types, decoded},
      else: receive_terminal!(conn, websocket, ref, types)
  end
end
