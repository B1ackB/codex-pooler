defmodule CodexPoolerWeb.V1.LegacyUserIdentifierTest do
  use CodexPoolerWeb.ConnCase, async: false

  import Ecto.Query
  import CodexPoolerWeb.Runtime.BackendCodexTestSupport, only: [auth: 2, gateway_setup: 1, public_websocket_connect_with_request_headers!: 5, public_websocket_receive_text!: 3, public_websocket_send_text!: 4, start_public_endpoint!: 0, start_upstream: 1]
  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport, only: [model_serving_scope: 0, set_model_serving_mode!: 3]

  alias CodexPooler.Accounting.{Attempt, LedgerEntry, Request}
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Repo

  @legacy_user "synthetic-private-legacy-user"

  for mode <- ~w(full lite), stream? <- [false, true] do
    test "#{mode} HTTP stream #{stream?} strips legacy user before egress and persistence" do
      upstream = start_upstream(FakeUpstream.sse_stream([{"response.completed", completed()}]))
      setup = gateway_setup(upstream)
      set_model_serving_mode!(model_serving_scope(), setup, unquote(mode))

      for user <- [@legacy_user, nil] do
        response = build_conn() |> auth(setup) |> post("/v1/responses", Map.merge(payload(setup, user), %{"stream" => unquote(stream?)}))
        assert response.status == 200
        if unquote(stream?), do: assert(response.resp_body =~ "response.completed"), else: assert(json_response(response, 200)["status"] == "completed")
        assert_forwarded!(List.last(FakeUpstream.requests(upstream)))
      end

      assert Repo.aggregate(from(request in Request, where: request.pool_id == ^setup.pool.id and request.status == "succeeded"), :count) == 2
      assert_private!(setup)
    end
  end

  for mode <- ~w(full lite) do
    test "#{mode} public websocket strips legacy user before egress" do
      upstream = start_upstream(FakeUpstream.websocket_text_frames([CodexPooler.JSON.encode!(completed())]))
      setup = gateway_setup(upstream)
      set_model_serving_mode!(model_serving_scope(), setup, unquote(mode))
      port = start_public_endpoint!()
      {conn, websocket, ref, _headers} = public_websocket_connect_with_request_headers!(port, setup, "legacy-user-#{System.unique_integer([:positive])}", "/v1/responses", [{"openai-beta", "responses_websockets=2026-02-06"}])

      try do
        {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, CodexPooler.JSON.encode!(Map.merge(payload(setup, @legacy_user), %{"type" => "response.create", "stream" => true})))
        {_conn, _websocket, frame} = public_websocket_receive_text!(conn, websocket, ref)
        assert %{"type" => "response.completed"} = CodexPooler.JSON.decode!(frame)
        assert [captured] = FakeUpstream.requests(upstream)
        assert_forwarded!(captured)
      after
        Mint.HTTP.close(conn)
      end
    end
  end

  for transport <- [:http, :websocket] do
    test "#{transport} malformed user values are rejected before accounting or dispatch" do
      upstream = start_upstream(FakeUpstream.json_response(%{"id" => "must_not_dispatch"}))
      setup = gateway_setup(upstream)
      port = if unquote(transport) == :websocket, do: start_public_endpoint!()

      for user <- [42, false, [], %{}] do
        error =
          if unquote(transport) == :http do
            response = build_conn() |> auth(setup) |> post("/v1/responses", payload(setup, user))
            json_response(response, 400)["error"]
          else
            {conn, websocket, ref, _headers} = public_websocket_connect_with_request_headers!(port, setup, "legacy-invalid-#{System.unique_integer([:positive])}", "/v1/responses", [{"openai-beta", "responses_websockets=2026-02-06"}])

            try do
              {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, CodexPooler.JSON.encode!(Map.merge(payload(setup, user), %{"type" => "response.create", "stream" => true})))
              {_conn, _websocket, frame} = public_websocket_receive_text!(conn, websocket, ref)
              assert %{"type" => "error", "status" => 400, "error" => error} = CodexPooler.JSON.decode!(frame)
              error
            after
              Mint.HTTP.close(conn)
            end
          end

        assert %{"param" => "user", "code" => "invalid_request", "message" => "user must be a string or null"} = error
      end

      assert FakeUpstream.count(upstream) == 0
      assert Repo.aggregate(Request, :count) == 0
      assert Repo.aggregate(Attempt, :count) == 0
      assert Repo.aggregate(LedgerEntry, :count) == 0
    end
  end

  test "the native backend contract keeps user passthrough and provider refusal" do
    upstream = start_upstream(FakeUpstream.json_response(%{"error" => %{"type" => "invalid_request_error", "code" => "unsupported_parameter", "param" => "user", "message" => "Unsupported parameter: user"}}, 400))
    setup = gateway_setup(upstream)
    set_model_serving_mode!(model_serving_scope(), setup, "full")

    response = build_conn() |> auth(setup) |> post("/backend-api/codex/responses", Map.merge(payload(setup, @legacy_user), %{"input" => [%{"type" => "message", "role" => "user", "content" => [%{"type" => "input_text", "text" => "synthetic"}]}], "stream" => true, "instructions" => "synthetic"}))

    assert %{"error" => %{"code" => "unsupported_parameter", "param" => "user"}} = json_response(response, 400)
    assert [captured] = FakeUpstream.requests(upstream)
    assert captured.json["user"] == @legacy_user
    assert_private!(setup)
  end

  defp payload(setup, user), do: %{"model" => setup.model.exposed_model_id, "input" => "synthetic", "user" => user, "safety_identifier" => "synthetic-modern-safety", "prompt_cache_key" => "synthetic-modern-cache", "store" => false}

  defp assert_forwarded!(captured) do
    refute Map.has_key?(captured.json, "user")
    # The ordinary upstream envelope strips safety_identifier independently of legacy user handling.
    refute Map.has_key?(captured.json, "safety_identifier")
    assert captured.json["prompt_cache_key"] == "synthetic-modern-cache"
  end

  defp assert_private!(setup) do
    requests = Repo.all(from(request in Request, where: request.pool_id == ^setup.pool.id))
    ids = Enum.map(requests, & &1.id)
    attempts = Repo.all(from(attempt in Attempt, where: attempt.request_id in ^ids))
    refute inspect({requests, attempts}, limit: :infinity, printable_limit: :infinity) =~ @legacy_user
  end

  defp completed, do: %{"type" => "response.completed", "response" => %{"id" => "resp_synthetic_legacy_user", "status" => "completed", "model" => "sample-model", "output" => [], "usage" => %{"input_tokens" => 1, "output_tokens" => 1, "total_tokens" => 2}}}
end
