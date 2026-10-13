defmodule CodexPoolerWeb.V1.ResponsesHomeAssistantHistoryTest do
  use CodexPoolerWeb.ConnCase, async: false

  import Ecto.Query
  import CodexPoolerWeb.Runtime.BackendCodexTestSupport, only: [auth: 2, gateway_setup: 1, start_upstream: 1]
  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport, only: [model_serving_scope: 0, set_model_serving_mode!: 3]

  alias CodexPooler.Accounting.{ClientIdentity, Request}
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Repo

  # provenance: source_derived, Home Assistant Core 2026.10.1 entity.py, with synthetic content and tool names.
  # This proves Pooler's HTTP boundary; released-client execution belongs to the external acceptance lane.
  # https://github.com/home-assistant/core/blob/abe1fdd7cd6b4de1715c87b6fd87d92699514ef9/homeassistant/components/openai_conversation/entity.py
  for replay? <- [false, true] do
    test "Full HTTP Responses accepts Home Assistant string-message history, replay #{replay?}" do
      upstream = start_upstream(completed_upstream())
      setup = gateway_setup(upstream)
      set_model_serving_mode!(model_serving_scope(), setup, "full")

      payload = %{
        "model" => setup.model.exposed_model_id,
        "input" => history(unquote(replay?)),
        "tools" => [tool()],
        "max_output_tokens" => 3000,
        "safety_identifier" => "synthetic-conversation",
        "prompt_cache_key" => "synthetic-entry",
        "service_tier" => "auto",
        "store" => false,
        "stream" => true,
        "reasoning" => %{"effort" => "low", "summary" => "auto"},
        "text" => %{"verbosity" => "low"},
        "prompt_cache_options" => %{"ttl" => "30m"}
      }

      response = build_conn() |> auth(setup) |> put_req_header("user-agent", "AsyncOpenAI/Python 3.10.0") |> post("/v1/responses", payload)

      assert response.status == 200
      assert response.resp_body =~ "event: response.completed"
      assert [captured] = FakeUpstream.requests(upstream)
      assert captured.path == "/backend-api/codex/responses"
      assert captured.json["instructions"] == "synthetic instructions"
      assert [%{"role" => "user", "content" => [%{"type" => "input_text"}]} | rest] = captured.json["input"]
      assert [%{"type" => "function", "name" => "read_fixture", "strict" => false}] = captured.json["tools"]
      refute Map.has_key?(captured.json, "previous_response_id")
      assert captured.json["store"] == false

      if unquote(replay?) do
        assert [%{"type" => "function_call", "call_id" => "call_synthetic"}, %{"type" => "function_call_output", "call_id" => "call_synthetic"}, %{"role" => "assistant", "content" => [%{"type" => "output_text"}]}, %{"role" => "user", "content" => [%{"type" => "input_text"}]}] = rest
      else
        assert rest == []
      end

      assert [%{status: "succeeded", user_agent: user_agent}] = Repo.all(from(request in Request, where: request.pool_id == ^setup.pool.id))
      assert ClientIdentity.classify(user_agent).kind == "openai_python"
      FakeUpstream.verify!(upstream)
    end
  end

  defp history(replay?) do
    initial = [%{"type" => "message", "role" => "developer", "content" => "synthetic instructions"}, %{"type" => "message", "role" => "user", "content" => "synthetic request"}]

    if replay? do
      initial ++
        [
          %{"type" => "function_call", "name" => "read_fixture", "arguments" => "{}", "call_id" => "call_synthetic"},
          %{"type" => "function_call_output", "call_id" => "call_synthetic", "output" => "{\"data\":{},\"error\":false}"},
          %{"type" => "message", "role" => "assistant", "content" => "synthetic answer"},
          %{"type" => "message", "role" => "user", "content" => "synthetic follow-up"}
        ]
    else
      initial
    end
  end

  defp tool, do: %{"type" => "function", "name" => "read_fixture", "description" => "Read synthetic fixture", "parameters" => %{"type" => "object", "properties" => %{}}, "strict" => false}

  defp completed_upstream do
    FakeUpstream.strict_sequence([
      FakeUpstream.expect_request(
        method: "POST",
        path: "/backend-api/codex/responses",
        json: [valid: true],
        respond: FakeUpstream.sse_stream([{"response.completed", %{"type" => "response.completed", "response" => %{"id" => "resp_synthetic_ha_history", "status" => "completed", "model" => "sample-model", "output" => [], "usage" => %{"input_tokens" => 1, "output_tokens" => 1, "total_tokens" => 2}}}}])
      )
    ])
  end
end
