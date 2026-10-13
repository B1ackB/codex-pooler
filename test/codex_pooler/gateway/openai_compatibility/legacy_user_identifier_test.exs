defmodule CodexPooler.Gateway.OpenAICompatibility.LegacyUserIdentifierTest do
  use ExUnit.Case, async: true

  alias CodexPooler.Gateway.OpenAICompatibility.{Chat, Responses}

  test "Responses accepts and discards the legacy user identifier without replacing modern fields" do
    for user <- [nil, "", "synthetic-private-legacy-user"] do
      payload = Map.merge(base(), %{"user" => user, "safety_identifier" => "synthetic-safety-id", "prompt_cache_key" => "synthetic-cache-key"})
      assert {:ok, validated} = Responses.validate(payload)
      refute Map.has_key?(validated, "user")
      assert {:ok, result} = Responses.coerce(payload)
      refute Map.has_key?(result.payload, "user")
      assert result.payload["safety_identifier"] == payload["safety_identifier"]
      assert result.payload["prompt_cache_key"] == payload["prompt_cache_key"]
    end
  end

  test "legacy user alone never becomes a safety or cache identity" do
    for adapter <- [Responses, Chat] do
      assert {:ok, result} = adapter.coerce(Map.put(base(), "user", "synthetic-private-legacy-user"))
      refute Map.has_key?(result.payload, "user")
      refute Map.has_key?(result.payload, "safety_identifier")
      refute Map.has_key?(result.payload, "prompt_cache_key")
    end
  end

  test "both public adapters reject malformed legacy user values" do
    for adapter <- [Responses, Chat], user <- [42, false, [], %{}] do
      assert {:error, %{status: 400, code: "invalid_request", param: "user", message: "user must be a string or null"}} = adapter.coerce(Map.put(base(), "user", user))
    end
  end

  defp base, do: %{"model" => "sample-model", "input" => "synthetic"}
end
