defmodule CodexPooler.Catalog.ContextPricing do
  @moduledoc false

  @max_tokens 9_007_199_254_740_991
  @type descriptor :: %{required(String.t()) => term()}

  @spec legacy() :: descriptor()
  def legacy, do: %{"version" => 1, "status" => "legacy", "long_context_input_tokens_gt" => 272_000}

  @spec semantics(descriptor() | nil) :: descriptor() | nil
  def semantics(nil), do: nil
  def semantics(rule), do: Map.delete(rule, "provenance")

  @spec normalize(term(), [String.t()], map(), String.t()) :: {:ok, %{String.t() => descriptor()}} | :error
  def normalize(value, tiers, prices, source_url) do
    with true <- exact_keys?(value, ~w(version tiers)),
         true <- value["version"] === 1,
         true <- exact_keys?(value["tiers"], tiers),
         true <- Enum.all?(value["tiers"], fn {tier, rule} -> valid_rule?(rule, prices[tier]) end) do
      {:ok,
       Map.new(value["tiers"], fn {tier, rule} ->
         {tier, rule |> Map.put("version", 1) |> update_in(["provenance"], &Map.put(&1, "source_url", source_url))}
       end)}
    else
      _invalid -> :error
    end
  end

  defp valid_rule?(rule, prices) when is_map(rule) do
    valid_provenance?(rule["provenance"]) and valid_status?(rule, prices)
  end

  defp valid_rule?(_rule, _prices), do: false

  defp valid_status?(%{"status" => "known"} = rule, _prices),
    do: exact_keys?(rule, ~w(status long_context_input_tokens_gt provenance)) and valid_tokens?(rule["long_context_input_tokens_gt"])

  defp valid_status?(%{"status" => "uniform"} = rule, prices),
    do: exact_keys?(rule, ~w(status provenance)) and not Map.has_key?(prices, "long_context")

  defp valid_status?(%{"status" => "unknown"} = rule, _prices),
    do: exact_keys?(rule, ~w(status reason observed_rules provenance)) and rule["reason"] in ~w(missing_source unsupported_source conflicting_source) and valid_observations?(rule["observed_rules"]) and (rule["reason"] != "missing_source" or rule["observed_rules"] == [])

  defp valid_status?(_rule, _prices), do: false

  defp valid_provenance?(value) do
    exact_keys?(value, ~w(locator evidence_sha256)) and is_binary(value["locator"]) and
      String.trim(value["locator"]) != "" and byte_size(value["locator"]) <= 256 and
      is_binary(value["evidence_sha256"]) and Regex.match?(~r/\A[0-9a-f]{64}\z/, value["evidence_sha256"])
  end

  defp valid_observations?(values) when is_list(values) do
    Enum.all?(values, &valid_observation?/1) and values == Enum.sort_by(Enum.uniq(values), &{&1["bucket"], &1["operator"], &1["input_tokens"]})
  end

  defp valid_observations?(_values), do: false

  defp valid_observation?(value),
    do: exact_keys?(value, ~w(bucket operator input_tokens)) and value["bucket"] in ~w(short_context long_context) and value["operator"] in ~w(lt lte gt gte) and valid_tokens?(value["input_tokens"])

  defp valid_tokens?(value), do: is_integer(value) and value > 0 and value <= @max_tokens

  defp exact_keys?(value, keys) when is_map(value), do: Enum.sort(Map.keys(value)) == Enum.sort(keys)
  defp exact_keys?(_value, _keys), do: false
end
