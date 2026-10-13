defmodule CodexPooler.Accounting.PricingResolution.ContextRule do
  @moduledoc false

  alias CodexPooler.Catalog.ContextPricing

  @spec classify(map(), non_neg_integer() | {:bucket, String.t()}) :: map()
  def classify(%{anchor: anchor, buckets: buckets}, input) do
    coherent? = anchor.config["importer_format_revision"] in [3, "3"] or Map.has_key?(anchor.config, "context_pricing")
    rule = if coherent?, do: validated_rule(anchor, buckets), else: ContextPricing.legacy()
    {bucket, band} = bucket(rule, buckets, input, coherent?)

    %{
      coherent: coherent?,
      rule: rule,
      bucket: bucket,
      band: band,
      primary: buckets[bucket],
      fallback: if(bucket == "long_context", do: buckets["default"]),
      facts: facts(anchor, rule, bucket, band)
    }
  end

  defp validated_rule(anchor, buckets) do
    rule = anchor.config["context_pricing"]

    if valid_rule?(anchor.config, buckets) and
         Enum.all?(buckets, fn {_bucket, snapshot} ->
           snapshot.config["catalog_schema_version"] == anchor.config["catalog_schema_version"] and
             semantics(snapshot.config["context_pricing"]) == semantics(rule)
         end) do
      rule
    else
      %{"status" => "invalid", "reason" => "invalid_rule"}
    end
  end

  defp valid_rule?(%{"catalog_schema_version" => 1, "context_pricing" => rule}, _buckets),
    do: rule == ContextPricing.legacy()

  defp valid_rule?(%{"catalog_schema_version" => 2, "context_pricing" => %{"version" => 1, "provenance" => %{"source_url" => source} = provenance} = rule}, buckets)
       when is_binary(source) do
    published = rule |> Map.delete("version") |> Map.put("provenance", Map.delete(provenance, "source_url"))

    match?({:ok, _rules}, ContextPricing.normalize(%{"version" => 1, "tiers" => %{"standard" => published}}, ["standard"], %{"standard" => buckets}, source))
  end

  defp valid_rule?(_config, _buckets), do: false

  defp semantics(rule) when is_map(rule), do: ContextPricing.semantics(rule)
  defp semantics(_rule), do: nil

  # Metadata-directed callers explicitly ask for a stored physical bucket;
  # they do not claim any estimated or actual input-token count.
  defp bucket(_rule, _buckets, {:bucket, bucket}, _coherent?), do: {bucket, "stored_bucket"}
  defp bucket(%{"status" => status}, _buckets, _input, _coherent?) when status in ["unknown", "invalid"], do: {nil, nil}
  defp bucket(%{"status" => "uniform"}, buckets, _input, _coherent?), do: {lower_bucket(buckets), "uniform"}

  defp bucket(%{"long_context_input_tokens_gt" => cutoff}, buckets, input, coherent?) do
    if input > cutoff do
      {"long_context", "long"}
    else
      {if(coherent?, do: lower_bucket(buckets), else: "default"), "lower"}
    end
  end

  defp lower_bucket(buckets), do: if(Map.has_key?(buckets, "default"), do: "default", else: "short_context")

  defp facts(anchor, rule, bucket, band) do
    %{
      "status" => rule["status"],
      "basis" => basis(rule["status"]),
      "generation" => %{
        "model_identifier" => anchor.model_identifier |> String.trim() |> String.downcase(),
        "price_version" => anchor.price_version,
        "importer_format_revision" => anchor.config["importer_format_revision"],
        "service_tier" => CodexPooler.ServiceTier.canonicalize(anchor.config["service_tier"])
      },
      "price_bucket" => bucket,
      "band" => band
    }
    |> maybe_put("long_context_input_tokens_gt", rule["long_context_input_tokens_gt"])
    |> maybe_put("reason", rule["reason"])
  end

  defp basis("legacy"), do: "legacy_assumption"
  defp basis(status) when status in ["known", "uniform"], do: "catalog"
  defp basis(_status), do: "unavailable"
  defp maybe_put(map, _key, nil), do: map
  defp maybe_put(map, key, value), do: Map.put(map, key, value)
end
