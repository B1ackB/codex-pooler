defmodule CodexPooler.Accounting.PricingResolution.InputEstimate do
  @moduledoc """
  Local input reservation approximation, independent of provider token billing.
  Semantic images receive one fixed allowance; ordinary JSON keeps its byte estimate.
  """

  alias CodexPooler.Accounting.PricingResolution.ImageProjection

  @tokens_per_image 1_600
  @image_strategy "json_bytes_div_4_plus_image_allowance_v1"
  @text_strategy "json_bytes_div_4_ceil"
  @basis "fixed_per_image_allowance"

  @type context :: ImageProjection.context()
  @type provenance :: %{
          version: pos_integer(),
          basis: String.t(),
          image_count: pos_integer(),
          image_tokens: pos_integer(),
          tokens_per_image: pos_integer()
        }
  @type t :: %{required(:input_tokens) => non_neg_integer(), required(:strategy) => String.t(), optional(:image_estimate) => provenance()}

  @spec estimate(map(), context()) :: t()
  def estimate(payload, context) do
    {projected, count} = ImageProjection.project(payload, context)
    input_tokens = text_tokens(projected) + count * @tokens_per_image

    if count > 0 do
      %{input_tokens: input_tokens, strategy: @image_strategy, image_estimate: image_provenance(count)}
    else
      %{input_tokens: input_tokens, strategy: @text_strategy}
    end
  end

  @doc false
  @spec precomputed(map()) :: t()
  def precomputed(%{input_tokens: input_tokens} = estimate) do
    case {estimate[:strategy], provenance(estimate[:image_estimate])} do
      {@image_strategy, %{} = provenance} ->
        %{input_tokens: input_tokens, strategy: @image_strategy, image_estimate: provenance}

      _legacy ->
        %{input_tokens: input_tokens, strategy: @text_strategy}
    end
  end

  @doc "Returns only the complete, typed image allowance receipt, including persisted string keys."
  @spec provenance(term()) :: provenance() | nil
  def provenance(value) when is_map(value) do
    count = field(value, :image_count)

    if field(value, :version) === 1 and field(value, :basis) == @basis and
         is_integer(count) and count > 0 and field(value, :tokens_per_image) === @tokens_per_image and
         field(value, :image_tokens) === count * @tokens_per_image do
      image_provenance(count)
    end
  end

  def provenance(_value), do: nil

  @doc false
  @spec put_metadata(map(), term()) :: map()
  def put_metadata(metadata, value) do
    case provenance(value) do
      nil -> metadata
      shaped -> Map.put(metadata, "image_estimate", Map.new(shaped, fn {key, value} -> {Atom.to_string(key), value} end))
    end
  end

  defp image_provenance(count), do: %{version: 1, basis: @basis, image_count: count, image_tokens: count * @tokens_per_image, tokens_per_image: @tokens_per_image}
  defp field(value, key), do: Map.get(value, key, Map.get(value, Atom.to_string(key)))

  defp text_tokens(payload) do
    prompt = Map.drop(payload, ["model", :model, "stream", :stream, "max_tokens", :max_tokens, "max_output_tokens", :max_output_tokens])
    if map_size(prompt) == 0, do: 0, else: div(byte_size(CodexPooler.JSON.encode!(prompt)) + 3, 4)
  end
end
