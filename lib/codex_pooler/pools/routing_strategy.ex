defmodule CodexPooler.Pools.RoutingStrategy do
  @moduledoc """
  Supported routing profiles and their ordering policy.
  """

  @ids ~w(bridge_ring deterministic_rotation least_recent_success quota_first)

  @spec ids() :: [String.t()]
  def ids, do: @ids

  @spec prompt_cache_locality?(String.t()) :: boolean()
  def prompt_cache_locality?(strategy), do: strategy == "bridge_ring"
end
