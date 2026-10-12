defmodule CodexPooler.Pools.RoutingStrategyTest do
  use ExUnit.Case, async: true

  alias CodexPooler.Pools.{RoutingSettings, RoutingStrategy}

  test "the persisted catalogue keeps its existing order and only the cache profile enables locality" do
    assert RoutingStrategy.ids() == ~w(bridge_ring deterministic_rotation least_recent_success quota_first)
    assert RoutingSettings.routing_strategies() == RoutingStrategy.ids()
    assert Enum.filter(RoutingStrategy.ids(), &RoutingStrategy.prompt_cache_locality?/1) == ["bridge_ring"]
  end

  test "routing settings accept every profile and reject an unknown profile" do
    for id <- RoutingStrategy.ids() do
      changeset = RoutingSettings.changeset(%RoutingSettings{}, %{routing_strategy: id})
      refute Keyword.has_key?(changeset.errors, :routing_strategy)
    end

    changeset = RoutingSettings.changeset(%RoutingSettings{}, %{routing_strategy: "unknown"})
    assert {"is invalid", [validation: :inclusion, enum: ids]} = changeset.errors[:routing_strategy]
    assert ids == RoutingStrategy.ids()
    refute RoutingStrategy.prompt_cache_locality?("unknown")
  end
end
