defmodule CodexPooler.Pools.RoutingProfileCompatibilityTest.LegacyRoutingSettings do
  use Ecto.Schema

  import Ecto.Changeset

  @type t :: %__MODULE__{}
  @type attrs :: map()

  @routing_strategies ~w(bridge_ring deterministic_rotation least_recent_success quota_first)

  @primary_key {:pool_id, :binary_id, autogenerate: false}
  @foreign_key_type :binary_id

  schema "pool_routing_settings" do
    field :routing_strategy, :string
    field :bridge_ring_size, :integer
    field :sticky_websocket_sessions, :boolean
    field :sticky_http_sessions, :boolean
    field :prompt_cache_affinity_enabled, :boolean, default: true
    field :v1_compatibility_enabled, :boolean, default: true
    field :allow_image_generation, :boolean, default: true
    field :allow_audio_transcription, :boolean, default: true
    field :metadata, :map
    field :created_at, :utc_datetime_usec
    field :updated_at, :utc_datetime_usec
  end

  @spec changeset(t() | Ecto.Changeset.t(), attrs()) :: Ecto.Changeset.t()
  def changeset(settings, attrs) do
    settings
    |> cast(attrs, [
      :pool_id,
      :routing_strategy,
      :bridge_ring_size,
      :sticky_websocket_sessions,
      :sticky_http_sessions,
      :prompt_cache_affinity_enabled,
      :v1_compatibility_enabled,
      :allow_image_generation,
      :allow_audio_transcription,
      :metadata,
      :created_at,
      :updated_at
    ])
    |> validate_required([
      :pool_id,
      :routing_strategy,
      :bridge_ring_size,
      :sticky_websocket_sessions,
      :sticky_http_sessions,
      :prompt_cache_affinity_enabled,
      :v1_compatibility_enabled,
      :allow_image_generation,
      :allow_audio_transcription,
      :metadata,
      :created_at,
      :updated_at
    ])
    |> validate_inclusion(:routing_strategy, @routing_strategies)
    |> validate_number(:bridge_ring_size, greater_than_or_equal_to: 1)
  end

  @spec routing_strategies() :: [String.t()]
  def routing_strategies, do: @routing_strategies
end

defmodule CodexPooler.Pools.RoutingProfileCompatibilityTest do
  use CodexPooler.DataCase, async: false
  import CodexPooler.AccountsFixtures
  import CodexPooler.PoolerFixtures
  alias CodexPooler.Accounts.Scope
  alias CodexPooler.Gateway.Payloads.RequestOptions
  alias CodexPooler.Gateway.Persistence.CodexSession
  alias CodexPooler.Gateway.Routing.{BridgeRing, RoutePlanInput}
  alias CodexPooler.Pools.{Routing, RoutingStrategy}
  alias CodexPooler.Pools.RoutingProfileCompatibilityTest.LegacyRoutingSettings
  @legacy_fields ~w(sticky_websocket_sessions sticky_http_sessions prompt_cache_affinity_enabled)a

  setup do
    reset_bootstrap_state_fixture!()
    %{user: owner} = bootstrap_owner_fixture()
    %{scope: Scope.for_user(owner, ["instance_owner"]), owner: owner}
  end

  for ws <- [false, true], http <- [false, true], cache <- [false, true] do
    @values [ws, http, cache]
    test "published legacy Ecto INSERT SELECT UPDATE survives #{inspect(@values)}", %{scope: scope, owner: owner} do
      pool = pool_fixture(%{created_by_user_id: owner.id})
      %{api_key: api_key} = active_api_key_fixture(pool)
      model = model_fixture(pool)
      assignments = for _ <- 1..3, do: active_upstream_assignment_fixture(pool)
      [excluded | eligible] = assignments
      input = %{auth: %{pool: pool, api_key: api_key}, model: model, candidates: Enum.map(eligible, &{&1.assignment, &1.identity}), route_plan_input: %RoutePlanInput{correlation_id: "legacy-reader"}, request_options: RequestOptions.build(%{request_id: "legacy-reader", codex_session: %CodexSession{id: Ecto.UUID.generate(), pool_upstream_assignment_id: excluded.assignment.id}}, "/backend-api/codex/responses", %{"prompt_cache_key" => "synthetic-cache"})}
      Repo.delete!(Routing.ensure_routing_settings(pool))
      now = DateTime.utc_now()
      values = Map.new(Enum.zip(@legacy_fields, unquote(@values)))
      attrs = Map.merge(values, %{pool_id: pool.id, routing_strategy: "bridge_ring", bridge_ring_size: 3, metadata: %{}, created_at: now, updated_at: now, v1_compatibility_enabled: true, allow_image_generation: true, allow_audio_transcription: true})
      assert {:ok, _} = Repo.insert(LegacyRoutingSettings.changeset(%LegacyRoutingSettings{}, attrs))
      assert Map.take(Repo.get!(LegacyRoutingSettings, pool.id), @legacy_fields) == values

      for profile <- RoutingStrategy.ids() do
        assert {:ok, current} = Routing.update_routing_settings(scope, pool, Map.merge(values, %{routing_strategy: profile, bridge_ring_size: 5}))
        assert current.routing_strategy == profile
        assert Map.take(Map.from_struct(current), @legacy_fields) == %{}
        assert Map.take(Repo.get!(LegacyRoutingSettings, pool.id), @legacy_fields) == values
        before_plan = BridgeRing.plan_route(input)
        assert before_plan.request_metadata["session_preference_status"] == "candidate_unavailable"
        refute Enum.any?(before_plan.candidates, fn {assignment, _} -> assignment.id == excluded.assignment.id end)
        inverse = Map.new(values, fn {key, value} -> {key, not value} end)
        legacy = Repo.get!(LegacyRoutingSettings, pool.id)
        assert {:ok, _} = Repo.update(LegacyRoutingSettings.changeset(legacy, inverse))
        assert Map.take(Repo.get!(LegacyRoutingSettings, pool.id), @legacy_fields) == inverse
        after_plan = BridgeRing.plan_route(input)
        assert Enum.map(before_plan.candidates, fn {assignment, _} -> assignment.id end) == Enum.map(after_plan.candidates, fn {assignment, _} -> assignment.id end)
        assert before_plan.affinity == after_plan.affinity
        assert Routing.routing_settings_with_defaults(pool).routing_strategy == profile
        assert RoutingStrategy.prompt_cache_locality?(Routing.get_routing_settings(pool).routing_strategy) == (profile == "bridge_ring")
        assert Map.take(Map.from_struct(Routing.get_routing_settings(pool)), @legacy_fields) == %{}
        assert {:ok, _} = Repo.update(LegacyRoutingSettings.changeset(Repo.get!(LegacyRoutingSettings, pool.id), values))
      end
    end
  end

  test "new context inserts retain SQL defaults readable by the published reader", %{owner: owner} do
    pool = pool_fixture(%{created_by_user_id: owner.id})
    Repo.delete!(Routing.ensure_routing_settings(pool))
    assert Routing.ensure_routing_settings(pool)
    row = Repo.get!(LegacyRoutingSettings, pool.id)
    assert Map.take(row, @legacy_fields) == %{sticky_websocket_sessions: true, sticky_http_sessions: false, prompt_cache_affinity_enabled: true}
  end
end
