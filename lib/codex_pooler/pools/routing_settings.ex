defmodule CodexPooler.Pools.RoutingSettings do
  use Ecto.Schema

  import Ecto.Changeset

  @type t :: %__MODULE__{}
  @type attrs :: map()

  alias CodexPooler.Pools.RoutingStrategy

  @primary_key {:pool_id, :binary_id, autogenerate: false}
  @foreign_key_type :binary_id

  schema "pool_routing_settings" do
    field :routing_strategy, :string
    field :bridge_ring_size, :integer
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
      :v1_compatibility_enabled,
      :allow_image_generation,
      :allow_audio_transcription,
      :metadata,
      :created_at,
      :updated_at
    ])
    |> validate_inclusion(:routing_strategy, routing_strategies())
    |> validate_number(:bridge_ring_size, greater_than_or_equal_to: 1)
  end

  @spec routing_strategies() :: [String.t()]
  defdelegate routing_strategies(), to: RoutingStrategy, as: :ids
end
