defmodule CodexPooler.Gateway.Transports.Websocket.ActivityRegistry.Drain do
  @moduledoc false

  @type entry :: %{
          required(:token) => reference(),
          required(:kind) => :direct | :proxy | :receipt_settlement,
          required(:pid) => pid(),
          required(:status) => :active | {:finished, :completed | :aborted | :failed}
        }

  @spec begin(nil | map(), map()) :: {map(), reference(), [map()]}
  def begin(nil, activities) do
    epoch = make_ref()
    activities = Map.reject(activities, fn {_token, entry} -> entry.kind == :local_owner end)
    drain = %{epoch: epoch, tokens: activities |> Map.keys() |> MapSet.new(), outcomes: %{}, receipts_closed?: false, receipt_producers_failed?: false}
    {drain, epoch, public_entries(drain, activities)}
  end

  def begin(drain, activities), do: {drain, drain.epoch, public_entries(drain, activities)}

  @spec join_receipt(nil | map(), reference()) :: nil | map()
  def join_receipt(nil, _token), do: nil
  def join_receipt(%{receipts_closed?: false} = drain, token), do: %{drain | tokens: MapSet.put(drain.tokens, token)}

  @spec close_receipts(nil | map(), boolean()) :: nil | map()
  def close_receipts(nil, _closed?), do: nil
  def close_receipts(drain, closed?), do: %{drain | receipts_closed?: drain.receipts_closed? or closed?}

  @spec receipt_entries(nil | map(), map()) :: [entry()]
  def receipt_entries(nil, _activities), do: []
  def receipt_entries(drain, activities), do: Enum.filter(entries(drain, activities), &(&1.kind == :receipt_settlement))

  defp public_entries(drain, activities), do: Enum.reject(entries(drain, activities), &(&1.kind == :receipt_settlement))

  @spec record_outcome(nil | map(), reference(), map(), atom()) :: nil | map()
  def record_outcome(nil, _token, _entry, _outcome), do: nil

  def record_outcome(drain, token, entry, outcome) do
    if MapSet.member?(drain.tokens, token) do
      put_in(drain.outcomes[token], %{entry: entry, outcome: outcome})
    else
      drain
    end
  end

  @spec outcome(nil | map(), reference()) :: {:finished, atom()} | :unknown
  def outcome(nil, _token), do: :unknown

  def outcome(drain, token) do
    case Map.get(drain.outcomes, token) do
      %{outcome: outcome} -> {:finished, outcome}
      nil -> :unknown
    end
  end

  defp entries(drain, activities) do
    Enum.map(drain.tokens, fn token ->
      case Map.get(activities, token) do
        nil ->
          %{entry: entry, outcome: outcome} = Map.fetch!(drain.outcomes, token)
          %{token: token, kind: entry.kind, pid: entry.pid, status: {:finished, outcome}} |> Map.merge(Map.take(entry, [:receipt_disposition]))

        entry ->
          %{token: token, kind: entry.kind, pid: entry.pid, status: :active}
      end
    end)
  end
end
