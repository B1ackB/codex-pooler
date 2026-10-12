defmodule CodexPooler.Gateway.Runtime.Finalization.RetainedUsage do
  @moduledoc "Internal, metadata-only receipt captured before an owner retires admitted work."

  import Ecto.Query
  alias CodexPooler.Accounting.{Attempt, ModelObservation, Request}
  alias CodexPooler.Gateway.Persistence.{CodexSession, CodexTurn}
  alias CodexPooler.Gateway.Persistence.SessionContinuity.OwnerWitness
  alias CodexPooler.Gateway.Runtime.Finalization.{AttemptSettlement, ResponseUsage, SettlementRetry}
  alias CodexPooler.Gateway.Websocket.OwnerCleanup
  alias CodexPooler.Repo

  @enforce_keys [:session_id, :pool_id, :api_key_id, :request_id, :attempt_id, :replay_generation, :owner_witness, :usage, :failure]
  defstruct @enforce_keys

  @type failure :: %{status: String.t(), response_status_code: pos_integer(), error_code: String.t()}
  @type principal :: %{id: Ecto.UUID.t(), pool_id: Ecto.UUID.t(), api_key_id: Ecto.UUID.t()}
  @type t :: %__MODULE__{session_id: Ecto.UUID.t(), pool_id: Ecto.UUID.t(), api_key_id: Ecto.UUID.t(), request_id: Ecto.UUID.t(), attempt_id: Ecto.UUID.t(), replay_generation: non_neg_integer(), owner_witness: OwnerWitness.t(), usage: ResponseUsage.usage(), failure: failure()}
  @type failure_evidence :: %{request_id: Ecto.UUID.t(), attempt_id: Ecto.UUID.t(), replay_generation: non_neg_integer(), counters: map(), reason_class: atom()}
  @type result :: AttemptSettlement.finalization_result() | {:noop, :identity_mismatch} | {:error, failure_evidence()}
  @failure_codes ~w(client_disconnected owner_drained owner_forward_timeout downstream_delivery_timeout stream_interrupted owner_unavailable downstream_stream_error stream_idle_timeout upstream_stream_error)
  @counters [:input_tokens, :cached_input_tokens, :cache_write_tokens, :output_tokens, :reasoning_tokens, :total_tokens]

  @doc "Pure capture from internal owner authority. No owner state, frames or schema structs cross to the worker."
  @spec capture(OwnerCleanup.t() | nil, principal() | nil, map() | nil, failure()) :: {:ok, t()} | {:noop, :usage_unknown} | {:error, :invalid_receipt}
  def capture(cleanup, principal, usage, failure)
  def capture(_cleanup, _principal, nil, _failure), do: {:noop, :usage_unknown}
  def capture(_cleanup, _principal, %{status: status}, _failure) when status in ["usage_unknown", "usage_pending", "not_applicable"], do: {:noop, :usage_unknown}

  def capture(%OwnerCleanup{} = cleanup, %{id: session_id, pool_id: pool_id, api_key_id: api_key_id}, usage, failure) do
    with true <- session_id == cleanup.session_id,
         true <- Enum.all?([session_id, pool_id, api_key_id, cleanup.request_id, cleanup.attempt_id, cleanup.owner_lease_token], &uuid?/1),
         true <- is_integer(cleanup.replay_generation) and cleanup.replay_generation >= 0,
         {:ok, usage} <- validated_usage(usage),
         {:ok, failure} <- validated_failure(failure) do
      {:ok, %__MODULE__{session_id: session_id, pool_id: pool_id, api_key_id: api_key_id, request_id: cleanup.request_id, attempt_id: cleanup.attempt_id, replay_generation: cleanup.replay_generation, owner_witness: %OwnerWitness{session_id: session_id, lease_token: cleanup.owner_lease_token}, usage: usage, failure: failure}}
    else
      _invalid -> {:error, :invalid_receipt}
    end
  end

  def capture(_cleanup, _principal, _usage, _failure), do: {:error, :invalid_receipt}

  @spec finalize(t(), [SettlementRetry.option()]) :: result()
  def finalize(%__MODULE__{} = snapshot, retry_options \\ []) do
    request = %Request{id: snapshot.request_id, pool_id: snapshot.pool_id, api_key_id: snapshot.api_key_id}
    attempt = %Attempt{id: snapshot.attempt_id, request_id: snapshot.request_id, replay_generation: snapshot.replay_generation}
    options = Keyword.merge(retry_options, exhaustion: :return, subject: "retained_usage", fallback: "receipt_failed")

    case SettlementRetry.run(:retained_usage, request, attempt, fn -> settle(snapshot) end, options) do
      {:error, :settlement_retry_exhausted} -> failed(snapshot, :settlement_retry_exhausted)
      result -> result
    end
  rescue
    _error in [DBConnection.ConnectionError, Postgrex.Error] -> failed(snapshot, :database_error)
    _error -> failed(snapshot, :settlement_error)
  end

  defp settle(snapshot) do
    # Accounting owns the canonical session/request/attempt lock prefix. This
    # wrapper adds no row locks, and verifies the binding again while that
    # prefix is held, before the outer transaction can publish or commit.
    case Repo.transaction(fn -> reconcile_snapshot(snapshot) end) do
      {:ok, result} -> result
      {:error, :identity_mismatch} -> {:noop, :identity_mismatch}
      {:error, _reason} -> failed(snapshot, :settlement_error)
    end
  end

  defp reconcile_snapshot(snapshot) do
    with {:ok, request, attempt} <- original_rows(snapshot),
         true <- session_matches?(snapshot) do
      attrs = %{usage: snapshot.usage, status: snapshot.failure.status, response_status_code: snapshot.failure.response_status_code, last_error_code: snapshot.failure.error_code, delivery_failure: snapshot.failure}
      result = AttemptSettlement.reconcile(request, attempt, attrs, snapshot.owner_witness, exhaustion: :return)
      if not session_matches?(snapshot), do: Repo.rollback(:identity_mismatch)

      case result do
        {:error, _reason} -> Repo.rollback(:settlement_error)
        _ -> result
      end
    else
      _ -> Repo.rollback(:identity_mismatch)
    end
  end

  defp original_rows(snapshot) do
    with %Request{} = request <- Repo.get(Request, snapshot.request_id),
         %Attempt{} = attempt <- Repo.get(Attempt, snapshot.attempt_id),
         true <- request.pool_id == snapshot.pool_id and request.api_key_id == snapshot.api_key_id and attempt.request_id == request.id do
      # Retain the captured generation: the locked accounting comparison must
      # reject a changed generation instead of substituting the current one.
      {:ok, request, %{attempt | replay_generation: snapshot.replay_generation}}
    else
      _ -> :error
    end
  end

  defp session_matches?(snapshot) do
    Repo.exists?(from session in CodexSession, join: turn in CodexTurn, on: turn.codex_session_id == session.id, where: session.id == ^snapshot.session_id and session.pool_id == ^snapshot.pool_id and session.api_key_id == ^snapshot.api_key_id and turn.request_id == ^snapshot.request_id)
  end

  defp validated_usage(%{status: "usage_known"} = usage) do
    counters = Map.take(usage, @counters)
    required? = Enum.all?([:input_tokens, :output_tokens, :total_tokens], &Map.has_key?(counters, &1))
    integers? = Enum.all?(counters, fn {_key, value} -> is_integer(value) and value >= 0 and value <= 9_223_372_036_854_775_807 end)

    envelope = %{"usage" => Map.new(counters, fn {key, value} -> {Atom.to_string(key), value} end), "model" => usage[:served_model], "service_tier" => usage[:service_tier]}

    case {required? and integers?, ResponseUsage.from_stream_event(envelope)} do
      {true, %{status: "usage_known"} = normalized} ->
        observation = ModelObservation.normalize(usage[:model_observation], normalized[:served_model])
        {:ok, normalized |> Map.put(:source, "retained_usage") |> Map.put(:model_observation, observation)}

      _ ->
        :error
    end
  end

  defp validated_usage(_usage), do: :error

  defp validated_failure(%{status: status, response_status_code: code, error_code: error}) when status in ["failed", "cancelled"] and is_integer(code) and code >= 100 and code <= 599 and error in @failure_codes,
    do: {:ok, %{status: status, response_status_code: code, error_code: error}}

  defp validated_failure(_failure), do: :error
  defp uuid?(value), do: match?({:ok, _}, Ecto.UUID.cast(value))

  defp failed(snapshot, reason), do: {:error, %{request_id: snapshot.request_id, attempt_id: snapshot.attempt_id, replay_generation: snapshot.replay_generation, counters: Map.take(snapshot.usage, @counters), reason_class: reason}}
end

defimpl Inspect, for: CodexPooler.Gateway.Runtime.Finalization.RetainedUsage do
  def inspect(_snapshot, _options), do: "#RetainedUsage<redacted>"
end
