defmodule CodexPooler.Gateway.Runtime.Finalization.AttemptSettlement do
  @moduledoc """
  Final accounting boundary for routed gateway attempts.

  Runtime gateway dispatch decides transport flow and route health side effects; this
  module owns the terminal attempt/reservation accounting calls and their
  sanitized failure logging.
  """

  import Ecto.Query
  alias CodexPooler.Accounting
  alias CodexPooler.Accounting.{Attempt, Metadata, ModelObservation, PreAttemptRelease, Request}
  alias CodexPooler.Accounting.FailureResponse
  alias CodexPooler.Gateway.Contracts
  alias CodexPooler.Gateway.Persistence.CodexTurn
  alias CodexPooler.Gateway.Persistence.SessionContinuity
  alias CodexPooler.Gateway.Persistence.SessionContinuity.OwnerWitness
  alias CodexPooler.Gateway.Runtime.Finalization.SettlementRetry
  alias CodexPooler.Repo

  @type attrs :: %{optional(atom()) => term()}
  @type usage :: %{optional(atom()) => term()} | %{optional(String.t()) => term()}
  @type gateway_error :: Contracts.gateway_error()
  @type finalization_result ::
          {:ok, Accounting.internal_request_result_row()}
          | {:stale_generation, Accounting.internal_request_result_row()}
          | {:error, gateway_error()}
  @type settlement_result ::
          finalization_result()
          | {:ok, Accounting.request_result_row() | Attempt.t()}

  @spec stale_generation?(term()) :: boolean()
  def stale_generation?({:stale_generation, _result}), do: true
  def stale_generation?(_result), do: false

  @doc false
  @spec first_settlement?(
          Accounting.internal_request_result_row()
          | Accounting.finalization_disposition()
        ) :: boolean()
  def first_settlement?(%{finalization_disposition: disposition}),
    do: first_settlement?(disposition)

  def first_settlement?(:inserted), do: true
  def first_settlement?(disposition) when disposition in [:replaced, :reused], do: false

  @spec finalize_success(Request.t(), Attempt.t(), usage(), attrs()) :: finalization_result()
  def finalize_success(request, attempt, usage, attrs),
    do: finalize_success(request, attempt, usage, attrs, nil)

  @spec finalize_success(Request.t(), Attempt.t(), usage(), attrs(), OwnerWitness.t() | nil) ::
          finalization_result()
  def finalize_success(request, attempt, usage, attrs, owner_witness) do
    attrs =
      attrs
      |> Map.new()
      |> complete_turn_before_commit(CodexTurn.succeeded_status(), nil, attempt, owner_witness)

    :finalize_success
    |> SettlementRetry.run(request, attempt, fn -> Accounting.finalize_success_with_disposition(request, attempt, usage, attrs) end)
    |> accounting_result(:finalize_success, request, attempt)
  end

  @spec finalize_failure(Request.t(), Attempt.t(), attrs()) :: finalization_result()
  def finalize_failure(request, attempt, attrs),
    do: finalize_failure(request, attempt, attrs, nil)

  @spec finalize_failure(Request.t(), Attempt.t(), attrs(), OwnerWitness.t() | nil) ::
          finalization_result()
  def finalize_failure(request, attempt, attrs, owner_witness) do
    attrs = Map.new(attrs)

    attrs =
      complete_turn_before_commit(
        attrs,
        CodexTurn.failed_status(),
        Map.get(attrs, :last_error_code),
        attempt,
        owner_witness
      )

    :finalize_failure
    |> SettlementRetry.run(request, attempt, fn -> Accounting.finalize_failure_with_disposition(request, attempt, attrs) end)
    |> accounting_result(:finalize_failure, request, attempt)
  end

  @spec finalize_partial_stream_failure(Request.t(), Attempt.t(), usage(), attrs()) ::
          finalization_result()
  def finalize_partial_stream_failure(request, attempt, usage, attrs),
    do: finalize_partial_stream_failure(request, attempt, usage, attrs, nil)

  @spec finalize_partial_stream_failure(
          Request.t(),
          Attempt.t(),
          usage(),
          attrs(),
          OwnerWitness.t() | nil
        ) :: finalization_result()
  def finalize_partial_stream_failure(request, attempt, usage, attrs, owner_witness) do
    attrs = Map.new(attrs)
    error_code = Map.get(attrs, :last_error_code)

    attrs =
      complete_turn_before_commit(
        attrs,
        partial_stream_turn_status(error_code),
        error_code,
        attempt,
        owner_witness
      )

    :finalize_partial_stream_failure
    |> SettlementRetry.run(request, attempt, fn -> Accounting.finalize_partial_stream_failure_with_disposition(request, attempt, usage, attrs) end)
    |> accounting_result(:finalize_partial_stream_failure, request, attempt)
  end

  @spec record_retryable_failure(Request.t(), Attempt.t(), attrs()) :: settlement_result()
  def record_retryable_failure(request, attempt, attrs) do
    attrs = Map.put_new(attrs, :now, DateTime.utc_now() |> DateTime.truncate(:microsecond))

    :record_retryable_failure
    |> SettlementRetry.run(request, attempt, fn -> Accounting.record_retryable_attempt_failure(attempt, attrs) end, after_retry: &reconcile_retryable_failure(&1, attempt, attrs))
    |> accounting_result(:record_retryable_failure, request, attempt)
  end

  # A try after a transient failure can meet the earlier try's own COMMIT: the
  # client saw the COMMIT fail, but PostgreSQL had committed it. The attempt
  # then already carries the retryable failure this call records, so the
  # request fails over as if the first try had answered, instead of ending
  # with `attempt_already_finalized` (findings#294). Only the executor of the
  # attempt records its retryable failure; any other terminal state keeps
  # the refusal.
  defp reconcile_retryable_failure({:error, %{code: :attempt_already_finalized}} = refused, %Attempt{id: attempt_id} = attempt, attrs) do
    request = %Request{id: attempt.request_id}
    status = Map.get(attrs, :attempt_status, "retryable_failed")
    code = blank_to_nil(Map.get(attrs, :last_error_code))
    upstream_status = Map.get(attrs, :response_status_code)
    completed_at = Map.fetch!(attrs, :now)

    result =
      Accounting.with_current_replay_generation(request, attempt, fn ->
        reconcile_recorded_failure(Repo.get(Attempt, attempt_id), request, attrs, refused, {status, code, upstream_status, completed_at})
      end)

    case result do
      {:ok, reconciled} -> reconciled
      {:error, _reason} -> refused
    end
  end

  defp reconcile_retryable_failure(result, _attempt, _attrs), do: result

  defp reconcile_recorded_failure(%Attempt{status: "retryable_failed", retryable: true, completed_at: completed_at, network_error_code: code, upstream_status_code: upstream_status} = recorded, request, attrs, refused, {"retryable_failed", code, upstream_status, completed_at}) do
    latest = Repo.one(from current in Attempt, where: current.request_id == ^request.id, order_by: [desc: current.attempt_number], limit: 1, select: current.id)
    expected = persisted_failure_attributes(attrs)
    if latest == recorded.id and Map.take(recorded, Map.keys(expected)) == expected, do: {:ok, recorded}, else: refused
  end

  defp reconcile_recorded_failure(_recorded, _request, _attrs, refused, _expected), do: refused

  defp persisted_failure_attributes(attrs) do
    usage = Map.get(attrs, :usage, %{})
    served_model = Metadata.bounded_model_identifier(Map.get(usage, :served_model, Map.get(usage, "served_model")))
    observation = ModelObservation.normalize(Map.get(usage, :model_observation, Map.get(usage, "model_observation")), served_model)
    %{error_message: blank_to_nil(Map.get(attrs, :error_message)), latency_ms: Map.get(attrs, :latency_ms), usage_status: Map.get(attrs, :usage_status, "usage_unknown"), served_model: served_model, model_observation: observation, response_metadata: Metadata.sanitize_metadata(Map.get(attrs, :attempt_metadata, %{}))}
  end

  defp blank_to_nil(value) when value in [nil, ""], do: nil
  defp blank_to_nil(value), do: value

  @doc """
  Finalizes a request routing refused (route filtering refused the remaining
  candidates, a circuit refused the next one, the held-back partition refused
  every candidate), its reservation released in full: a pre-attempt
  `routing_rejected` release while the request has no attempt, otherwise a
  release after its latest attempt (`finalize_before_candidate_attempt/3`,
  findings#321).
  """
  @spec finalize_routing_refusal(Request.t(), attrs()) :: settlement_result()
  def finalize_routing_refusal(%Request{} = request, attrs),
    do: finalize_before_candidate_attempt(request, attrs, PreAttemptRelease.routing_rejected())

  @doc """
  Finalizes a request stopped before its next candidate's attempt, its
  reservation released in full. While the request has no attempt the release
  is a pre-attempt one declaring `phase`. After an earlier candidate's attempt
  it carries the latest attempt and no phase, with usage unknown, as every
  release written after an attempt does (findings#221): the routing refusals
  and the drain checkpoint that stop a later candidate used to record a
  pre-attempt release then (findings#321).
  """
  @spec finalize_before_candidate_attempt(Request.t(), attrs(), String.t()) :: settlement_result()
  def finalize_before_candidate_attempt(%Request{} = request, attrs, phase) when is_binary(phase) do
    release =
      case Repo.one(from attempt in Attempt, where: attempt.request_id == ^request.id, order_by: [desc: attempt.attempt_number], limit: 1) do
        %Attempt{} = attempt -> %{usage_status: "usage_unknown", released_after_attempt: attempt}
        nil -> %{usage_status: "not_applicable", pre_attempt_phase: phase}
      end

    finalize_reservation_failure(request, attrs |> Map.new() |> Map.drop([:usage_status, :pre_attempt_phase, :released_after_attempt]) |> Map.merge(release))
  end

  @spec finalize_reservation_failure(Request.t(), attrs()) :: settlement_result()
  def finalize_reservation_failure(request, attrs) do
    attrs = Map.new(attrs)
    error_code = Map.get(attrs, :last_error_code)

    attrs =
      Map.put(attrs, :before_commit, fn result ->
        SessionContinuity.complete_codex_turn({:ok, result}, CodexTurn.failed_status(), error_code)
        :ok
      end)

    :finalize_reservation_failure
    |> SettlementRetry.run(request, nil, fn -> Accounting.finalize_reservation_failure(request, attrs) end)
    |> accounting_result(:finalize_reservation_failure, request)
  end

  # The request's codex turn completes inside the settlement transaction, as
  # its last write, so no resend can observe the request terminal while its
  # turn is still in progress (findings#288). Accounting runs the callback
  # only for the current generation: a stale generation writes nothing, and
  # its turn belongs to the generation that replaced it.
  defp complete_turn_before_commit(attrs, _status, _error_code, attempt, owner_witness) do
    Map.put(attrs, :before_commit, fn result ->
      request = result.request
      status = if request.status == "succeeded", do: CodexTurn.succeeded_status(), else: partial_stream_turn_status(request.last_error_code)
      SessionContinuity.complete_codex_turn({:ok, result}, status, request.last_error_code, attempt, owner_witness)
      :ok
    end)
  end

  defp accounting_result(result, operation, request, attempt \\ nil)

  defp accounting_result(
         {:ok, %{stale_generation?: true} = value},
         _operation,
         _request,
         _attempt
       ),
       do: {:stale_generation, value}

  defp accounting_result({:ok, value}, _operation, _request, _attempt), do: {:ok, value}

  # `SettlementRetry` already logged the one warning that names the request
  # and the stage; the request is left to execution recovery.
  defp accounting_result({:error, :settlement_retry_exhausted}, _operation, _request, _attempt),
    do: {:error, %{status: 500, code: "gateway_accounting_failed", message: "gateway accounting finalization failed"}}

  defp accounting_result(
         {:error, %{code: code}},
         _operation,
         _request,
         _attempt
       )
       when code in [:request_already_finalized, :attempt_already_finalized] do
    {:error,
     %{
       status: 499,
       code: Atom.to_string(code),
       message: "request lifecycle already completed"
     }}
  end

  defp accounting_result({:error, reason}, operation, request, attempt) do
    FailureResponse.accounting_failure(operation, request, attempt, reason)
  end

  defp partial_stream_turn_status("client_disconnected"), do: CodexTurn.interrupted_status()
  defp partial_stream_turn_status(:client_disconnected), do: CodexTurn.interrupted_status()
  # A rollout drain ends the turn the same way owner-side drain finalization
  # does (`Finalization.Interruption.terminal_turn_status/1`): interrupted, not
  # failed. The upstream did nothing wrong.
  defp partial_stream_turn_status("owner_drained"), do: CodexTurn.interrupted_status()
  defp partial_stream_turn_status(:owner_drained), do: CodexTurn.interrupted_status()
  defp partial_stream_turn_status(_error_code), do: CodexTurn.failed_status()
end
