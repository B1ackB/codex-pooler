defmodule CodexPooler.Accounting.Usage.Observatory.QueryScope do
  @moduledoc false

  import Ecto.Query

  alias CodexPooler.Accounting.{Request, RequestLogFact, RequestOutcome}
  alias CodexPooler.Catalog.Model

  @failed_statuses ~w(failed rejected)
  @usage_known "usage_known"
  @usage_not_applicable "not_applicable"
  @safe_model_pattern "^[A-Za-z0-9][A-Za-z0-9._:/-]{0,79}$"

  defmacrop known_usage(status, value) do
    quote do
      type(
        fragment(
          "CASE WHEN ? = ? THEN COALESCE(?, 0) ELSE 0 END",
          unquote(status),
          unquote(@usage_known),
          unquote(value)
        ),
        :integer
      )
    end
  end

  defmacrop settled_cost(status, pricing_status, value) do
    quote do
      type(
        fragment(
          "CASE WHEN ? = ? AND COALESCE(?, '') = 'priced' THEN ROUND(COALESCE(?, 0), 0)::bigint ELSE 0 END",
          unquote(status),
          unquote(@usage_known),
          unquote(pricing_status),
          unquote(value)
        ),
        :integer
      )
    end
  end

  defmacrop settled_cost_available(status, pricing_status) do
    quote do
      fragment(
        "CASE WHEN ? = ? AND COALESCE(?, '') = 'priced' THEN 1 ELSE 0 END",
        unquote(status),
        unquote(@usage_known),
        unquote(pricing_status)
      )
    end
  end

  defmacrop estimated_cost(status, pricing_status, value) do
    quote do
      type(
        fragment(
          "CASE WHEN NOT (? = ? AND COALESCE(?, '') = 'priced') AND COALESCE(?, 0) > 0 THEN ROUND(?, 0)::bigint ELSE 0 END",
          unquote(status),
          unquote(@usage_known),
          unquote(pricing_status),
          unquote(value),
          unquote(value)
        ),
        :integer
      )
    end
  end

  defmacrop estimated_cost_available(status, pricing_status, value) do
    quote do
      fragment(
        "CASE WHEN NOT (? = ? AND COALESCE(?, '') = 'priced') AND COALESCE(?, 0) > 0 THEN 1 ELSE 0 END",
        unquote(status),
        unquote(@usage_known),
        unquote(pricing_status),
        unquote(value)
      )
    end
  end

  defmacrop cost_unavailable(status, pricing_status, value) do
    quote do
      fragment(
        "CASE WHEN (? = ? AND COALESCE(?, '') = 'priced') OR (NOT (? = ? AND COALESCE(?, '') = 'priced') AND COALESCE(?, 0) > 0) THEN 0 ELSE 1 END",
        unquote(status),
        unquote(@usage_known),
        unquote(pricing_status),
        unquote(status),
        unquote(@usage_known),
        unquote(pricing_status),
        unquote(value)
      )
    end
  end

  defmacrop bucket_index(admitted_at, started_at, bucket_seconds) do
    quote do
      type(
        fragment(
          "FLOOR(EXTRACT(EPOCH FROM (? - ?)) / ?)::integer",
          unquote(admitted_at),
          unquote(started_at),
          unquote(bucket_seconds)
        ),
        :integer
      )
    end
  end

  defmacrop model_label(exposed_model_id) do
    quote do
      fragment(
        "CASE WHEN ? ~ ? THEN ? ELSE 'Unknown model' END",
        unquote(exposed_model_id),
        unquote(@safe_model_pattern),
        unquote(exposed_model_id)
      )
    end
  end

  defmacrop endpoint_class(endpoint) do
    quote do
      fragment(
        "CASE WHEN ? = '/backend-api/codex/responses/compact' THEN 'compaction' WHEN ? IN ('/v1/responses', '/backend-api/codex/responses') THEN 'responses' WHEN ? IN ('/backend-api/files', '/backend-api/files/uploaded', '/v1/files', '/v1/files/content', '/v1/files/delete') THEN 'files' WHEN ? = '/backend-api/transcribe' THEN 'audio' WHEN ? IN ('/v1/models', '/backend-api/codex/models') THEN 'models' WHEN ? IN ('/api/codex/usage', '/wham/usage', '/backend-api/wham/usage', '/v1/usage') THEN 'usage' ELSE 'other' END",
        unquote(endpoint),
        unquote(endpoint),
        unquote(endpoint),
        unquote(endpoint),
        unquote(endpoint),
        unquote(endpoint)
      )
    end
  end

  defmacrop model_name(display_name, exposed_model_id) do
    quote do
      fragment(
        "CASE WHEN BTRIM(?) ~ '^[A-Za-z0-9][A-Za-z0-9 ._:/()+-]{0,79}$' THEN BTRIM(?) ELSE ? END",
        unquote(display_name),
        unquote(display_name),
        model_label(unquote(exposed_model_id))
      )
    end
  end

  defmacrop safe_code(last_error_code) do
    quote do
      fragment(
        "CASE WHEN ? IS NULL THEN NULL WHEN ? ~* '(rate|quota)' THEN 'rate_limited' WHEN ? ~* '(auth|unauthor|forbidden)' THEN 'authentication' WHEN ? ~* 'timeout' THEN 'timeout' WHEN ? ~* '(network|connect|upstream)' THEN 'service_unavailable' WHEN ? ~* '(schema|invalid)' THEN 'invalid_request' ELSE 'request_failed' END",
        unquote(last_error_code),
        unquote(last_error_code),
        unquote(last_error_code),
        unquote(last_error_code),
        unquote(last_error_code),
        unquote(last_error_code)
      )
    end
  end

  defmacrop request_scope(request, pool_id, api_key_id, started_at, ended_at) do
    quote do
      unquote(request).pool_id == unquote(pool_id) and
        unquote(request).api_key_id == unquote(api_key_id) and
        unquote(request).admitted_at >= unquote(started_at) and
        unquote(request).admitted_at < unquote(ended_at)
    end
  end

  @doc """
  Aggregation-facing rows for the scoped key/pool/window.

  Tokens and cost read from the request's denormalized fact
  (`request_log_facts`) typed columns, so the aggregate queries never join
  `ledger_entries` or dig through settlement JSONB. The fact is a 1:1 projection
  keyed by `request_id`, so the join is one primary-key lookup per scoped
  request. Token/settled reads stay gated on `latest_settlement_usage_status`
  (backfilled facts store raw usage columns, live facts store them pre-gated, so
  the SQL gate keeps both identical to the old ledger read); estimated cost is
  stored ungated and gated here to non-priced rows.
  """
  def scoped_facts(identity, window) do
    from(request in Request,
      as: :request,
      left_join: fact in RequestLogFact,
      on: fact.request_id == request.id,
      left_join: model in Model,
      on: model.id == request.model_id and model.pool_id == request.pool_id,
      where:
        request_scope(
          request,
          ^identity.pool_id,
          ^identity.api_key_id,
          ^window.started_at,
          ^window.ended_at
        ),
      select: %{
        request_id: request.id,
        bucket_index: bucket_index(request.admitted_at, ^window.started_at, ^window.bucket_seconds),
        model_label: model_label(model.exposed_model_id),
        model_name: model_name(model.display_name, model.exposed_model_id),
        succeeded: fragment("CASE WHEN ? = 'succeeded' THEN 1 ELSE 0 END", request.status),
        in_progress:
          fragment(
            "CASE WHEN ? IN ('accepted', 'in_progress') THEN 1 ELSE 0 END",
            request.status
          ),
        has_settlement: fragment("CASE WHEN ? IS NULL THEN 0 ELSE 1 END", fact.latest_settlement_entry_id),
        # A settlement whose usage does not apply (a refusal answered before
        # anything reached the provider) has no usage to miss.
        unknown_usage:
          fragment(
            "CASE WHEN ? IS NOT NULL AND ? NOT IN (?, ?) THEN 1 ELSE 0 END",
            fact.latest_settlement_entry_id,
            fact.latest_settlement_usage_status,
            ^@usage_known,
            ^@usage_not_applicable
          ),
        input_tokens: known_usage(fact.latest_settlement_usage_status, fact.latest_input_tokens),
        cached_input_tokens: known_usage(fact.latest_settlement_usage_status, fact.latest_cached_input_tokens),
        output_tokens: known_usage(fact.latest_settlement_usage_status, fact.latest_output_tokens),
        reasoning_tokens: known_usage(fact.latest_settlement_usage_status, fact.latest_reasoning_tokens),
        total_tokens: known_usage(fact.latest_settlement_usage_status, fact.latest_total_tokens),
        settled_cost_micros:
          settled_cost(
            fact.latest_settlement_usage_status,
            fact.latest_settlement_pricing_status,
            fact.latest_settled_cost_micros
          ),
        settled_cost_available:
          settled_cost_available(
            fact.latest_settlement_usage_status,
            fact.latest_settlement_pricing_status
          ),
        estimated_cost_micros:
          estimated_cost(
            fact.latest_settlement_usage_status,
            fact.latest_settlement_pricing_status,
            fact.latest_estimated_cost_micros
          ),
        estimated_cost_available:
          estimated_cost_available(
            fact.latest_settlement_usage_status,
            fact.latest_settlement_pricing_status,
            fact.latest_estimated_cost_micros
          ),
        cost_unavailable:
          cost_unavailable(
            fact.latest_settlement_usage_status,
            fact.latest_settlement_pricing_status,
            fact.latest_estimated_cost_micros
          )
      }
    )
    |> select_merge(^outcome_flags())
  end

  @doc """
  One keyset page of the most recent scoped outcomes, with one lookahead row.
  Admission time and id provide a deterministic cursor even when timestamps tie.

  This is a dedicated query rather than a slice of `scoped_facts/2` so the
  endpoint/error-code regex classification runs over only the rows the
  limit keeps, not the whole window. `requests_api_key_pool_admitted_id_idx`
  yields the scoped rows in the requested deterministic order, so the limit
  stops early and the fact/model joins are bounded primary-key lookups.
  """
  def recent_outcomes(identity, window, cursor \\ nil) do
    from(request in Request,
      as: :request,
      left_join: fact in RequestLogFact,
      on: fact.request_id == request.id,
      left_join: model in Model,
      on: model.id == request.model_id and model.pool_id == request.pool_id,
      where:
        request_scope(
          request,
          ^identity.pool_id,
          ^identity.api_key_id,
          ^window.started_at,
          ^window.ended_at
        ),
      order_by: [desc: request.admitted_at, desc: request.id],
      limit: 201,
      select: %{
        id: request.id,
        user_agent: request.user_agent,
        timestamp: request.admitted_at,
        model: model_label(model.exposed_model_id),
        model_display_name: model_name(model.display_name, model.exposed_model_id),
        endpoint_class: endpoint_class(request.endpoint),
        response_status_code: request.response_status_code,
        total_tokens: known_usage(fact.latest_settlement_usage_status, fact.latest_total_tokens),
        input_tokens: known_usage(fact.latest_settlement_usage_status, fact.latest_input_tokens),
        cached_input_tokens: known_usage(fact.latest_settlement_usage_status, fact.latest_cached_input_tokens),
        output_tokens: known_usage(fact.latest_settlement_usage_status, fact.latest_output_tokens),
        reasoning_effort: request.reasoning_effort,
        service_tier: request.service_tier,
        requested_service_tier: request.requested_service_tier,
        actual_service_tier: request.actual_service_tier,
        settled_cost_micros:
          settled_cost(
            fact.latest_settlement_usage_status,
            fact.latest_settlement_pricing_status,
            fact.latest_settled_cost_micros
          ),
        settled_cost_available:
          settled_cost_available(
            fact.latest_settlement_usage_status,
            fact.latest_settlement_pricing_status
          ),
        estimated_cost_micros:
          estimated_cost(
            fact.latest_settlement_usage_status,
            fact.latest_settlement_pricing_status,
            fact.latest_estimated_cost_micros
          ),
        estimated_cost_available:
          estimated_cost_available(
            fact.latest_settlement_usage_status,
            fact.latest_settlement_pricing_status,
            fact.latest_estimated_cost_micros
          )
      }
    )
    |> before_cursor(cursor)
    |> select_merge(^outcome_columns())
  end

  defp before_cursor(query, nil), do: query

  defp before_cursor(query, %{timestamp: timestamp, id: id}) do
    where(query, [request: request], fragment("ROW(?, ?) < ROW(?, ?)", request.admitted_at, request.id, type(^timestamp, :utc_datetime_usec), type(^id, :binary_id)))
  end

  # A request the client cancelled (`RequestOutcome`) is counted apart and is
  # not a failure, so it leaves `failed` and the success rate's base.
  defp outcome_flags do
    %{
      failed: dynamic(fragment("CASE WHEN ? THEN 1 ELSE 0 END", ^failure_condition())),
      client_cancelled: dynamic(fragment("CASE WHEN ? THEN 1 ELSE 0 END", ^RequestOutcome.client_cancelled_condition(:request)))
    }
  end

  # The status a holder is shown is the outcome class, and only a failure carries
  # a reason: a client cancellation is not one (its `client_disconnected` code
  # would read as a service failure to the regex families of `safe_code/1`), and
  # a late answer that corrected an interrupt to a success keeps its old code.
  defp outcome_columns do
    %{
      status: dynamic([request: request], fragment("CASE WHEN ? THEN ? ELSE ? END", ^RequestOutcome.client_cancelled_condition(:request), ^RequestOutcome.client_cancelled(), request.status)),
      code: dynamic([request: request], fragment("CASE WHEN ? THEN ? END", ^failure_condition(), safe_code(request.last_error_code)))
    }
  end

  defp failure_condition do
    not_client_cancelled = RequestOutcome.not_client_cancelled_condition(:request)
    dynamic([request: request], request.status in ^@failed_statuses and ^not_client_cancelled)
  end
end
