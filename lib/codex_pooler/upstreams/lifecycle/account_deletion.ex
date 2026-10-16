defmodule CodexPooler.Upstreams.Lifecycle.AccountDeletion do
  @moduledoc """
  Permanently removes an upstream account after fencing new work and draining admitted requests.

  The identity marker and unique Oban job commit together. Small idle accounts disappear in the
  calling request; larger accounts are detached from shared accounting in bounded batches. The
  marker prevents a credential import from reviving a partially deleted account. Every batch
  takes the same database identity lock, so workers on different nodes serialize naturally.
  """

  import Ecto.Query

  alias CodexPooler.Accounting.{Attempt, Request}
  alias CodexPooler.Accounts.{Scope, User}
  alias CodexPooler.{Alerts, Audit, Events, Pools, Repo}
  alias CodexPooler.Jobs.{DeletionDeadline, UpstreamDeletionWorker}
  alias CodexPooler.Upstreams.Lifecycle.IdentitySlotLock
  alias CodexPooler.Upstreams.Schemas.{PoolUpstreamAssignment, UpstreamIdentity}
  alias CodexPooler.Upstreams.Secrets

  @marker "permanent_deletion_requested_at"
  @worker_name "CodexPooler.Jobs.UpstreamDeletionWorker"
  @pending_states ~w(available scheduled executing retryable)
  @immediate_row_limit 2_000
  @immediate_timeout_ms 5_000
  @batch_timeout_ms 10_000
  @batch_size 500
  @batch_size_key "permanent_deletion_batch_size"

  # Preserve shared request accounting and replay provenance. Assignment-owned state is left
  # for the final foreign-key cascade; no secret is copied into the job or deletion receipt.
  # Detach BOTH identity and assignment references before deleting the identity: the
  # cascading assignment delete must never race the sibling identity SET NULL actions.
  @detach_steps [
    {"ledger_entries", "upstream_identity_id", :identity},
    {"ledger_entries", "pool_upstream_assignment_id", :assignments},
    {"attempts", "upstream_identity_id", :identity},
    {"attempts", "pool_upstream_assignment_id", :assignments},
    {"request_log_facts", "latest_upstream_identity_id", :identity},
    {"request_log_facts", "latest_pool_upstream_assignment_id", :assignments},
    {"codex_sessions", "pool_upstream_assignment_id", :assignments},
    {"codex_files", "upstream_identity_id", :identity},
    {"codex_files", "pool_upstream_assignment_id", :assignments}
  ]

  @type state :: :in_progress | :failed
  @type error :: %{required(:code) => atom(), required(:message) => String.t()}
  @type receipt :: %{status: :deleted, identity: UpstreamIdentity.t(), assignments: [PoolUpstreamAssignment.t()], secret_status: :missing}
  @type request_result :: {:ok, receipt()} | {:deleting, receipt()} | {:error, error() | term()}
  @type continue_result :: :deleted | :gone | :more | {:cancel, atom()} | {:error, term()}

  @spec request(Scope.t(), UpstreamIdentity.t() | Ecto.UUID.t(), map()) :: request_result()
  def request(scope, identity_or_id, attrs \\ %{})

  def request(%Scope{} = scope, identity_or_id, attrs) when is_map(attrs) do
    with {:ok, identity} <- authorize(scope, identity_or_id),
         {:ok, receipt} <- mark_requested(scope, identity.id, attrs) do
      case try_immediate(receipt.identity.id, scope.user.id) do
        :deleted -> {:ok, receipt}
        :gone -> {:ok, receipt}
        _queued -> {:deleting, receipt}
      end
    end
  end

  def request(_scope, _identity, _attrs), do: {:error, error(:invalid_request, "user scope is required")}

  @spec authorize(Scope.t(), UpstreamIdentity.t() | Ecto.UUID.t()) :: {:ok, UpstreamIdentity.t()} | {:error, error()}
  def authorize(%Scope{} = scope, identity_or_id) do
    with {:ok, id} <- identity_id(identity_or_id),
         %UpstreamIdentity{} = identity <- Repo.get(UpstreamIdentity, id),
         assignments = assignments(identity.id),
         :ok <- authorize_assignments(scope, assignments),
         :ok <- require_unassigned(assignments) do
      {:ok, identity}
    else
      nil -> {:error, error(:upstream_identity_not_found, "upstream account was not found")}
      {:error, _reason} = result -> result
    end
  end

  def authorize(_scope, _identity), do: {:error, error(:invalid_request, "user scope is required")}

  @spec permissions(Scope.t(), [Ecto.UUID.t()]) :: %{Ecto.UUID.t() => boolean()}
  def permissions(scope, identity_ids) do
    existing = Repo.all(from identity in UpstreamIdentity, where: identity.id in ^identity_ids, select: identity.id) |> MapSet.new()
    owner? = Pools.owner?(scope)
    visible_pools = scope |> Pools.list_visible_pools() |> MapSet.new(& &1.id)

    assignments_by_identity =
      Repo.all(from assignment in PoolUpstreamAssignment, where: assignment.upstream_identity_id in ^identity_ids, select: %{upstream_identity_id: assignment.upstream_identity_id, pool_id: assignment.pool_id, status: assignment.status})
      |> Enum.group_by(& &1.upstream_identity_id)

    Map.new(identity_ids, fn id ->
      assignments = Map.get(assignments_by_identity, id, [])
      pool_ids = Enum.map(assignments, & &1.pool_id)
      allowed? = owner? or (pool_ids != [] and Enum.all?(pool_ids, &MapSet.member?(visible_pools, &1)))
      {id, MapSet.member?(existing, id) and allowed? and require_unassigned(assignments) == :ok}
    end)
  end

  defp mark_requested(scope, identity_id, attrs) do
    transaction(fn -> mark_locked_request(scope, identity_id, attrs) end, @immediate_timeout_ms)
  rescue
    _error in [Postgrex.Error, DBConnection.ConnectionError] ->
      {:error, error(:upstream_deletion_unavailable, "upstream deletion could not be queued; retry Delete")}
  end

  defp mark_locked_request(scope, identity_id, attrs) do
    %{identities: identities, assignments: assignments} = IdentitySlotLock.lock_identity_rows!([identity_id])

    with [identity] <- identities,
         :ok <- authorize_assignments(scope, assignments),
         :ok <- require_unassigned(assignments),
         :ok <- validate_confirmation(identity, attrs) do
      timestamp = DateTime.utc_now() |> DateTime.truncate(:microsecond)
      metadata = Map.put_new(identity.metadata || %{}, @marker, DateTime.to_iso8601(timestamp))
      identity = identity |> Ecto.Changeset.change(status: "deleted", disabled_at: identity.disabled_at || timestamp, updated_at: timestamp, metadata: metadata) |> Repo.update!()
      Secrets.revoke_active_secrets(identity.id, timestamp)

      job =
        case Oban.insert(UpstreamDeletionWorker.new(%{"upstream_identity_id" => identity.id, "requested_by_user_id" => scope.user.id}), retry: false) do
          {:ok, job} -> job
          {:error, _reason} -> Repo.rollback(error(:upstream_deletion_unavailable, "upstream deletion could not be queued; retry Delete"))
        end

      unless job.conflict?, do: audit!(scope.user, identity, assignments, "upstream_account.delete_requested")
      broadcast!(identity, assignments, "upstream_account_deletion_requested")
      receipt(identity, assignments)
    else
      [] -> Repo.rollback(error(:upstream_identity_not_found, "upstream account was not found"))
      {:error, reason} -> Repo.rollback(reason)
    end
  end

  defp try_immediate(identity_id, actor_id) do
    case transaction(
           fn ->
             with_locked_target(identity_id, &delete_small_idle_account(&1, &2, actor_id))
           end,
           @immediate_timeout_ms
         ) do
      {:ok, result} -> result
      {:error, _reason} -> :more
    end
  end

  defp delete_small_idle_account(identity, assignments, actor_id) do
    cond do
      live_work?(identity.id) ->
        :more

      not small_history?(identity, assignments) ->
        :more

      true ->
        Enum.each(@detach_steps, fn {table, field, reference} ->
          ids = reference_ids(reference, identity, assignments)
          Repo.query!("UPDATE #{table} SET #{field} = NULL WHERE #{field} = ANY($1::uuid[])", [ids])
        end)

        delete_identity!(identity, assignments, actor_id)
    end
  end

  @spec continue(Ecto.UUID.t(), Ecto.UUID.t() | nil, integer()) :: continue_result()
  def continue(identity_id, actor_id, deadline) do
    DeletionDeadline.run(deadline, fn -> continue_until_deadline(identity_id, actor_id, deadline) end)
  end

  defp continue_until_deadline(identity_id, actor_id, deadline) do
    with :done <- detach_history(@detach_steps, identity_id, deadline),
         true <- DeletionDeadline.remaining(deadline) > 0 do
      transaction(fn -> with_locked_target(identity_id, &finish_if_idle(&1, &2, actor_id)) end, @batch_timeout_ms)
      |> unwrap_result()
    else
      false -> :more
      result -> result
    end
  end

  defp detach_history([], _identity_id, _deadline), do: :done

  defp detach_history([{table, field, reference} | rest] = steps, identity_id, deadline) do
    if DeletionDeadline.remaining(deadline) == 0 do
      :more
    else
      result = detach_batch(identity_id, table, field, reference)

      case result do
        {:ok, {:batch, count, limit}} when count < limit -> detach_history(rest, identity_id, deadline)
        {:ok, {:batch, _count, _limit}} -> detach_history(steps, identity_id, deadline)
        {:ok, result} -> result
        {:error, reason} -> {:error, reason}
      end
    end
  end

  defp detach_batch(identity_id, table, field, reference) do
    result =
      transaction(
        fn ->
          with_locked_target(identity_id, fn identity, assignments -> detach_idle_batch(identity, assignments, table, field, reference) end)
        end,
        @batch_timeout_ms
      )

    case result do
      {:error, {:batch_canceled, limit}} ->
        persist_smaller_batch(identity_id, limit)

      result ->
        result
    end
  end

  defp detach_idle_batch(identity, assignments, table, field, reference) do
    if live_work?(identity.id) do
      :more
    else
      # ANY keeps the field ordering meaningful even for one identity. The existing
      # reference index can satisfy ORDER BY without scanning unrelated history when
      # LIMIT and stale/generic estimates would otherwise favor a sequential scan.
      ids = reference_ids(reference, identity, assignments)
      limit = batch_size(identity)
      sql = "UPDATE #{table} SET #{field} = NULL WHERE ctid = ANY(ARRAY(SELECT ctid FROM #{table} WHERE #{field} = ANY($1::uuid[]) ORDER BY #{field} LIMIT $2))"

      try do
        %{num_rows: count} = Repo.query!(sql, [ids, limit], timeout: @batch_timeout_ms + 1_000)
        {:batch, count, limit}
      rescue
        exception in Postgrex.Error ->
          if match?(%Postgrex.Error{postgres: %{code: :query_canceled}}, exception), do: Repo.rollback({:batch_canceled, limit}), else: reraise(exception, __STACKTRACE__)
      end
    end
  end

  defp persist_smaller_batch(identity_id, failed_limit) do
    # The failed batch has rolled back. Commit learning separately, under the same
    # identity lock, so a later worker or another node resumes at the smaller size.
    result =
      transaction(
        fn ->
          with_locked_target(identity_id, fn identity, _assignments ->
            limit = min(batch_size(identity), max(div(failed_limit, 2), 1))
            metadata = Map.put(identity.metadata, @batch_size_key, limit)
            identity |> Ecto.Changeset.change(metadata: metadata) |> Repo.update!()
            :reduced
          end)
        end,
        @batch_timeout_ms
      )

    case result do
      {:ok, :reduced} -> {:error, error(:upstream_deletion_busy, "upstream deletion is waiting for database work to finish")}
      result -> result
    end
  end

  defp batch_size(%UpstreamIdentity{metadata: metadata}) do
    case Map.get(metadata, @batch_size_key) do
      limit when is_integer(limit) and limit >= 1 and limit <= @batch_size -> limit
      _invalid -> @batch_size
    end
  end

  defp with_locked_target(identity_id, operation) do
    case IdentitySlotLock.lock_identity_rows!([identity_id]) do
      %{identities: []} ->
        :gone

      %{identities: [%UpstreamIdentity{status: "deleted", metadata: %{@marker => _}} = identity], assignments: assignments} ->
        with :ok <- require_unassigned(assignments), do: operation.(identity, assignments)

      _other ->
        {:cancel, :upstream_account_not_deleting}
    end
  end

  defp finish_if_idle(identity, assignments, actor_id) do
    if live_work?(identity.id), do: :more, else: delete_identity!(identity, assignments, actor_id)
  end

  defp delete_identity!(identity, assignments, actor_id) do
    actor = if is_binary(actor_id), do: Repo.get(User, actor_id)
    audit!(actor, identity, assignments, "upstream_account.delete")

    case Alerts.invalidate_notifications_after_cascade({:upstream_identity, identity.id}, fn -> Repo.delete(identity) end) do
      {:ok, _deleted} ->
        broadcast!(identity, assignments, "upstream_account_deleted")
        :deleted

      {:error, reason} ->
        Repo.rollback(reason)
    end
  end

  defp live_work?(identity_id) do
    assignment_ids = from assignment in PoolUpstreamAssignment, where: assignment.upstream_identity_id == ^identity_id, select: assignment.id

    target_attempts =
      from attempt in Attempt,
        where: attempt.upstream_identity_id == ^identity_id or attempt.pool_upstream_assignment_id in subquery(assignment_ids)

    Repo.exists?(from attempt in target_attempts, where: attempt.status in ["queued", "in_progress"]) or
      Repo.exists?(
        from request in Request,
          as: :live_request,
          where: request.status in ["accepted", "in_progress"],
          where: exists(from attempt in target_attempts, where: attempt.request_id == parent_as(:live_request).id, offset: 0, select: 1)
      )
  end

  defp small_history?(identity, assignments) do
    Enum.all?(@detach_steps, fn {table, field, reference} ->
      ids = reference_ids(reference, identity, assignments)
      %{rows: [[count]]} = Repo.query!("SELECT count(*) FROM (SELECT 1 FROM #{table} WHERE #{field} = ANY($1::uuid[]) ORDER BY #{field} LIMIT $2) bounded", [ids, immediate_row_limit()])
      count < immediate_row_limit()
    end)
  end

  defp reference_ids(:identity, identity, _assignments), do: [Ecto.UUID.dump!(identity.id)]
  defp reference_ids(:assignments, _identity, assignments), do: Enum.map(assignments, &Ecto.UUID.dump!(&1.id))

  defp authorize_assignments(scope, assignments) do
    cond do
      Pools.owner?(scope) ->
        :ok

      assignments == [] ->
        {:error, error(:capability_denied, "instance owner access is required to delete an unassigned account")}

      true ->
        assignments
        |> Enum.map(& &1.pool_id)
        |> Enum.uniq()
        |> Enum.reduce_while(:ok, &authorize_pool(scope, &1, &2))
    end
  end

  defp authorize_pool(scope, pool_id, :ok) do
    case Pools.require_capability(scope, Pools.capability(:pool_operate), pool_id: pool_id) do
      {:ok, _decision} -> {:cont, :ok}
      {:error, reason} -> {:halt, {:error, reason}}
    end
  end

  defp require_unassigned(assignments) do
    if Enum.all?(assignments, &(&1.status == "deleted")) do
      :ok
    else
      {:error, error(:upstream_account_assigned, "remove this account from all Pools before deleting it")}
    end
  end

  defp validate_confirmation(identity, attrs) do
    case Map.fetch(attrs, :confirmation_label) do
      :error ->
        :ok

      {:ok, label} when is_binary(label) ->
        expected = Enum.find([identity.account_label, identity.chatgpt_account_id], "Upstream account", &(is_binary(&1) and String.trim(&1) != ""))
        if String.trim(label) == expected, do: :ok, else: {:error, error(:confirmation_mismatch, "type the current account label exactly")}

      _invalid ->
        {:error, error(:confirmation_mismatch, "type the current account label exactly")}
    end
  end

  defp audit!(actor, identity, assignments, action) do
    pool_ids = assignments |> Enum.map(& &1.pool_id) |> Enum.uniq()

    Enum.each(if(pool_ids == [], do: [nil], else: pool_ids), fn pool_id ->
      attrs = %{pool_id: pool_id, action: action, target_type: "upstream_identity", target_id: identity.id, details: %{upstream_identity_id: identity.id, account_label: identity.account_label, assignment_count: length(assignments), permanent: true}}
      result = if actor, do: Audit.record_user_event(actor, attrs), else: Audit.record_system_event(attrs)

      case result do
        {:ok, _event} -> :ok
        {:error, reason} -> Repo.rollback(reason)
      end
    end)
  end

  defp broadcast!(identity, assignments, reason) do
    pool_ids = assignments |> Enum.map(& &1.pool_id) |> Enum.uniq()
    pool_ids = if pool_ids == [], do: Enum.map(Pools.list_active_pools(), & &1.id), else: pool_ids

    Enum.each(pool_ids, fn pool_id ->
      case Events.broadcast_upstreams_after_commit(pool_id, reason, %{upstream_identity_id: identity.id, upstream_status: "deleted"}) do
        {:ok, _event} -> :ok
        {:error, reason} -> Repo.rollback(reason)
      end
    end)
  end

  @spec broadcast_failed(Ecto.UUID.t()) :: :ok
  def broadcast_failed(identity_id) do
    case Repo.get(UpstreamIdentity, identity_id) do
      nil ->
        :ok

      identity ->
        transaction(fn -> broadcast!(identity, assignments(identity_id), "upstream_account_deletion_failed") end, @batch_timeout_ms)
        :ok
    end
  end

  @spec states([Ecto.UUID.t()]) :: %{Ecto.UUID.t() => state()}
  def states([]), do: %{}

  def states(identity_ids) do
    targets = Enum.reduce(identity_ids, dynamic(false), fn id, targets -> dynamic([job], ^targets or fragment("? @> ?", job.args, ^%{"upstream_identity_id" => id})) end)

    Repo.all(from job in Oban.Job, where: job.worker == ^@worker_name, where: ^targets, order_by: [desc: job.id], select: {fragment("?->>'upstream_identity_id'", job.args), job.state})
    |> Enum.group_by(&elem(&1, 0), &elem(&1, 1))
    |> Enum.reduce(%{}, fn {id, states}, acc ->
      cond do
        Enum.any?(states, &(&1 in @pending_states)) -> Map.put(acc, id, :in_progress)
        hd(states) in ["discarded", "cancelled"] -> Map.put(acc, id, :failed)
        true -> acc
      end
    end)
  end

  defp transaction(operation, timeout_ms) do
    Repo.transaction(
      fn ->
        Repo.query!("SELECT set_config('statement_timeout', $1, true)", ["#{timeout_ms}ms"])
        operation.()
      end,
      timeout: timeout_ms + 1_000
    )
  rescue
    error in Postgrex.Error ->
      case error do
        %Postgrex.Error{postgres: %{code: code}} when code in [:query_canceled, :deadlock_detected, :lock_not_available] -> {:error, error(:upstream_deletion_busy, "upstream deletion is waiting for database work to finish")}
        _other -> reraise error, __STACKTRACE__
      end
  end

  defp assignments(identity_id), do: Repo.all(from assignment in PoolUpstreamAssignment, where: assignment.upstream_identity_id == ^identity_id, order_by: assignment.id)
  defp receipt(identity, assignments), do: %{status: :deleted, identity: identity, assignments: assignments, secret_status: :missing}
  defp identity_id(%UpstreamIdentity{id: id}), do: identity_id(id)

  defp identity_id(value) do
    case Ecto.UUID.cast(value) do
      {:ok, id} -> {:ok, id}
      :error -> {:error, error(:upstream_identity_not_found, "upstream account was not found")}
    end
  end

  defp error(code, message), do: %{code: code, message: message}
  defp unwrap_result({:ok, result}), do: result
  defp unwrap_result({:error, _reason} = error), do: error

  if Mix.env() == :test do
    defp immediate_row_limit, do: Application.get_env(:codex_pooler, :upstream_deletion_immediate_row_limit, @immediate_row_limit)
  else
    defp immediate_row_limit, do: @immediate_row_limit
  end
end
