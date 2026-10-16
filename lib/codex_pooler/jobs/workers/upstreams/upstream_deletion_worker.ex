defmodule CodexPooler.Jobs.UpstreamDeletionWorker do
  @moduledoc "Permanently removes an upstream after admitted work drains, with bounded shared database batches."

  use Oban.Worker,
    queue: :jobs,
    max_attempts: 5,
    tags: ["upstream_deletion"],
    unique: [fields: [:args, :queue, :worker], keys: [:upstream_identity_id], states: :incomplete, period: :infinity]

  alias CodexPooler.Upstreams

  require Logger

  @impl Oban.Worker
  def timeout(%Oban.Job{}), do: :timer.minutes(2)

  @impl Oban.Worker
  def perform(%Oban.Job{id: job_id, args: %{"upstream_identity_id" => identity_id} = args}) when is_binary(identity_id) do
    case Upstreams.continue_account_deletion(identity_id, Map.get(args, "requested_by_user_id"), System.monotonic_time(:millisecond) + 45_000) do
      :more ->
        {:snooze, 1}

      :deleted ->
        :ok

      :gone ->
        :ok

      {:cancel, reason} ->
        {:cancel, reason}

      {:error, %{code: :upstream_deletion_busy}} ->
        Logger.warning("upstream deletion deferred job_id=#{job_id} reason=upstream_deletion_busy retry_in_seconds=30")
        {:snooze, 30}

      {:error, reason} ->
        {:error, reason}
    end
  end

  def perform(%Oban.Job{}), do: {:cancel, :upstream_deletion_target_invalid}
end
