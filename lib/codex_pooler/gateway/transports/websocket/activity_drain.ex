defmodule CodexPooler.Gateway.Transports.Websocket.ActivityDrain do
  @moduledoc false

  alias CodexPooler.Gateway.Transports.Websocket.ActivityRegistry
  alias CodexPooler.Platform.ExecutionRegistry

  @poll_interval_ms 200

  @type outcome :: :completed | :aborted | :failed
  @type policy :: %{
          required(:now_ms) => (-> integer()),
          required(:schedule_wait) => (pid(), reference(), non_neg_integer() -> term()),
          required(:cancel_wait) => (term(), reference() -> :ok),
          required(:owner_post_deadline_call_budget_ms) => pos_integer()
        }

  @spec drain(ActivityRegistry.drain_entry(), integer(), policy(), GenServer.server()) ::
          outcome()
  def drain(%{status: {:finished, outcome}}, _deadline, _policy, _registry), do: outcome

  def drain(%{token: token, pid: pid}, deadline_ms, policy, registry) do
    monitor = Process.monitor(pid)

    try do
      case ActivityRegistry.status(token, name: registry) do
        {:finished, outcome} -> outcome
        {:active, _status} -> await(token, monitor, deadline_ms, policy, registry)
        :unknown -> :failed
      end
    catch
      :exit, _reason -> :failed
    after
      Process.demonitor(monitor, [:flush])
    end
  end

  @doc "Drains dynamically registered receipt work, closing only after every producer has handed off."
  @spec drain_receipts(integer(), policy(), GenServer.server(), (map() -> term())) :: map()
  def drain_receipts(deadline_ms, policy, registry, observe) do
    cohort = ActivityRegistry.receipt_cohort(name: registry)
    observe.(cohort)
    now_ms = policy.now_ms.()
    active = Enum.filter(cohort.entries, &(&1.status == :active))
    settlement_deadline = deadline_ms + policy.owner_post_deadline_call_budget_ms

    cond do
      cohort.closed? and active == [] ->
        cohort

      now_ms >= settlement_deadline ->
        :ok = ActivityRegistry.force_receipt_cohort(name: registry)
        cohort = ActivityRegistry.receipt_cohort(name: registry)
        observe.(cohort)
        cohort

      true ->
        if now_ms >= deadline_ms do
          Enum.each(active, &ActivityRegistry.cancel(&1.token, :owner_drained, name: registry))
        end

        wait_token = make_ref()
        until_ms = if now_ms < deadline_ms, do: deadline_ms, else: settlement_deadline
        policy.schedule_wait.(self(), wait_token, min(@poll_interval_ms, max(0, until_ms - now_ms)))

        receive do
          {:rollout_drain_wait_elapsed, ^wait_token} -> drain_receipts(deadline_ms, policy, registry, observe)
        end
    end
  end

  defp await(token, monitor, deadline_ms, policy, registry) do
    remaining_ms = max(0, deadline_ms - policy.now_ms.())

    if remaining_ms == 0 do
      cancel(token, monitor, policy, registry)
    else
      case wait_or_down(monitor, policy, min(@poll_interval_ms, remaining_ms)) do
        :process_down -> activity_outcome(token, registry)
        :elapsed -> await(token, monitor, deadline_ms, policy, registry)
        :wait_failed -> :failed
      end
    end
  end

  defp cancel(token, monitor, policy, registry) do
    budget_ms = policy.owner_post_deadline_call_budget_ms

    case ActivityRegistry.cancel(token, :owner_drained, name: registry) do
      :ok -> await_cancelled(token, monitor, budget_ms, registry)
      :terminal_delivered -> await_delivered_settlement(token, monitor, budget_ms, registry)
    end
  end

  # The socket already pushed this turn's terminal, so only its settlement is
  # left (findings#287): the task gets half the post-deadline budget to settle
  # and finish, and counts as the turn it settled. One still running then is
  # cut as the deadline cuts a live turn, with the other half left for it to
  # stop before it is killed, so the budget as a whole is unchanged.
  defp await_delivered_settlement(token, monitor, budget_ms, registry) do
    settle_ms = div(budget_ms, 2)

    receive do
      {:DOWN, ^monitor, :process, _pid, _reason} -> activity_outcome(token, registry)
    after
      settle_ms ->
        :ok = ActivityRegistry.cancel(token, :owner_drained, name: registry, force: true)
        await_cancelled(token, monitor, budget_ms - settle_ms, registry)
    end
  end

  defp await_cancelled(token, monitor, wait_ms, registry) do
    receive do
      {:DOWN, ^monitor, :process, _pid, _reason} -> activity_outcome(token, registry)
    after
      wait_ms -> force_cancel(token, registry)
    end
  end

  defp force_cancel(token, registry) do
    case ActivityRegistry.activities(name: registry) |> Enum.find(&(&1.token == token)) do
      %{pid: pid} when is_pid(pid) ->
        _marked = ExecutionRegistry.mark_interruption(pid, "owner_drained")
        Process.exit(pid, :kill)

      _finished ->
        :ok
    end

    activity_outcome(token, registry)
  end

  defp activity_outcome(token, registry) do
    case ActivityRegistry.status(token, name: registry) do
      {:finished, outcome} -> outcome
      {:active, :cancelling} -> :aborted
      {:active, _status} -> :failed
      :unknown -> :failed
    end
  end

  defp wait_or_down(monitor, policy, wait_ms) do
    wait_token = make_ref()

    try do
      wait_ref = policy.schedule_wait.(self(), wait_token, wait_ms)

      receive do
        {:DOWN, ^monitor, :process, _pid, _reason} ->
          :ok = policy.cancel_wait.(wait_ref, wait_token)
          :process_down

        {:rollout_drain_wait_elapsed, ^wait_token} ->
          :elapsed
      end
    catch
      _kind, _reason -> :wait_failed
    end
  end
end
