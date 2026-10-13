defmodule CodexPooler.Gateway.Transports.RolloutDrainFailureSupport do
  @moduledoc false
  import ExUnit.Callbacks
  import ExUnit.Assertions

  alias CodexPooler.Gateway.Transports.Websocket.{ActivityRegistry, RolloutDrain}

  @budget 10_000

  @spec hold_call!(GenServer.server(), atom()) :: {reference(), pid()}
  def hold_call!(server, phase) do
    ref = make_ref()
    pid = GenServer.whereis(server)

    on_exit(fn ->
      try do
        :sys.remove(pid, ref)
      catch
        :exit, _reason -> :ok
      end
    end)

    :ok = :sys.install(pid, {ref, &__MODULE__.hold_call/3, %{ref: ref, parent: self(), phase: phase}})
    {ref, pid}
  end

  @doc false
  @spec hold_call(map(), term(), term()) :: map() | :done
  def hold_call(probe, {:in, {:"$gen_call", {caller, _tag}, request}}, _name) do
    if phase(request) == probe.phase do
      monitor = Process.monitor(probe.parent)
      send(probe.parent, {probe.ref, :call_held, self(), caller})
      ref = probe.ref

      receive do
        {^ref, :release} ->
          Process.demonitor(monitor, [:flush])
          :done

        {:DOWN, ^monitor, :process, _pid, _reason} ->
          :done
      after
        @budget -> exit(:owned_registry_barrier_not_released)
      end
    else
      probe
    end
  end

  def hold_call(probe, _event, _name), do: probe

  defp phase(:begin_drain), do: :begin
  defp phase({:begin_drain, _}), do: :begin
  defp phase({:status, _}), do: :status
  defp phase(:receipt_cohort), do: :receipts
  defp phase({:cancel, _, _, _}), do: :cancel
  defp phase({:complete_drain, _}), do: :complete
  defp phase({:start_drain, _, _}), do: :join
  defp phase({:drain_entries, _}), do: :entries
  defp phase(:drain_entries), do: :entries
  defp phase(_), do: :other

  @spec start_activity!(GenServer.server(), :direct | :proxy) :: {Task.t(), pid(), reference()}
  def start_activity!(registry, kind \\ :direct) do
    parent = self()
    supervisor = start_supervised!({Task.Supervisor, []}, id: make_ref())

    task =
      Task.Supervisor.async_nolink(supervisor, fn ->
        {:ok, token} = ActivityRegistry.register(kind, self(), name: registry)
        :ok = ActivityRegistry.admit(token, name: registry)
        send(parent, {:owned_activity, self(), token})

        receive do
          :finish -> ActivityRegistry.unregister(token, :completed, name: registry)
          :stop -> :ok
          {:websocket_activity_cancel, ^token, :owner_drained} -> ActivityRegistry.unregister(token, :aborted, name: registry)
        end
      end)

    assert_receive {:owned_activity, pid, token}, @budget
    {task, pid, token}
  end

  @spec request!(GenServer.server(), keyword()) :: Task.t()
  def request!(name, opts \\ []) do
    supervisor = start_supervised!({Task.Supervisor, []}, id: make_ref())

    Task.Supervisor.async_nolink(supervisor, fn ->
      try do
        {:summary, RolloutDrain.start_drain([name: name, timeout_ms: 100, deadline_margin_ms: 0, deadline_floor_ms: 1] ++ opts)}
      catch
        :exit, _reason -> :call_exited
      end
    end)
  end

  @spec result(Task.t()) :: map() | atom()
  def result(task) do
    case Task.yield(task, 1_500) do
      {:ok, {:summary, summary}} -> summary
      {:ok, other} -> other
      {:exit, _} -> :task_exited
      nil -> :no_summary
    end
  end

  @doc false
  @spec observe_result(map(), term(), term()) :: map()
  def observe_result(probe, {:in, {:"$gen_call", _from, {:rollout_drain_work_result, _ref, {:activity, _entry}, {:activity, :direct, :completed}}}}, _name) do
    send(probe.parent, {:completed_cohort_observed, probe.ref})
    probe
  end

  def observe_result(probe, _event, _name), do: probe

  @doc false
  @spec start_shutdown_activity(pid()) :: {:ok, pid()} | {:error, term()}
  def start_shutdown_activity(parent) do
    publisher = CodexPooler.Platform.ExecutionProofPublisher
    Application.put_env(:codex_pooler, publisher, enabled: true)
    {:ok, _publisher} = Supervisor.restart_child(CodexPooler.Supervisor, publisher)

    child = %{
      id: :owned_shutdown_activity,
      start:
        {Task, :start_link,
         [
           fn ->
             {:ok, token} = ActivityRegistry.register(:direct)
             :ok = ActivityRegistry.admit(token)
             send(parent, {:shutdown_activity_ready, self()})

             receive do
               :never -> :ok
             end
           end
         ]},
      restart: :temporary
    }

    Supervisor.start_child(CodexPooler.Supervisor, child)
  end

  @doc false
  @spec observe_shutdown(map(), term(), term()) :: map()
  # A receipt worker exit can end the coordinator without its finished message.
  # Observe the summary the server actually returned, including that fallback.
  def observe_shutdown(probe, {:noreply, %{active_drain: nil, last_summary: summary}}, _name) when is_map(summary) do
    send(probe.parent, {:shutdown_drain_summary, Map.take(summary, [:result, :direct_turns_seen, :direct_turns_failed])})
    probe
  end

  def observe_shutdown(probe, {:in, {:"$gen_call", _from, :flush}}, _name) do
    send(probe.parent, :shutdown_proof_flush_reached)
    probe
  end

  def observe_shutdown(probe, _event, _name), do: probe
end
