defmodule CodexPooler.Dev.CodexRetainedUsageFixtureDrainTrace do
  @moduledoc false
  use GenServer
  alias CodexPooler.Gateway.Transports.Websocket.ActivityRegistry

  @pattern {ActivityRegistry, :handle_call, 3}

  @spec start(pid(), pid(), (-> map()), pos_integer()) :: GenServer.on_start()
  def start(fixture, registry, run, timeout), do: GenServer.start(__MODULE__, {fixture, registry, run, timeout})

  @spec stop(pid() | nil) :: :ok
  def stop(nil), do: :ok

  def stop(pid) do
    GenServer.call(pid, :stop)
  catch
    :exit, {:noproc, _} -> :ok
    :exit, {:normal, _} -> :ok
  end

  @impl true
  def init({fixture, registry, run, timeout}) do
    {:traced, false} = :erlang.trace_info(@pattern, :traced)
    {:flags, flags} = :erlang.trace_info(registry, :flags)
    false = :call in flags
    :erlang.trace_pattern(@pattern, [{[:receipt_cohort, :_, :_], [], [{:return_trace}]}], [:local])
    :erlang.trace(registry, true, [:call, {:tracer, self()}])
    fixture_monitor = Process.monitor(fixture)
    guardian = self()
    {caller, caller_monitor} = spawn_monitor(fn -> send(guardian, {:result, self(), run.()}) end)
    timer = Process.send_after(self(), :deadline, timeout)
    {:ok, %{fixture: fixture, fixture_monitor: fixture_monitor, registry: registry, caller: caller, caller_monitor: caller_monitor, timer: timer, result: nil, delivery: nil}}
  end

  @impl true
  def handle_call(:stop, _from, state) do
    finish(state, failed("drain_fixture_stopped"))
    {:stop, :normal, :ok, state}
  end

  @impl true
  def handle_info({:result, caller, result}, %{caller: caller} = state) do
    delivery = :erlang.trace_delivered(state.registry)
    {:noreply, %{state | result: result, delivery: delivery}}
  end

  def handle_info({:trace_delivered, registry, delivery}, %{registry: registry, delivery: delivery, result: result} = state) do
    finish(state, result)
    {:stop, :normal, state}
  end

  def handle_info({:trace, registry, :return_from, @pattern, {:reply, %{closed?: true}, %{drain: %{epoch: epoch}}}}, %{registry: registry} = state) do
    send(state.fixture, {:drain_cohort_closed, self(), epoch})
    {:noreply, state}
  end

  def handle_info({:DOWN, monitor, :process, _pid, _reason}, %{fixture_monitor: monitor} = state) do
    finish(state, failed("drain_fixture_down"))
    {:stop, :normal, state}
  end

  def handle_info({:DOWN, monitor, :process, _pid, _reason}, %{caller_monitor: monitor, result: nil} = state) do
    finish(state, failed("drain_caller_exit"))
    {:stop, :normal, state}
  end

  def handle_info(:deadline, state) do
    finish(state, failed("drain_fixture_deadline"))
    {:stop, :normal, state}
  end

  def handle_info(_message, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, state), do: cleanup(state)

  defp finish(state, result) do
    cleanup(state)
    send(state.fixture, {:drain_finished, self(), result})
  end

  defp cleanup(state) do
    Process.cancel_timer(state.timer)
    if Process.alive?(state.registry), do: :erlang.trace(state.registry, false, [:call])
    :erlang.trace_pattern(@pattern, false, [:local])
    stop_caller(state.caller)
    Process.demonitor(state.caller_monitor, [:flush])
    Process.demonitor(state.fixture_monitor, [:flush])
    :ok
  end

  defp stop_caller(caller) do
    monitor = Process.monitor(caller)
    Process.exit(caller, :kill)

    receive do
      {:DOWN, ^monitor, :process, ^caller, _} -> :ok
    end
  end

  defp failed(reason), do: %{result: "error", fixture_failure: reason}
end
