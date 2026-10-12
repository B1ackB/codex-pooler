defmodule CodexPooler.Gateway.Transports.Websocket.RolloutDrain do
  @moduledoc false

  use GenServer

  require Logger

  alias CodexPooler.Gateway.Transports.Streaming.{DeferredStreamDrain, DeferredStreamRegistry}
  alias CodexPooler.Telemetry.RelayRuntime

  alias CodexPooler.Gateway.Transports.Websocket.{
    ActivityDrain,
    ActivityRegistry,
    OwnerDefaults,
    WebsocketOwnerSession
  }

  @registry WebsocketOwnerSession.Registry
  @timeout_env "CODEX_POOLER_WEBSOCKET_DRAIN_TIMEOUT_MS"
  @default_timeout_ms 50_000
  @drain_poll_interval_ms 200
  @owner_call_timeout_ms OwnerDefaults.owner_call_timeout_ms()
  @default_owner_post_deadline_call_budget_ms @owner_call_timeout_ms * 2
  @owner_task_finish_margin_ms 500
  @drain_deadline_floor_ms 10

  @type summary :: %{
          required(:result) => :ok | :error,
          required(:owners_seen) => non_neg_integer(),
          required(:owners_drained) => non_neg_integer(),
          required(:owners_idle) => non_neg_integer(),
          required(:owners_failed) => non_neg_integer(),
          required(:turns_completed) => non_neg_integer(),
          required(:turns_aborted) => non_neg_integer(),
          required(:direct_turns_seen) => non_neg_integer(),
          required(:direct_turns_completed) => non_neg_integer(),
          required(:direct_turns_aborted) => non_neg_integer(),
          required(:direct_turns_failed) => non_neg_integer(),
          required(:proxy_turns_seen) => non_neg_integer(),
          required(:proxy_turns_completed) => non_neg_integer(),
          required(:proxy_turns_aborted) => non_neg_integer(),
          required(:proxy_turns_failed) => non_neg_integer(),
          required(:receipt_settlements_seen) => non_neg_integer(),
          required(:receipt_settlements_completed) => non_neg_integer(),
          required(:receipt_settlements_superseded) => non_neg_integer(),
          required(:receipt_settlements_aborted) => non_neg_integer(),
          required(:receipt_settlements_failed) => non_neg_integer(),
          required(:receipt_producers_failed?) => boolean(),
          required(:http_streams_seen) => non_neg_integer(),
          required(:http_streams_completed) => non_neg_integer(),
          required(:http_streams_aborted) => non_neg_integer(),
          required(:http_streams_failed) => non_neg_integer(),
          required(:timeout_ms) => pos_integer(),
          required(:elapsed_ms) => non_neg_integer(),
          required(:already_draining?) => boolean()
        }

  @type deadline :: %{
          required(:now_ms) => (-> integer()),
          required(:schedule_wait) => (pid(), reference(), non_neg_integer() -> term()),
          required(:cancel_wait) => (term(), reference() -> :ok)
        }

  @type option ::
          {:name, GenServer.server()}
          | {:timeout_ms, pos_integer()}
          | {:deadline, deadline()}
          | {:deadline_margin_ms, non_neg_integer()}
          | {:deadline_floor_ms, non_neg_integer()}
          | {:activity_registry, GenServer.server()}
          | {:stream_registry, GenServer.server()}
          | {:owner_registry, Registry.registry()}
          | {:owner_post_deadline_call_budget_ms, pos_integer()}
          | {:relay, GenServer.server()}

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @spec draining?([option()]) :: boolean()
  def draining?(opts \\ []) do
    opts
    |> configured_server_name()
    |> call_if_started(:draining?, false)
  end

  @spec start_drain([option()]) :: summary()
  def start_drain(opts \\ []) do
    timeout_ms = timeout_ms(opts)
    drain_policy = drain_policy(opts)

    call_drain(
      opts,
      {:start_drain, timeout_ms, drain_policy},
      timeout_ms,
      coordinator_call_timeout_ms(timeout_ms, drain_policy)
    )
  end

  @spec drain_for_shutdown() :: summary()
  def drain_for_shutdown do
    drain_for_shutdown(shutdown_timeout_ms())
  end

  @spec drain_for_shutdown(pos_integer(), [option()]) :: summary()
  def drain_for_shutdown(timeout_ms, opts \\ []) do
    call_drain(
      opts,
      {:drain_for_shutdown, timeout_ms},
      timeout_ms,
      conservative_call_timeout_ms(timeout_ms)
    )
  end

  @doc """
  What is left, in milliseconds, of the budget the node's shutdown drain
  started: 0 once it is spent, or while no shutdown drain has started it.
  """
  @spec shutdown_budget_remaining_ms([option()]) :: non_neg_integer()
  def shutdown_budget_remaining_ms(opts \\ []) do
    opts
    |> configured_server_name()
    |> call_if_started(:shutdown_budget_remaining_ms, 0)
  catch
    :exit, _reason -> 0
  end

  @spec configured_timeout_ms() :: pos_integer()
  def configured_timeout_ms do
    @timeout_env
    |> System.get_env()
    |> parse_timeout_ms()
  end

  # A release sets the shutdown budget through the environment or takes the default. Only the test
  # configuration sets `:shutdown_timeout_ms`, and only while the environment variable is unset:
  # `mix codex_pooler.test` stops the application before dropping a run-scoped database, and an
  # owner a test leaked must not hold that exit for the release budget.
  defp shutdown_timeout_ms do
    configured =
      :codex_pooler
      |> Application.get_env(__MODULE__, [])
      |> Keyword.get(:shutdown_timeout_ms)

    case {System.get_env(@timeout_env), configured} do
      {nil, timeout_ms} when is_integer(timeout_ms) and timeout_ms > 0 -> timeout_ms
      _environment_or_default -> configured_timeout_ms()
    end
  end

  @impl GenServer
  def init(opts) do
    activity_registry = Keyword.get(opts, :activity_registry, ActivityRegistry)
    stream_registry = Keyword.get(opts, :stream_registry, DeferredStreamRegistry)
    # The registry whose local owners a drain enumerates. Only a test passes its own, so owners
    # another test leaves in the application registry cannot join its drain.
    owner_registry = Keyword.get(opts, :owner_registry, @registry)
    drain_policy = drain_policy(opts)
    :ok = warn_when_budget_leaves_no_turn_window(configured_timeout_ms(), drain_policy)

    {:ok,
     %{
       draining?: activity_registry_draining?(activity_registry),
       active_drain: nil,
       last_summary: nil,
       deadline_ms: nil,
       shutdown_started_at_ms: nil,
       shutdown_timeout_ms: nil,
       drain_policy: drain_policy,
       activity_registry: activity_registry,
       stream_registry: stream_registry,
       owner_registry: owner_registry,
       relay: Keyword.get(opts, :relay, RelayRuntime)
     }}
  end

  @impl GenServer
  def handle_call(:draining?, _from, state) do
    {:reply, state.draining?, state}
  end

  def handle_call(:shutdown_budget_remaining_ms, _from, state) do
    remaining_ms =
      case shutdown_timeout_budget(state) do
        {:remaining, remaining_ms} -> remaining_ms
        _not_started_or_exhausted -> 0
      end

    {:reply, remaining_ms, state}
  end

  def handle_call(
        {:start_drain, _timeout_ms, _drain_policy},
        from,
        %{active_drain: active_drain} = state
      )
      when is_map(active_drain) do
    active_drain = %{active_drain | waiters: [from | active_drain.waiters]}
    {:noreply, %{state | active_drain: active_drain}}
  end

  def handle_call({:start_drain, timeout_ms, drain_policy}, from, state) do
    start_local_drain(timeout_ms, from, state, false, drain_policy)
  end

  def handle_call(
        {:drain_for_shutdown, timeout_ms},
        from,
        %{active_drain: active_drain} = state
      )
      when is_map(active_drain) do
    quiesce_relay!(state)
    active_drain = %{active_drain | waiters: [from | active_drain.waiters]}

    {:noreply,
     state
     |> ensure_shutdown_budget_started(timeout_ms)
     |> Map.put(:active_drain, active_drain)}
  end

  def handle_call({:drain_for_shutdown, timeout_ms}, from, state) do
    quiesce_relay!(state)

    case shutdown_timeout_budget(state) do
      :not_started ->
        start_local_drain(timeout_ms, from, state, true, state.drain_policy)

      {:remaining, remaining_timeout_ms} ->
        start_local_drain(remaining_timeout_ms, from, state, true, state.drain_policy)

      :exhausted ->
        result = if state.last_summary, do: state.last_summary.result, else: :ok
        summary = empty_summary(result, state.shutdown_timeout_ms, true)
        log_drain_finished(summary)
        {:reply, summary, state}
    end
  end

  # A cohort cannot run until the waiter-owning server can observe its death.
  def handle_call({:rollout_drain_worker, ref, pid}, _from, %{active_drain: %{ref: ref, coordinator_down?: false} = active} = state) do
    monitor = Process.monitor(pid)
    {:reply, :ok, %{state | active_drain: %{active | workers: Map.put(active.workers, monitor, pid)}}}
  end

  def handle_call({:rollout_drain_worker, _ref, _pid}, _from, state), do: {:reply, :closed, state}

  def handle_call({:rollout_drain_progress, ref, counters, pending, owners_seen}, _from, %{active_drain: %{ref: ref} = active} = state) do
    {:reply, :ok, %{state | active_drain: %{active | counters: counters, pending: pending, owners_seen: owners_seen}}}
  end

  # Checkpoint each result before returning it to the ordered async stream:
  # a later completed cohort must survive an earlier cohort/coordinator failure.
  def handle_call({:rollout_drain_work_result, ref, item, result}, _from, %{active_drain: %{ref: ref} = active} = state) do
    active =
      if item in active.pending do
        %{active | counters: count_work_result({item, {:ok, result}}, active.counters), pending: List.delete(active.pending, item)}
      else
        active
      end

    {:reply, :ok, %{state | active_drain: active}}
  end

  @impl GenServer
  def handle_info({:rollout_drain_finished, ref, summary}, %{active_drain: %{ref: ref} = active} = state) do
    finish_drain_if_stopped(%{state | active_drain: %{active | summary: summary}})
  end

  def handle_info({:rollout_drain_timeout, ref}, %{active_drain: %{ref: ref} = active} = state) do
    Process.exit(active.pid, :kill)
    {:noreply, state}
  end

  def handle_info({:DOWN, monitor, :process, _pid, _reason}, %{active_drain: %{monitor: monitor} = active} = state) do
    if is_nil(active.summary), do: Enum.each(active.workers, fn {_ref, pid} -> Process.exit(pid, :kill) end)
    finish_drain_if_stopped(%{state | active_drain: %{active | coordinator_down?: true}})
  end

  def handle_info({:DOWN, monitor, :process, _pid, _reason}, %{active_drain: %{workers: workers} = active} = state) when is_map_key(workers, monitor) do
    finish_drain_if_stopped(%{state | active_drain: %{active | workers: Map.delete(workers, monitor)}})
  end

  def handle_info(_message, state), do: {:noreply, state}

  @impl GenServer
  def terminate(_reason, %{active_drain: active}) when is_map(active) do
    Process.cancel_timer(active.timer)
    Process.exit(active.pid, :kill)
    Enum.each(active.workers, fn {_monitor, pid} -> Process.exit(pid, :kill) end)
    :ok
  end

  def terminate(_reason, _state), do: :ok

  defp finish_drain_if_stopped(%{active_drain: %{coordinator_down?: true, workers: workers} = active} = state) when map_size(workers) == 0 do
    summary = active.summary || failed_drain_summary(active)
    Process.cancel_timer(active.timer)
    :ets.delete(active.snapshot)
    log_drain_finished(summary)
    Enum.each(active.waiters, &GenServer.reply(&1, summary))
    {:noreply, %{state | active_drain: nil, last_summary: summary}}
  end

  defp finish_drain_if_stopped(state), do: {:noreply, state}

  defp failed_drain_summary(active) do
    counters = Enum.reduce(active.pending, active.counters, &count_work_result({&1, {:exit, :coordinator_down}}, &2))
    summary = summary(counters, active.owners_seen, active.timeout_ms, active.started_at, active.already_draining?)
    %{summary | result: :error}
  end

  @spec drain_local_work(
          pos_integer(),
          boolean(),
          map(),
          {GenServer.server(), Registry.registry()},
          {GenServer.server(), integer(), :ets.tid(), pid(), reference()}
        ) :: summary()
  defp drain_local_work(
         timeout_ms,
         already_draining?,
         drain_policy,
         {activity_registry, owner_registry},
         {stream_registry, deadline_ms, snapshot, caller, ref}
       ) do
    started_at = System.monotonic_time(:millisecond)
    {stream_drain_epoch, initial_streams} = DeferredStreamRegistry.begin_drain(name: stream_registry, deadline: %{at: deadline_ms, now_ms: drain_policy.now_ms})
    :ets.insert(snapshot, {:entries, initial_streams})
    {drain_epoch, activities} = ActivityRegistry.begin_drain(name: activity_registry)
    owners = local_owner_sessions(owner_registry)

    # The server owns the observed HTTP cohort so coordinator death cannot
    # erase admissions promoted after begin_drain.
    work =
      Enum.map(owners, fn {key, owner} = entry ->
        if Registry.lookup(owner_registry, key) == [{owner, :starting}], do: {:starting_owner, entry}, else: {:owner, entry}
      end) ++
        Enum.map(activities, &{:activity, &1}) ++ [{:http_streams, snapshot}, {:receipt_settlements, snapshot}]

    counters = empty_counters(activities, [])
    :ok = GenServer.call(caller, {:rollout_drain_progress, ref, counters, work, length(owners)})

    results =
      work
      |> Task.async_stream(
        fn item ->
          case GenServer.call(caller, {:rollout_drain_worker, ref, self()}) do
            :ok ->
              result = drain_work_item(item, deadline_ms, drain_policy, stream_registry, activity_registry)
              :ok = GenServer.call(caller, {:rollout_drain_work_result, ref, item, result})
              result

            :closed ->
              :coordinator_closed
          end
        end,
        max_concurrency: max(1, length(work)),
        on_timeout: :kill_task,
        ordered: true,
        timeout: owner_task_timeout_ms(timeout_ms, drain_policy)
      )

    {counters, []} =
      Enum.reduce(results, {counters, work}, fn result, {counters, [item | remaining]} ->
        counters = count_work_result({item, result}, counters)
        {counters, remaining}
      end)

    activity_completed? = complete_registry_drain(fn -> ActivityRegistry.complete_drain(drain_epoch, name: activity_registry) end)
    stream_completed? = complete_registry_drain(fn -> DeferredStreamRegistry.complete_drain(stream_drain_epoch, name: stream_registry) end)
    result = summary(counters, length(owners), timeout_ms, started_at, already_draining?)
    if activity_completed? and stream_completed?, do: result, else: %{result | result: :error}
  end

  defp drain_work_item({:owner, {_key, owner}}, deadline, policy, _streams, _activities), do: {:owner, drain_owner_after_turn(owner, deadline, policy)}
  defp drain_work_item({:starting_owner, {_key, owner}}, deadline, policy, _streams, _activities), do: {:owner, drain_starting_owner_after_turn(owner, deadline, policy)}
  defp drain_work_item({:activity, activity}, deadline, policy, _streams, activities), do: {:activity, activity.kind, ActivityDrain.drain(activity, deadline, policy, activities)}
  defp drain_work_item({:http_streams, snapshot}, deadline, policy, streams, _activities), do: drain_http_cohort(deadline, policy, streams, snapshot)

  defp drain_work_item({:receipt_settlements, snapshot}, deadline, policy, _streams, activities) do
    {:receipt_settlements, ActivityDrain.drain_receipts(deadline, policy, activities, fn cohort -> :ets.insert(snapshot, {:receipts, cohort}) end)}
  end

  defp complete_registry_drain(fun) do
    fun.() == :ok
  catch
    :exit, _reason -> false
  end

  defp summary(counters, owners_seen, timeout_ms, started_at, already_draining?) do
    elapsed_ms = max(0, System.monotonic_time(:millisecond) - started_at)

    %{
      result: drain_result(counters),
      owners_seen: owners_seen,
      owners_drained: counters.owners_drained,
      owners_idle: counters.owners_idle,
      owners_failed: counters.owners_failed,
      turns_completed: counters.turns_completed,
      turns_aborted: counters.turns_aborted,
      direct_turns_seen: counters.direct_turns_seen,
      direct_turns_completed: counters.direct_turns_completed,
      direct_turns_aborted: counters.direct_turns_aborted,
      direct_turns_failed: counters.direct_turns_failed,
      proxy_turns_seen: counters.proxy_turns_seen,
      proxy_turns_completed: counters.proxy_turns_completed,
      proxy_turns_aborted: counters.proxy_turns_aborted,
      proxy_turns_failed: counters.proxy_turns_failed,
      receipt_settlements_seen: counters.receipt_settlements_seen,
      receipt_settlements_completed: counters.receipt_settlements_completed,
      receipt_settlements_superseded: counters.receipt_settlements_superseded,
      receipt_settlements_aborted: counters.receipt_settlements_aborted,
      receipt_settlements_failed: counters.receipt_settlements_failed,
      receipt_producers_failed?: counters.receipt_producers_failed?,
      http_streams_seen: counters.http_streams_seen,
      http_streams_completed: counters.http_streams_completed,
      http_streams_aborted: counters.http_streams_aborted,
      http_streams_failed: counters.http_streams_failed,
      timeout_ms: timeout_ms,
      elapsed_ms: elapsed_ms,
      already_draining?: already_draining?
    }
  end

  defp local_owner_sessions(owner_registry) do
    Registry.select(owner_registry, [{{:"$1", :"$2", :_}, [], [{{:"$1", :"$2"}}]}])
    |> Enum.filter(fn {_key, owner} -> is_pid(owner) and Process.alive?(owner) end)
  end

  defp drain_owner(owner) do
    WebsocketOwnerSession.drain_owner(owner)
  catch
    :exit, _reason -> {:error, :owner_unavailable}
  end

  defp drain_starting_owner_after_turn(owner, deadline_ms, drain_policy) do
    :ok = WebsocketOwnerSession.begin_drain(owner)
    remaining_ms = max(1, deadline_ms - drain_policy.now_ms.())

    try do
      case WebsocketOwnerSession.local_drain_status(owner, %{at: deadline_ms, now_ms: drain_policy.now_ms}, remaining_ms) do
        {:ok, %{active_turn?: false, compact_delivery_status: status}} when status in [:terminal_error, :aborted, :skipped] -> drain_settled_owner(:aborted, owner)
        {:ok, %{active_turn?: true}} -> drain_owner_after_turn(owner, deadline_ms, drain_policy, :active)
        {:ok, %{active_turn?: false}} -> drain_settled_owner(:idle, owner)
      end
    catch
      :exit, _reason ->
        # Queue the final drain even if initialization outlasts the budget.
        # The coordinator may stop this worker, but the owner must still stop
        # and release its lease when initialization eventually returns.
        drain_owner(owner)
    end
  end

  # `observed` is what the drain already knows of the owner's turn. A starting
  # owner was asked once before this (`drain_starting_owner_after_turn/3`); a
  # turn it reported active and that ended before the next look is a completed
  # turn, never an idle owner (Drone 1708 counted it `owners_idle: 1,
  # turns_completed: 0`).
  defp drain_owner_after_turn(owner, deadline_ms, drain_policy, observed \\ :unobserved) do
    :ok = WebsocketOwnerSession.begin_drain(owner)
    owner_ref = Process.monitor(owner)

    outcome =
      owner
      |> await_turn_outcome(owner_ref, deadline_ms, drain_policy, observed)
      |> drain_settled_owner(owner)

    Process.demonitor(owner_ref, [:flush])
    outcome
  end

  defp await_turn_outcome(owner, owner_ref, deadline_ms, drain_policy, :active),
    do: poll_owner_status(owner, owner_ref, deadline_ms, drain_policy)

  defp await_turn_outcome(owner, owner_ref, deadline_ms, drain_policy, :unobserved) do
    case owner_status(owner, owner_ref, deadline_ms, drain_policy) do
      {:ok, %{active_turn?: false, compact_delivery_status: status}} when status in [:terminal_error, :aborted, :skipped] ->
        :aborted

      {:ok, %{active_turn?: false}} ->
        :idle

      {:ok, %{active_turn?: true}} ->
        poll_active_turn(owner, owner_ref, deadline_ms, drain_policy)

      {:error, :owner_unavailable} ->
        :failed
    end
  end

  defp poll_active_turn(owner, owner_ref, deadline_ms, drain_policy) do
    remaining_ms = max(0, deadline_ms - drain_policy.now_ms.())

    if remaining_ms == 0 do
      :aborted
    else
      wait_ms = min(@drain_poll_interval_ms, remaining_ms)

      case wait_or_owner_down(owner_ref, drain_policy, wait_ms) do
        :owner_down ->
          :failed

        :elapsed ->
          poll_owner_status(owner, owner_ref, deadline_ms, drain_policy)

        :wait_failed ->
          :failed
      end
    end
  end

  defp poll_owner_status(owner, owner_ref, deadline_ms, drain_policy) do
    case owner_status(owner, owner_ref, deadline_ms, drain_policy) do
      {:ok, %{active_turn?: false, compact_delivery_status: status}} when status in [:terminal_error, :aborted, :skipped] ->
        :aborted

      {:ok, %{active_turn?: false}} ->
        :completed

      {:ok, %{active_turn?: true}} ->
        poll_active_turn(owner, owner_ref, deadline_ms, drain_policy)

      {:error, :owner_unavailable} ->
        :failed
    end
  end

  defp owner_status(owner, owner_ref, deadline_ms, drain_policy) do
    receive do
      {:DOWN, ^owner_ref, :process, ^owner, _reason} ->
        {:error, :owner_unavailable}
    after
      0 ->
        WebsocketOwnerSession.local_drain_status(owner, %{at: deadline_ms, now_ms: drain_policy.now_ms})
    end
  catch
    :exit, _reason -> {:error, :owner_unavailable}
  end

  defp wait_or_owner_down(owner_ref, drain_policy, wait_ms) do
    wait_token = make_ref()

    try do
      wait_ref = drain_policy.schedule_wait.(self(), wait_token, wait_ms)

      receive do
        {:DOWN, ^owner_ref, :process, _owner, _reason} ->
          :ok = drain_policy.cancel_wait.(wait_ref, wait_token)
          :owner_down

        {:rollout_drain_wait_elapsed, ^wait_token} ->
          :elapsed
      end
    catch
      _kind, _reason ->
        :wait_failed
    end
  end

  defp drain_settled_owner(:failed, _owner), do: {:error, :owner_unavailable}

  defp drain_settled_owner(outcome, owner) do
    case drain_owner(owner) do
      :ok -> {:ok, outcome}
      {:ok, :settled} -> {:ok, settled_outcome(outcome)}
      {:error, _reason} = error -> error
    end
  end

  # The owner let the turn whose terminal it forwarded settle before it
  # stopped (findings#287): the turn the deadline found still active
  # completed, and the drain's summary says so.
  defp settled_outcome(:aborted), do: :completed
  defp settled_outcome(outcome), do: outcome

  defp drain_http_cohort(deadline, policy, registry, snapshot) do
    {:http_streams, DeferredStreamDrain.drain_all(deadline, policy, registry, fn entries -> :ets.insert(snapshot, {:entries, entries}) end)}
  rescue
    exception -> {:http_streams_failed, exception.__struct__}
  catch
    kind, _reason -> {:http_streams_failed, kind}
  end

  defp count_work_result({{:starting_owner, owner}, result}, counters), do: count_work_result({{:owner, owner}, result}, counters)

  defp count_work_result({{:owner, _owner}, {:ok, {:owner, {:ok, outcome}}}}, counters) do
    counters
    |> Map.update!(:owners_drained, &(&1 + 1))
    |> count_outcome(outcome)
  end

  defp count_work_result({{:owner, _owner}, _result}, counters) do
    Map.update!(counters, :owners_failed, &(&1 + 1))
  end

  defp count_work_result(
         {{:activity, %{kind: kind}}, {:ok, {:activity, kind, outcome}}},
         counters
       ),
       do: count_activity_outcome(counters, kind, outcome)

  defp count_work_result({{:activity, %{kind: kind}}, _result}, counters),
    do: count_activity_outcome(counters, kind, :failed)

  defp count_work_result({{:http_streams, _snapshot}, {:ok, {:http_streams, entries}}}, counters) do
    Enum.reduce(entries, counters, fn
      %{phase: :streaming, status: {:finished, outcome}}, counters ->
        counters
        |> Map.update!(:http_streams_seen, &(&1 + 1))
        |> count_http_stream_outcome(outcome)

      _admission, counters ->
        counters
    end)
  end

  defp count_work_result({{:http_streams, snapshot}, result}, counters) do
    [{:entries, entries}] = :ets.lookup(snapshot, :entries)

    failure =
      case result do
        {:ok, {:http_streams_failed, reason}} -> reason
        {:exit, :timeout} -> :timeout
        _other -> :unexpected_result
      end

    Logger.warning("websocket rollout drain HTTP cohort unavailable reason_class=#{inspect(failure)} observed_entries=#{length(entries)}")

    entries =
      Enum.map(entries, fn
        %{status: {:finished, _outcome}} = entry -> entry
        entry -> %{entry | status: {:finished, :failed}}
      end)

    count_work_result({{:http_streams, snapshot}, {:ok, {:http_streams, entries}}}, %{counters | http_stream_cohort_failed?: true})
  end

  defp count_work_result({{:receipt_settlements, _snapshot}, {:ok, {:receipt_settlements, cohort}}}, counters) do
    counters = %{counters | receipt_producers_failed?: cohort.producers_failed?, receipt_cohort_failed?: not cohort.closed?}

    Enum.reduce(cohort.entries, counters, fn entry, acc ->
      outcome =
        case entry.status do
          {:finished, outcome} -> outcome
          :active -> :failed
        end

      counter = if Map.get(entry, :receipt_disposition) == :superseded, do: :receipt_settlements_superseded, else: receipt_counter(outcome)
      acc |> Map.update!(:receipt_settlements_seen, &(&1 + 1)) |> Map.update!(counter, &(&1 + 1))
    end)
  end

  defp count_work_result({{:receipt_settlements, snapshot}, _result}, counters) do
    [{:receipts, cohort}] = :ets.lookup(snapshot, :receipts)
    counters = count_work_result({{:receipt_settlements, snapshot}, {:ok, {:receipt_settlements, cohort}}}, counters)
    %{counters | receipt_cohort_failed?: true}
  end

  defp receipt_counter(:completed), do: :receipt_settlements_completed
  defp receipt_counter(:aborted), do: :receipt_settlements_aborted
  defp receipt_counter(:failed), do: :receipt_settlements_failed

  defp count_outcome(counters, :idle), do: Map.update!(counters, :owners_idle, &(&1 + 1))

  defp count_outcome(counters, :completed),
    do: Map.update!(counters, :turns_completed, &(&1 + 1))

  defp count_outcome(counters, :aborted),
    do: Map.update!(counters, :turns_aborted, &(&1 + 1))

  defp count_activity_outcome(counters, kind, outcome) do
    Map.update!(counters, activity_counter(kind, outcome), &(&1 + 1))
  end

  defp count_http_stream_outcome(counters, outcome) do
    Map.update!(counters, http_stream_counter(outcome), &(&1 + 1))
  end

  defp http_stream_counter(:completed), do: :http_streams_completed
  defp http_stream_counter(:aborted), do: :http_streams_aborted
  defp http_stream_counter(:failed), do: :http_streams_failed

  defp activity_counter(:direct, :completed), do: :direct_turns_completed
  defp activity_counter(:direct, :aborted), do: :direct_turns_aborted
  defp activity_counter(:direct, :failed), do: :direct_turns_failed
  defp activity_counter(:proxy, :completed), do: :proxy_turns_completed
  defp activity_counter(:proxy, :aborted), do: :proxy_turns_aborted
  defp activity_counter(:proxy, :failed), do: :proxy_turns_failed

  defp empty_counters(activities, streams) do
    %{
      owners_drained: 0,
      owners_idle: 0,
      owners_failed: 0,
      turns_completed: 0,
      turns_aborted: 0,
      direct_turns_seen: Enum.count(activities, &(&1.kind == :direct)),
      direct_turns_completed: 0,
      direct_turns_aborted: 0,
      direct_turns_failed: 0,
      proxy_turns_seen: Enum.count(activities, &(&1.kind == :proxy)),
      proxy_turns_completed: 0,
      proxy_turns_aborted: 0,
      proxy_turns_failed: 0,
      receipt_settlements_seen: 0,
      receipt_settlements_completed: 0,
      receipt_settlements_superseded: 0,
      receipt_settlements_aborted: 0,
      receipt_settlements_failed: 0,
      receipt_producers_failed?: false,
      receipt_cohort_failed?: false,
      http_streams_seen: length(streams),
      http_streams_completed: 0,
      http_streams_aborted: 0,
      http_streams_failed: 0,
      http_stream_cohort_failed?: false
    }
  end

  defp drain_result(counters) do
    if counters.owners_failed + counters.direct_turns_failed + counters.proxy_turns_failed +
         counters.http_streams_failed + counters.receipt_settlements_failed + counters.receipt_settlements_aborted == 0 and not counters.http_stream_cohort_failed? and not counters.receipt_cohort_failed? and not counters.receipt_producers_failed?,
       do: :ok,
       else: :error
  end

  defp call_drain(opts, request, timeout_ms, call_timeout) do
    case GenServer.whereis(configured_server_name(opts)) do
      nil ->
        summary = empty_summary(:error, timeout_ms, false)
        log_drain_finished(summary)
        summary

      server ->
        GenServer.call(server, request, call_timeout)
    end
  end

  # Only a shutdown drain closes relay claim admission: quiesce is permanent
  # for the consumer's lifetime, and a pod that drains but keeps serving must
  # keep consuming the shared relay. Every shutdown branch (fresh, joining an
  # active drain, exhausted budget) passes through here.
  defp quiesce_relay!(state), do: :ok = RelayRuntime.quiesce(state.relay, 5_000)

  defp start_local_drain(timeout_ms, from, state, shutdown?, drain_policy) do
    already_draining? = state.draining?
    ref = make_ref()
    caller = self()

    deadline_ms =
      state.deadline_ms || poll_deadline_ms(timeout_ms, drain_policy.now_ms.(), drain_policy)

    snapshot = :ets.new(__MODULE__, [:set, :public])
    :ets.insert(snapshot, [{:entries, []}, {:receipts, %{entries: [], closed?: false, producers_failed?: false}}])
    log_drain_started(timeout_ms, already_draining?, max(0, deadline_ms - drain_policy.now_ms.()))

    {pid, monitor} =
      spawn_monitor(fn ->
        summary = drain_local_work(timeout_ms, already_draining?, drain_policy, {state.activity_registry, state.owner_registry}, {state.stream_registry, deadline_ms, snapshot, caller, ref})
        send(caller, {:rollout_drain_finished, ref, summary})
      end)

    timer = Process.send_after(self(), {:rollout_drain_timeout, ref}, owner_task_timeout_ms(timeout_ms, drain_policy))
    active_drain = %{ref: ref, timer: timer, pid: pid, monitor: monitor, waiters: [from], workers: %{}, coordinator_down?: false, summary: nil, snapshot: snapshot, counters: empty_counters([], []), pending: [{:http_streams, snapshot}, {:receipt_settlements, snapshot}], owners_seen: 0, timeout_ms: timeout_ms, started_at: System.monotonic_time(:millisecond), already_draining?: already_draining?}

    state =
      state
      |> ensure_shutdown_budget_started(timeout_ms, shutdown?)
      |> Map.merge(%{draining?: true, active_drain: active_drain, deadline_ms: deadline_ms})

    {:noreply, state}
  end

  defp ensure_shutdown_budget_started(state, timeout_ms, true) do
    ensure_shutdown_budget_started(state, timeout_ms)
  end

  defp ensure_shutdown_budget_started(state, _timeout_ms, false), do: state

  defp ensure_shutdown_budget_started(%{shutdown_started_at_ms: nil} = state, timeout_ms) do
    %{
      state
      | shutdown_started_at_ms: System.monotonic_time(:millisecond),
        shutdown_timeout_ms: timeout_ms
    }
  end

  defp ensure_shutdown_budget_started(state, _timeout_ms), do: state

  defp shutdown_timeout_budget(%{shutdown_started_at_ms: nil}), do: :not_started

  defp shutdown_timeout_budget(state) do
    elapsed_ms = max(0, System.monotonic_time(:millisecond) - state.shutdown_started_at_ms)
    remaining_timeout_ms = max(0, state.shutdown_timeout_ms - elapsed_ms)

    if remaining_timeout_ms > 0 do
      {:remaining, remaining_timeout_ms}
    else
      :exhausted
    end
  end

  defp call_if_started(server_name, request, fallback) do
    case GenServer.whereis(server_name) do
      nil -> fallback
      server -> GenServer.call(server, request)
    end
  end

  defp configured_server_name(opts) do
    Keyword.get(opts, :name) ||
      :codex_pooler
      |> Application.get_env(__MODULE__, [])
      |> Keyword.get(:server_name, __MODULE__)
  end

  defp timeout_ms(opts) do
    case Keyword.get(opts, :timeout_ms, configured_timeout_ms()) do
      timeout_ms when is_integer(timeout_ms) and timeout_ms > 0 -> timeout_ms
      _invalid -> @default_timeout_ms
    end
  end

  defp drain_policy(opts) do
    deadline =
      Keyword.get(opts, :deadline, %{
        now_ms: fn -> System.monotonic_time(:millisecond) end,
        schedule_wait: fn recipient, wait_token, wait_ms ->
          Process.send_after(recipient, {:rollout_drain_wait_elapsed, wait_token}, wait_ms)
        end,
        cancel_wait: &cancel_timer/2
      })

    owner_post_deadline_call_budget_ms =
      positive_option(
        opts,
        :owner_post_deadline_call_budget_ms,
        @default_owner_post_deadline_call_budget_ms
      )

    default_margin_ms =
      owner_post_deadline_call_budget_ms + @drain_poll_interval_ms +
        @owner_task_finish_margin_ms

    %{
      now_ms: Map.fetch!(deadline, :now_ms),
      schedule_wait: Map.fetch!(deadline, :schedule_wait),
      cancel_wait: Map.fetch!(deadline, :cancel_wait),
      margin_ms: non_negative_option(opts, :deadline_margin_ms, default_margin_ms),
      floor_ms: non_negative_option(opts, :deadline_floor_ms, @drain_deadline_floor_ms),
      owner_post_deadline_call_budget_ms: owner_post_deadline_call_budget_ms
    }
  end

  defp non_negative_option(opts, key, default) do
    case Keyword.get(opts, key, default) do
      value when is_integer(value) and value >= 0 -> value
      _invalid -> default
    end
  end

  defp positive_option(opts, key, default) do
    case Keyword.get(opts, key, default) do
      value when is_integer(value) and value > 0 -> value
      _invalid -> default
    end
  end

  defp poll_deadline_ms(timeout_ms, started_at, drain_policy) do
    elapsed_ms = max(0, drain_policy.now_ms.() - started_at)
    remaining_budget_ms = max(0, timeout_ms - elapsed_ms)
    drain_policy.now_ms.() + turn_window_ms(remaining_budget_ms, drain_policy)
  end

  # How long a drain given `budget_ms` lets an active turn run before it is cut: the budget less the
  # margin the drain keeps for its own owner calls, poll and finish (10.7 s by default), never less
  # than the floor.
  defp turn_window_ms(budget_ms, drain_policy),
    do: max(budget_ms - drain_policy.margin_ms, drain_policy.floor_ms)

  # The release takes its budget from the environment with no lower bound, and a budget at or under
  # the margin leaves an active turn only the floor: every drain then cuts every turn in flight at
  # once (`owner_drained`) without saying why. Said once, when the drain server starts
  # (findings#270 row 270-222). A shutdown can first spend up to 5 s of the same budget quiescing the
  # relay, so a window above the floor may still be shorter in practice; each drain's
  # `websocket rollout drain started` line names the window it actually gave.
  defp warn_when_budget_leaves_no_turn_window(budget_ms, drain_policy) do
    window_ms = turn_window_ms(budget_ms, drain_policy)

    if window_ms <= drain_policy.floor_ms do
      Logger.warning(
        "websocket rollout drain budget leaves active turns no time " <>
          "timeout_ms=#{budget_ms} margin_ms=#{drain_policy.margin_ms} " <>
          "turn_window_ms=#{window_ms} setting=#{@timeout_env}"
      )
    end

    :ok
  end

  defp owner_task_timeout_ms(timeout_ms, drain_policy) do
    poll_budget_ms = turn_window_ms(timeout_ms, drain_policy)

    max(
      timeout_ms,
      poll_budget_ms + drain_policy.owner_post_deadline_call_budget_ms +
        @owner_task_finish_margin_ms
    )
  end

  defp coordinator_call_timeout_ms(timeout_ms, drain_policy) do
    owner_task_timeout_ms(timeout_ms, drain_policy) + 1_000
  end

  defp conservative_call_timeout_ms(timeout_ms) do
    timeout_ms + @default_owner_post_deadline_call_budget_ms + @owner_task_finish_margin_ms +
      1_000
  end

  defp cancel_timer(timer_ref, wait_token) do
    _result = Process.cancel_timer(timer_ref)

    receive do
      {:rollout_drain_wait_elapsed, ^wait_token} -> :ok
    after
      0 -> :ok
    end
  end

  defp parse_timeout_ms(value) when is_binary(value) do
    case Integer.parse(value) do
      {timeout_ms, ""} when timeout_ms > 0 -> timeout_ms
      _invalid -> @default_timeout_ms
    end
  end

  defp parse_timeout_ms(_value), do: @default_timeout_ms

  defp log_drain_started(timeout_ms, already_draining?, window_ms) do
    Logger.info(
      "websocket rollout drain started " <>
        "timeout_ms=#{timeout_ms} already_draining=#{already_draining?} " <>
        "turn_window_ms=#{window_ms}"
    )
  end

  defp log_drain_finished(summary) do
    Logger.info(
      "websocket rollout drain finished " <>
        "owners_seen=#{summary.owners_seen} " <>
        "owners_drained=#{summary.owners_drained} " <>
        "owners_idle=#{summary.owners_idle} " <>
        "owners_failed=#{summary.owners_failed} " <>
        "turns_completed=#{summary.turns_completed} " <>
        "turns_aborted=#{summary.turns_aborted} " <>
        "direct_turns_seen=#{summary.direct_turns_seen} " <>
        "direct_turns_completed=#{summary.direct_turns_completed} " <>
        "direct_turns_aborted=#{summary.direct_turns_aborted} " <>
        "direct_turns_failed=#{summary.direct_turns_failed} " <>
        "proxy_turns_seen=#{summary.proxy_turns_seen} " <>
        "proxy_turns_completed=#{summary.proxy_turns_completed} " <>
        "proxy_turns_aborted=#{summary.proxy_turns_aborted} " <>
        "proxy_turns_failed=#{summary.proxy_turns_failed} " <>
        "receipt_settlements_seen=#{summary.receipt_settlements_seen} " <>
        "receipt_settlements_completed=#{summary.receipt_settlements_completed} " <>
        "receipt_settlements_superseded=#{summary.receipt_settlements_superseded} " <>
        "receipt_settlements_aborted=#{summary.receipt_settlements_aborted} " <>
        "receipt_settlements_failed=#{summary.receipt_settlements_failed} " <>
        "receipt_producers_failed=#{summary.receipt_producers_failed?} " <>
        "http_streams_seen=#{summary.http_streams_seen} " <>
        "http_streams_completed=#{summary.http_streams_completed} " <>
        "http_streams_aborted=#{summary.http_streams_aborted} " <>
        "http_streams_failed=#{summary.http_streams_failed} " <>
        "timeout_ms=#{summary.timeout_ms} " <>
        "elapsed_ms=#{summary.elapsed_ms} " <>
        "result=#{summary.result}"
    )
  end

  defp empty_summary(result, timeout_ms, already_draining?) do
    %{
      result: result,
      owners_seen: 0,
      owners_drained: 0,
      owners_idle: 0,
      owners_failed: 0,
      turns_completed: 0,
      turns_aborted: 0,
      direct_turns_seen: 0,
      direct_turns_completed: 0,
      direct_turns_aborted: 0,
      direct_turns_failed: 0,
      proxy_turns_seen: 0,
      proxy_turns_completed: 0,
      proxy_turns_aborted: 0,
      proxy_turns_failed: 0,
      receipt_settlements_seen: 0,
      receipt_settlements_completed: 0,
      receipt_settlements_superseded: 0,
      receipt_settlements_aborted: 0,
      receipt_settlements_failed: 0,
      receipt_producers_failed?: false,
      http_streams_seen: 0,
      http_streams_completed: 0,
      http_streams_aborted: 0,
      http_streams_failed: 0,
      timeout_ms: timeout_ms,
      elapsed_ms: 0,
      already_draining?: already_draining?
    }
  end

  defp activity_registry_draining?(registry) do
    ActivityRegistry.draining?(name: registry)
  catch
    :exit, _reason -> false
  end
end
