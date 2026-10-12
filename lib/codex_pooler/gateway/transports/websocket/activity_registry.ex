defmodule CodexPooler.Gateway.Transports.Websocket.ActivityRegistry do
  @moduledoc false

  use GenServer

  alias __MODULE__.{Drain, Entry}
  alias CodexPooler.Gateway.Websocket.DirectCleanup
  alias CodexPooler.Platform.ExecutionRegistry

  @spec begin_direct_cleanup(DirectCleanup.t()) :: :ok | {:error, :cancelled}
  def begin_direct_cleanup(context),
    do: GenServer.call(context.registry, {:direct_begin, context})

  @spec bind_direct_cleanup(DirectCleanup.t(), DirectCleanup.receipt()) :: :ok
  def bind_direct_cleanup(context, receipt),
    do: GenServer.call(context.registry, {:direct_bind, context, receipt})

  @spec ready_direct_cleanup(DirectCleanup.t()) :: :ok | {:error, :cancelled}
  def ready_direct_cleanup(context),
    do: GenServer.call(context.registry, {:direct_ready, context})

  @spec await_direct_cleanup(DirectCleanup.t()) :: {:ok, DirectCleanup.receipt()} | :none
  def await_direct_cleanup(context),
    do: GenServer.call(context.registry, {:direct_await, context}, 30_000)

  @spec cancel_pending_direct_cleanup(DirectCleanup.t()) :: :pending | :not_pending
  def cancel_pending_direct_cleanup(context),
    do: GenServer.call(context.registry, {:direct_cancel_pending, context})

  @spec mark_direct_cleanup_reason(DirectCleanup.t(), String.t()) :: :ok
  def mark_direct_cleanup_reason(context, reason),
    do: GenServer.call(context.registry, {:direct_cancel_reason, context, reason})

  @spec handoff_direct_cleanup(DirectCleanup.t()) :: :ok
  def handoff_direct_cleanup(context),
    do: GenServer.call(context.registry, {:direct_handoff, context})

  @doc """
  The direct task marks the span it spends blocked on its upstream request,
  the one point where stopping it touches no database work in flight
  (`stop_direct_upstream_wait/1`). Leaving the span after a stop was granted
  answers `{:error, :stopped}`: the task must then exit without settling.
  """
  @spec enter_direct_upstream_wait(DirectCleanup.t()) :: :ok | {:error, :stopped}
  def enter_direct_upstream_wait(context),
    do: GenServer.call(context.registry, {:direct_upstream_wait, context, true})

  @spec leave_direct_upstream_wait(DirectCleanup.t()) :: :ok | {:error, :stopped}
  def leave_direct_upstream_wait(context),
    do: GenServer.call(context.registry, {:direct_upstream_wait, context, false})

  @spec stop_direct_upstream_wait(DirectCleanup.t()) :: :stop | :busy
  def stop_direct_upstream_wait(context),
    do: GenServer.call(context.registry, {:direct_stop_upstream_wait, context})

  @type activity_kind :: Entry.kind()
  @type outcome :: :completed | :aborted | :failed
  @type token :: reference()
  @type drain_entry :: Drain.entry()

  @type receipt_identity :: %{request_id: String.t(), attempt_id: String.t(), replay_generation: non_neg_integer()}
  @type receipt_disposition :: :inserted | :reused | :replaced | :superseded | :failed | :aborted

  # Called only by the accepting owner, before it exposes the accepted turn.
  # The capability carries original admission identity; it never admits provider work.
  @spec begin_receipt_producer(receipt_identity(), keyword()) :: {:ok, token()} | {:error, :owner_drained | :invalid_identity}
  def begin_receipt_producer(identity, opts \\ []),
    do: GenServer.call(server(opts), {:begin_receipt_producer, identity})

  @spec register_receipt_settlement(token(), receipt_identity(), pid(), keyword()) :: {:ok, token()} | {:error, :producer_closed | :identity_mismatch | :already_registered}
  def register_receipt_settlement(producer, identity, worker, opts \\ []) when is_reference(producer) and is_pid(worker),
    do: GenServer.call(server(opts), {:register_receipt_settlement, producer, identity, worker})

  @spec finish_receipt_producer(token(), keyword()) :: :ok | {:error, :producer_closed}
  def finish_receipt_producer(producer, opts \\ []),
    do: GenServer.call(server(opts), {:finish_receipt_producer, producer})

  @spec complete_receipt_settlement(token(), receipt_disposition(), keyword()) :: :ok | {:error, :unknown_activity}
  def complete_receipt_settlement(token, disposition, opts \\ []) when disposition in [:inserted, :reused, :replaced, :superseded, :failed, :aborted],
    do: GenServer.call(server(opts), {:complete_receipt_settlement, token, disposition})

  @spec receipt_cohort(keyword()) :: %{closed?: boolean(), entries: [Drain.entry()], pending_producers: non_neg_integer(), producers_failed?: boolean()}
  def receipt_cohort(opts \\ []), do: GenServer.call(server(opts), :receipt_cohort)

  @spec force_receipt_cohort(keyword()) :: :ok
  def force_receipt_cohort(opts \\ []), do: GenServer.call(server(opts), :force_receipt_cohort)

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    GenServer.start_link(__MODULE__, :ok, name: Keyword.get(opts, :name, __MODULE__))
  end

  def child_spec(opts) do
    name = Keyword.get(opts, :name, __MODULE__)

    %{
      id: {__MODULE__, name},
      start: {__MODULE__, :start_link, [opts]},
      type: :worker
    }
  end

  @spec register(activity_kind(), pid(), keyword()) :: {:ok, token()}
  def register(kind, pid \\ self(), opts \\ [])
      when kind in [:direct, :proxy, :local_owner] and is_pid(pid) do
    GenServer.call(
      server(opts),
      {:register, kind, pid, Keyword.take(opts, [:direct_cleanup_ref, :direct_cleanup_parent])}
    )
  end

  @spec admit(token(), keyword()) :: :ok | {:error, :owner_drained | :unknown_activity}
  def admit(token, opts \\ []) when is_reference(token) do
    GenServer.call(server(opts), {:admit, token})
  end

  @spec unregister(token(), outcome(), keyword()) :: :ok
  def unregister(token, outcome, opts \\ [])
      when is_reference(token) and outcome in [:completed, :aborted, :failed] do
    GenServer.call(server(opts), {:unregister, token, outcome})
  end

  @spec complete(token(), outcome(), keyword()) :: :ok
  def complete(token, outcome, opts \\ [])
      when is_reference(token) and outcome in [:completed, :aborted, :failed] do
    GenServer.call(server(opts), {:complete, token, outcome})
  end

  @spec set_cancel_recipient(token(), pid(), keyword()) :: :ok | {:error, :unknown_activity}
  def set_cancel_recipient(token, pid, opts \\ []) when is_reference(token) and is_pid(pid) do
    GenServer.call(server(opts), {:set_cancel_recipient, token, pid})
  end

  @spec handoff_cancel_recipient(token(), pid(), pid(), keyword()) ::
          :ok | {:cancelled, :owner_drained, pid()} | {:error, :unknown_activity}
  def handoff_cancel_recipient(token, from_pid, to_pid, opts \\ [])
      when is_reference(token) and is_pid(from_pid) and is_pid(to_pid) do
    GenServer.call(server(opts), {:handoff_cancel_recipient, token, from_pid, to_pid})
  end

  @spec delivery_target(pid(), keyword()) ::
          {:ok, token(), pid(), :registered | :admitted | :cancelling} | :unknown
  def delivery_target(pid, opts \\ []) when is_pid(pid) do
    GenServer.call(server(opts), {:delivery_target, pid})
  end

  @spec begin_drain(keyword()) :: {reference(), [Drain.entry()]}
  def begin_drain(opts \\ []), do: GenServer.call(server(opts), :begin_drain)

  @spec complete_drain(reference(), keyword()) :: :ok | {:error, :receipt_work_pending}
  def complete_drain(epoch, opts \\ []) when is_reference(epoch) do
    GenServer.call(server(opts), {:complete_drain, epoch})
  end

  @doc """
  Cancels an admitted activity for a drain. An activity whose terminal the
  socket already pushed (`mark_terminal_delivered/2`) is left alone and
  answered `:terminal_delivered`, unless `force: true`: the drain's cut once
  the task had its time to settle.
  """
  @spec cancel(token(), :owner_drained, keyword()) :: :ok | :terminal_delivered
  def cancel(token, :owner_drained = reason, opts \\ []) when is_reference(token) do
    GenServer.call(server(opts), {:cancel, token, reason, Keyword.get(opts, :force, false)})
  end

  @doc """
  The socket is pushing the terminal of the turn `pid` runs (findings#287):
  from now on the client holds the turn's outcome and only its settlement is
  left, so a drain's `cancel/3` leaves that activity to settle
  (`ActivityDrain`); cancelling it had recorded a turn the client completed as
  `owner_drained` and put an error frame after its terminal. An unknown `pid`
  is a no-op.
  """
  @spec mark_terminal_delivered(pid(), keyword()) :: :ok
  def mark_terminal_delivered(pid, opts \\ []) when is_pid(pid),
    do: GenServer.call(server(opts), {:mark_terminal_delivered, pid})

  @spec status(token(), keyword()) ::
          {:active, :registered | :admitted | :cancelling}
          | {:finished, outcome()}
          | :unknown
  def status(token, opts \\ []) when is_reference(token) do
    GenServer.call(server(opts), {:status, token})
  end

  @spec activities(keyword()) :: [Entry.t()]
  def activities(opts \\ []), do: GenServer.call(server(opts), :activities)

  @spec draining?(keyword()) :: boolean()
  def draining?(opts \\ []), do: GenServer.call(server(opts), :draining?)

  @impl GenServer
  def init(:ok) do
    {:ok, %{activities: %{}, monitors: %{}, draining?: false, drain: nil, finished_direct: %{}, receipt_producers: %{}, receipt_producer_monitors: %{}}}
  end

  @impl GenServer
  def handle_call({:direct_handoff, context}, _from, state) do
    case direct_entry(state, context.task) do
      {token, %{direct_cleanup: %{context: ^context} = cleanup}} ->
        cleanup = %{cleanup | receipt: nil, pending?: false}
        cleanup = Map.put(cleanup, :consumed?, true)
        {:reply, :ok, put_in(state.activities[token].direct_cleanup, cleanup)}

      _ ->
        {:reply, :ok, state}
    end
  end

  def handle_call({:direct_cancel_reason, context, reason}, _from, state)
      when reason in [
             "owner_drained",
             "owner_unavailable",
             "owner_crashed",
             "stale_owner",
             "client_disconnected"
           ] do
    case direct_entry(state, context.task) do
      {token, %{direct_cleanup: %{context: ^context} = cleanup}} ->
        receipt = if cleanup.receipt, do: Map.put_new(cleanup.receipt, :cancel_reason, reason)
        cleanup = Map.put_new(cleanup, :cancel_reason, reason)

        {:reply, :ok, put_in(state.activities[token].direct_cleanup, %{cleanup | receipt: receipt})}

      _ ->
        {:reply, :ok, state}
    end
  end

  def handle_call({:direct_upstream_wait, context, waiting?}, {caller, _}, state) do
    case direct_entry(state, context.task) do
      {_token, %{direct_cleanup: %{context: ^context, upstream_stop?: true}}} when caller == context.task ->
        {:reply, {:error, :stopped}, state}

      {token, %{direct_cleanup: %{context: ^context} = cleanup}} when caller == context.task ->
        {:reply, :ok, put_in(state.activities[token].direct_cleanup, Map.put(cleanup, :upstream_wait?, waiting?))}

      _ ->
        {:reply, :ok, state}
    end
  end

  def handle_call({:direct_stop_upstream_wait, context}, _from, state) do
    case direct_entry(state, context.task) do
      {token, %{direct_cleanup: %{context: ^context, upstream_wait?: true} = cleanup}} ->
        {:reply, :stop, put_in(state.activities[token].direct_cleanup, Map.put(cleanup, :upstream_stop?, true))}

      _ ->
        {:reply, :busy, state}
    end
  end

  def handle_call({:direct_cancel_pending, context}, _from, state) do
    case direct_entry(state, context.task) do
      {token, %{direct_cleanup: %{context: ^context, pending?: true}} = entry} ->
        {:reply, :pending, put_in(state.activities[token], Map.put(entry, :direct_cancelled?, true))}

      _ ->
        {:reply, :not_pending, state}
    end
  end

  def handle_call({:direct_begin, context}, {caller, _}, state) do
    case direct_entry(state, context.task) do
      {token, %{status: :admitted, direct_ref: ref, direct_parent: parent} = entry}
      when caller == context.task and ref == context.ref and parent == context.parent and
             not is_map_key(entry, :direct_cancelled?) and not is_map_key(entry, :direct_cleanup) ->
        cleanup = %{context: context, pending?: true, receipt: nil, waiters: []}
        {:reply, :ok, put_in(state.activities[token], Map.put(entry, :direct_cleanup, cleanup))}

      _ ->
        {:reply, {:error, :cancelled}, state}
    end
  end

  def handle_call({:direct_bind, context, receipt}, {caller, _}, state) do
    case direct_entry(state, context.task) do
      {token, %{direct_cleanup: %{context: ^context} = cleanup}} when caller == context.task ->
        receipt =
          case Map.get(cleanup, :cancel_reason) do
            nil -> receipt
            reason -> Map.put(receipt, :cancel_reason, reason)
          end

        send(context.parent, {:direct_request_cleanup, context.task, context.ref, receipt})

        {:reply, :ok, put_in(state.activities[token].direct_cleanup, %{cleanup | receipt: receipt})}

      _ ->
        {:reply, :ok, state}
    end
  end

  def handle_call({:direct_ready, context}, {caller, _}, state) do
    case direct_entry(state, context.task) do
      {token, %{direct_cleanup: %{context: ^context} = cleanup} = entry}
      when caller == context.task ->
        reply_direct_waiters(cleanup)

        result =
          if entry.status == :cancelling or cleanup.waiters != [] or
               Map.get(entry, :direct_cancelled?, false),
             do: {:error, :cancelled},
             else: :ok

        {:reply, result, put_in(state.activities[token].direct_cleanup, %{cleanup | pending?: false, waiters: []})}

      _ ->
        {:reply, {:error, :cancelled}, state}
    end
  end

  def handle_call({:direct_await, context}, from, state) do
    case direct_entry(state, context.task) do
      {token, %{direct_cleanup: %{context: ^context, pending?: true} = cleanup}} ->
        {:noreply,
         put_in(state.activities[token].direct_cleanup, %{
           cleanup
           | waiters: [from | cleanup.waiters]
         })}

      {token, %{direct_cleanup: %{context: ^context} = cleanup}} ->
        {:reply, direct_receipt(cleanup), put_in(state.activities[token].direct_cleanup, Map.put(cleanup, :consumed?, true))}

      {token, %{direct_ref: ref, direct_parent: parent} = entry}
      when entry.pid == context.task and ref == context.ref and parent == context.parent ->
        {:reply, :none, put_in(state.activities[token], Map.put(entry, :direct_cancelled?, true))}

      _ ->
        case Map.get(state.finished_direct, context.ref) do
          %{context: ^context} = completed ->
            Process.demonitor(completed.monitor, [:flush])

            {:reply, {:ok, completed.receipt}, %{state | finished_direct: Map.delete(state.finished_direct, context.ref)}}

          _ ->
            {:reply, :none, state}
        end
    end
  end

  def handle_call({:begin_receipt_producer, _identity}, _from, %{draining?: true} = state),
    do: {:reply, {:error, :owner_drained}, state}

  def handle_call({:begin_receipt_producer, identity}, {owner, _}, state) do
    case identity do
      %{request_id: request, attempt_id: attempt, replay_generation: generation}
      when is_binary(request) and byte_size(request) > 0 and is_binary(attempt) and byte_size(attempt) > 0 and is_integer(generation) and generation >= 0 and map_size(identity) == 3 ->
        token = make_ref()
        monitor = Process.monitor(owner)
        producer = %{owner: owner, identity: identity, monitor: monitor, worker: nil}
        state = %{state | receipt_producers: Map.put(state.receipt_producers, token, producer), receipt_producer_monitors: Map.put(state.receipt_producer_monitors, monitor, token)}
        {:reply, {:ok, token}, state}

      _invalid ->
        {:reply, {:error, :invalid_identity}, state}
    end
  end

  def handle_call({:register_receipt_settlement, producer, identity, worker}, {owner, _}, state) do
    case Map.get(state.receipt_producers, producer) do
      %{owner: ^owner, identity: ^identity, worker: nil} ->
        token = make_ref()
        monitor = Process.monitor(worker)
        entry = Entry.new(token, :receipt_settlement, worker, monitor) |> Map.put(:status, :admitted) |> Map.put(:receipt_identity, identity)
        state = %{state | activities: Map.put(state.activities, token, entry), monitors: Map.put(state.monitors, monitor, token), drain: Drain.join_receipt(state.drain, token)}
        state = put_in(state.receipt_producers[producer].worker, {worker, token})
        {:reply, {:ok, token}, state}

      %{owner: ^owner, identity: ^identity, worker: {^worker, token}} ->
        {:reply, {:ok, token}, state}

      %{owner: ^owner, identity: ^identity} ->
        {:reply, {:error, :already_registered}, state}

      %{owner: ^owner} ->
        {:reply, {:error, :identity_mismatch}, state}

      _closed_or_wrong_owner ->
        {:reply, {:error, :producer_closed}, state}
    end
  end

  def handle_call({:finish_receipt_producer, token}, {owner, _}, state) do
    case Map.get(state.receipt_producers, token) do
      %{owner: ^owner} -> {:reply, :ok, finish_receipt_producer_state(state, token, false)}
      _closed_or_wrong_owner -> {:reply, {:error, :producer_closed}, state}
    end
  end

  def handle_call({:complete_receipt_settlement, token, disposition}, {worker, _}, state) do
    case Map.get(state.activities, token) do
      %{kind: :receipt_settlement, pid: ^worker} ->
        state = put_in(state.activities[token][:receipt_disposition], disposition)
        outcome = if disposition in [:failed, :aborted], do: disposition, else: :completed
        {:reply, :ok, finish_activity(state, token, outcome, true, true)}

      nil ->
        {:reply, :ok, state}

      _other ->
        {:reply, {:error, :unknown_activity}, state}
    end
  end

  def handle_call(:receipt_cohort, _from, state) do
    drain = Drain.close_receipts(state.drain, map_size(state.receipt_producers) == 0)
    cohort = %{closed?: not is_nil(drain) and drain.receipts_closed?, entries: Drain.receipt_entries(drain, state.activities), pending_producers: map_size(state.receipt_producers), producers_failed?: not is_nil(drain) and drain.receipt_producers_failed?}
    {:reply, cohort, %{state | drain: drain}}
  end

  def handle_call(:force_receipt_cohort, _from, state) do
    state = Enum.reduce(Map.keys(state.receipt_producers), state, &finish_receipt_producer_state(&2, &1, true))

    state =
      Enum.reduce(state.activities, state, fn
        {token, %{kind: :receipt_settlement, pid: pid}}, acc ->
          Process.exit(pid, :kill)
          finish_activity(acc, token, :aborted, true, true)

        _other, acc ->
          acc
      end)

    {:reply, :ok, %{state | drain: Drain.close_receipts(state.drain, true)}}
  end

  def handle_call({:register, kind, pid, opts}, _from, state) do
    token = make_ref()
    monitor = Process.monitor(pid)

    entry = Entry.new(token, kind, pid, monitor)

    entry =
      case {Keyword.get(opts, :direct_cleanup_ref), Keyword.get(opts, :direct_cleanup_parent)} do
        {ref, parent} when is_reference(ref) and is_pid(parent) ->
          Map.merge(entry, %{direct_ref: ref, direct_parent: parent})

        _ ->
          entry
      end

    state = %{
      state
      | activities: Map.put(state.activities, token, entry),
        monitors: Map.put(state.monitors, monitor, token)
    }

    {:reply, {:ok, token}, state}
  end

  def handle_call({:admit, token}, _from, %{draining?: true} = state) do
    reply =
      if Map.has_key?(state.activities, token),
        do: {:error, :owner_drained},
        else: {:error, :unknown_activity}

    {:reply, reply, state}
  end

  def handle_call({:admit, token}, _from, state) do
    case Map.fetch(state.activities, token) do
      {:ok, entry} ->
        activities = Map.put(state.activities, token, %{entry | status: :admitted})
        {:reply, :ok, %{state | activities: activities}}

      :error ->
        {:reply, {:error, :unknown_activity}, state}
    end
  end

  def handle_call({:unregister, token, outcome}, _from, state) do
    {:reply, :ok, finish_activity(state, token, outcome)}
  end

  def handle_call({:complete, token, outcome}, _from, state) do
    {:reply, :ok, finish_activity(state, token, outcome, true, true)}
  end

  def handle_call({:set_cancel_recipient, token, pid}, _from, state) do
    case Map.fetch(state.activities, token) do
      {:ok, entry} ->
        entry = Entry.set_recipient(entry, pid)
        activities = Map.put(state.activities, token, entry)
        {:reply, :ok, %{state | activities: activities}}

      :error ->
        {:reply, {:error, :unknown_activity}, state}
    end
  end

  def handle_call({:handoff_cancel_recipient, token, from_pid, to_pid}, _from, state) do
    case Map.get(state.activities, token) do
      entry when is_map(entry) ->
        case Entry.handoff(entry, from_pid, to_pid) do
          {:ok, entry} ->
            {:reply, :ok, put_in(state.activities[token], entry)}

          {:cancelled, reason, ack_pid} ->
            {:reply, {:cancelled, reason, ack_pid}, state}

          :stale ->
            {:reply, {:error, :unknown_activity}, state}
        end

      nil ->
        {:reply, {:error, :unknown_activity}, state}
    end
  end

  def handle_call({:delivery_target, pid}, _from, state) do
    reply =
      Enum.find_value(state.activities, :unknown, fn {token, entry} ->
        if entry.pid == pid do
          {token, ack_pid, status} = Entry.delivery_target(entry, token)
          {:ok, token, ack_pid, status}
        end
      end)

    {:reply, reply, state}
  end

  def handle_call(:begin_drain, _from, %{drain: nil} = state) do
    {drain, epoch, entries} = Drain.begin(nil, state.activities)
    state = %{state | draining?: true, drain: drain}
    {:reply, {epoch, entries}, state}
  end

  def handle_call(:begin_drain, _from, state) do
    {drain, epoch, entries} = Drain.begin(state.drain, state.activities)
    {:reply, {epoch, entries}, %{state | drain: drain}}
  end

  def handle_call({:complete_drain, epoch}, _from, %{drain: %{epoch: epoch}} = state) do
    pending? = map_size(state.receipt_producers) > 0 or Enum.any?(Drain.receipt_entries(state.drain, state.activities), &(&1.status == :active))

    if pending?, do: {:reply, {:error, :receipt_work_pending}, state}, else: {:reply, :ok, %{state | drain: nil}}
  end

  def handle_call({:complete_drain, _epoch}, _from, state), do: {:reply, :ok, state}

  def handle_call({:mark_terminal_delivered, pid}, _from, state) do
    activities =
      Map.new(state.activities, fn
        {token, %{pid: ^pid} = entry} -> {token, Map.put(entry, :terminal_delivered?, true)}
        other -> other
      end)

    {:reply, :ok, %{state | activities: activities}}
  end

  def handle_call({:cancel, token, reason, force?}, _from, state) do
    case Map.get(state.activities, token) do
      %{terminal_delivered?: true, status: status} when status in [:registered, :admitted] and not force? ->
        {:reply, :terminal_delivered, state}

      %{status: status} = entry when status in [:registered, :admitted] ->
        _marked = ExecutionRegistry.mark_interruption(entry.pid, "owner_drained")
        {cancel_pid, entry} = Entry.cancel(entry, reason)
        send(cancel_pid, {:websocket_activity_cancel, token, reason})
        activities = Map.put(state.activities, token, entry)

        {:reply, :ok, %{state | activities: activities}}

      _finished_unknown_or_cancelling ->
        {:reply, :ok, state}
    end
  end

  def handle_call({:status, token}, _from, state) do
    reply =
      case Map.get(state.activities, token) do
        %{status: status} -> {:active, status}
        nil -> Drain.outcome(state.drain, token)
      end

    {:reply, reply, state}
  end

  def handle_call(:activities, _from, state) do
    entries = Enum.map(state.activities, fn {_token, entry} -> Map.drop(entry, [:monitor]) end)
    {:reply, entries, state}
  end

  def handle_call(:draining?, _from, state), do: {:reply, state.draining?, state}

  @impl GenServer
  def handle_info({:DOWN, monitor, :process, _pid, _reason}, %{receipt_producer_monitors: monitors} = state) when is_map_key(monitors, monitor) do
    token = Map.fetch!(monitors, monitor)
    failed? = is_nil(state.receipt_producers[token].worker)
    {:noreply, finish_receipt_producer_state(state, token, failed?)}
  end

  def handle_info({:DOWN, monitor, :process, _pid, _reason}, state) do
    case Map.pop(state.monitors, monitor) do
      {nil, _monitors} ->
        finished =
          Map.reject(state.finished_direct, fn {_ref, completed} ->
            completed.monitor == monitor
          end)

        {:noreply, %{state | finished_direct: finished}}

      {token, monitors} ->
        state = %{state | monitors: monitors}
        {:noreply, finish_activity(state, token, :failed, false)}
    end
  end

  defp finish_activity(
         state,
         token,
         outcome,
         demonitor? \\ true,
         honor_outcome? \\ false
       ) do
    case Map.pop(state.activities, token) do
      {nil, _activities} ->
        state

      {entry, activities} ->
        if cleanup = Map.get(entry, :direct_cleanup), do: reply_direct_waiters(cleanup)
        state = retain_direct_cleanup(state, Map.get(entry, :direct_cleanup))

        if demonitor?, do: Process.demonitor(entry.monitor, [:flush])
        monitors = Map.delete(state.monitors, entry.monitor)

        outcome =
          if not honor_outcome? and entry.status == :cancelling,
            do: :aborted,
            else: outcome

        state = %{state | activities: activities, monitors: monitors}

        %{state | drain: Drain.record_outcome(state.drain, token, entry, outcome)}
    end
  end

  defp finish_receipt_producer_state(state, token, failed?) do
    {producer, producers} = Map.pop(state.receipt_producers, token)
    Process.demonitor(producer.monitor, [:flush])
    drain = if failed? and state.drain, do: %{state.drain | receipt_producers_failed?: true}, else: state.drain
    %{state | receipt_producers: producers, receipt_producer_monitors: Map.delete(state.receipt_producer_monitors, producer.monitor), drain: drain}
  end

  defp retain_direct_cleanup(state, %{consumed?: true}), do: state

  defp retain_direct_cleanup(state, %{context: context, receipt: receipt})
       when not is_nil(receipt) do
    completed = %{context: context, receipt: receipt, monitor: Process.monitor(context.parent)}
    %{state | finished_direct: Map.put(state.finished_direct, context.ref, completed)}
  end

  defp retain_direct_cleanup(state, _cleanup), do: state

  defp server(opts), do: Keyword.get(opts, :name, __MODULE__)

  defp direct_entry(state, pid),
    do: Enum.find(state.activities, fn {_token, entry} -> entry.pid == pid end)

  defp direct_receipt(%{receipt: nil}), do: :none
  defp direct_receipt(%{receipt: receipt}), do: {:ok, receipt}

  defp reply_direct_waiters(cleanup),
    do: Enum.each(cleanup.waiters, &GenServer.reply(&1, direct_receipt(cleanup)))
end
