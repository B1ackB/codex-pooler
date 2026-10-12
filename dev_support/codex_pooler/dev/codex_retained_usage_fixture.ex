defmodule CodexPooler.Dev.CodexRetainedUsageFixture do
  @moduledoc "Owned provider-free retained-usage controls; never installed in the product endpoint."
  use GenServer

  alias CodexPooler.Dev.CodexRetainedUsageFixtureData, as: Data
  alias CodexPooler.Dev.CodexRetainedUsageFixtureDrainTrace, as: DrainTrace
  alias CodexPooler.Dev.CodexRetainedUsageFixtureUI, as: UI
  alias CodexPooler.Dev.QaBackgroundWorkers
  alias CodexPooler.Gateway.Transports.Websocket.{ActivityRegistry, RolloutDrain, WebsocketOwnerSession}
  alias CodexPooler.Gateway.Transports.Websocket.UpstreamWebsocketSession.TerminalDiscriminator
  alias CodexPooler.Gateway.Transports.Websocket.WebsocketOwnerSession.Persistence
  alias CodexPooler.Platform.InstancePresence.Identity

  @spec validate_database!(String.t()) :: :ok
  def validate_database!(database) do
    unless Regex.match?(~r/\Acodex_pooler_relqa_[a-f0-9]{16}\z/, database), do: raise(ArgumentError, "retained_fixture_database_not_owned")
    :ok
  end

  @spec boot!() :: pid()
  def boot! do
    Logger.configure(level: :info)
    validate_database!(System.fetch_env!("CODEX_POOLER_DEV_POSTGRES_DB"))
    endpoint = Application.fetch_env!(:codex_pooler, CodexPoolerWeb.Endpoint)
    Application.put_env(:codex_pooler, CodexPoolerWeb.Endpoint, Keyword.merge(endpoint, watchers: [], reloadable_apps: []))
    QaBackgroundWorkers.disable_before_boot!()
    {:ok, _} = Application.ensure_all_started(:codex_pooler)
    :ok = QaBackgroundWorkers.verify_disabled!()
    {:ok, pid} = GenServer.start(__MODULE__, [], name: __MODULE__)
    pid
  end

  @spec loaded_beam_hashes() :: %{String.t() => String.t()}
  def loaded_beam_hashes do
    modules = [CodexPooler.Accounting.RequestLifecycle, WebsocketOwnerSession, CodexPooler.Gateway.Runtime.Finalization.RetainedUsage, ActivityRegistry, __MODULE__]

    Map.new(modules, fn module ->
      bytes = module |> :code.which() |> File.read!()
      {:ok, {^module, loaded_md5}} = :beam_lib.md5(bytes)
      true = loaded_md5 == module.module_info(:md5)
      {Atom.to_string(module), :crypto.hash(:sha256, bytes) |> Base.encode16(case: :lower)}
    end)
  end

  @spec start_owner(map(), pid(), String.t(), boolean()) :: {:ok, pid()} | {:error, term()}
  def start_owner(session, coordinator, thread, hold?) do
    WebsocketOwnerSession.start_owner(
      codex_session_id: session.id,
      pool_id: session.pool_id,
      api_key_id: session.api_key_id,
      owner_lease_token: session.owner_lease_token,
      owner_instance_id: session.owner_instance_id,
      upstream: boundary(coordinator, thread, hold?),
      terminal_delivery_timeout_ms: 60_000
    )
  end

  @spec hold_writer(pid(), String.t(), function(), binary(), term()) :: :ok
  def hold_writer(coordinator, thread, writer, frame, discriminator) do
    spawn(fn ->
      monitor = Process.monitor(coordinator)
      send(coordinator, {:retained_writer, thread, self()})

      receive do
        :deliver -> writer.(frame, discriminator)
        :discard -> :ok
        {:DOWN, ^monitor, :process, ^coordinator, _} -> :ok
      after
        60_000 -> :ok
      end

      Process.demonitor(monitor, [:flush])
    end)

    :ok
  end

  @spec boundary(pid(), String.t(), boolean()) :: map()
  def boundary(coordinator, thread, hold?) do
    real = WebsocketOwnerSession.default_upstream_boundary()
    once = :atomics.new(1, [])

    %{
      real
      | send: fn upstream, request, writer ->
          Data.observe_client_binding(coordinator, thread, request)

          held = fn frame, discriminator ->
            if hold? and TerminalDiscriminator.terminal?(discriminator) and :atomics.compare_exchange(once, 1, 0, 1) == :ok,
              do: hold_writer(coordinator, thread, writer, frame, discriminator),
              else: writer.(frame, discriminator)
          end

          real.send.(upstream, request, held)
        end
    }
  end

  @impl true
  def init(_) do
    :ok = Data.verify_target!()
    {:ok, listener} = :gen_tcp.listen(String.to_integer(System.fetch_env!("RETAINED_CONTROL_PORT")), [:binary, packet: :line, packet_size: 16_384, active: false, ip: {127, 0, 0, 1}, reuseaddr: true])
    coordinator = self()
    spawn_link(fn -> accept(listener, coordinator) end)
    {:ok, %{listener: listener, nonce: System.fetch_env!("RETAINED_CONTROL_NONCE"), threads: %{}, drain: nil, drain_trace: nil, ui_barrier: nil, closed_cohort: nil, owner_trace: nil, trace_events: []}}
  end

  @impl true
  def handle_call({:control, %{"nonce" => nonce} = command}, _from, %{nonce: nonce} = state) do
    {reply, state} = dispatch(command, state)
    {:reply, reply, state}
  rescue
    error ->
      {:reply, %{error: "fixture_control_failed", errorClass: inspect(error.__struct__), at: stack_location(__STACKTRACE__)}, state}
  catch
    :exit, _ -> {:reply, %{error: "fixture_control_process_unavailable"}, state}
  end

  def handle_call({:control, _}, _from, state), do: {:reply, %{error: "fixture_control_unauthorized"}, state}

  @impl true
  def handle_info({:retained_writer, thread, writer}, state) do
    {:noreply, update_in(state.threads[thread].writers, &[writer | &1])}
  end

  def handle_info({:client_binding, thread, request_id, binding}, state) do
    {:noreply, put_in(state.threads[thread].bindings[request_id], binding)}
  end

  def handle_info({:ui_query_held, pid, token}, state), do: {:noreply, %{state | ui_barrier: %{pid: pid, token: token}}}

  def handle_info({:drain_finished, trace, result}, %{drain_trace: trace} = state), do: {:noreply, %{state | drain: result, drain_trace: nil}}
  def handle_info({:drain_finished, _trace, _result}, state), do: {:noreply, state}

  def handle_info({:drain_cohort_closed, _trace, epoch}, state), do: {:noreply, %{state | closed_cohort: %{epoch: fingerprint(epoch), closed: true}}}

  def handle_info({:stop_owner_trace, owner}, %{owner_trace: owner} = state) do
    :erpc.call(node(owner), __MODULE__, :configure_owner_trace, [owner, self(), false])
    {:noreply, %{state | owner_trace: nil}}
  end

  def handle_info({:stop_owner_trace, _owner}, state), do: {:noreply, state}

  def handle_info({:trace, _pid, :call, {module, function, args}}, state) when module in [WebsocketOwnerSession, Persistence], do: {:noreply, record_trace(state, module, function, length(args), "call")}
  def handle_info({:trace, _pid, :return_from, {module, function, arity}, _result}, state) when module in [WebsocketOwnerSession, Persistence], do: {:noreply, record_trace(state, module, function, arity, "return")}

  def handle_info({:trace, _pid, _kind, _details}, state), do: {:noreply, state}
  def handle_info({:trace, _pid, _kind, _details, _result}, state), do: {:noreply, state}

  @impl true
  def terminate(_, state) do
    DrainTrace.stop(state.drain_trace)
    :gen_tcp.close(state.listener)

    if state.ui_barrier do
      :telemetry.detach(state.ui_barrier.token)
      if state.ui_barrier.pid, do: send(state.ui_barrier.pid, {state.ui_barrier.token, :release})
    end

    Enum.each(state.threads, fn {_thread, item} ->
      Enum.each(item.writers, &send(&1, :discard))
      if item.lock, do: send(item.lock, :unlock)
    end)

    if state.owner_trace, do: :erpc.call(node(state.owner_trace), __MODULE__, :configure_owner_trace, [state.owner_trace, self(), false])
  catch
    :error, {:erpc, _} -> :ok
  end

  defp dispatch(%{"op" => "status"}, state) do
    identity = Identity.local()
    :ok = QaBackgroundWorkers.verify_disabled!()
    hashes = loaded_beam_hashes()

    {%{node: Atom.to_string(node()), instanceId: identity.instance_id, bootId: identity.boot_id, nodes: Enum.map(Node.list(), &Atom.to_string/1), osPid: System.pid(), obanDisabled: true, beamHashes: hashes, drain: state.drain, registry: cohort(state)}, state}
  end

  defp dispatch(%{"op" => "connect", "node" => peer}, state) do
    # Only the launcher-provisioned peer is accepted; no arbitrary node atoms.
    true = peer == System.fetch_env!("RETAINED_PEER_NODE")
    true = Node.connect(String.to_atom(peer))
    {%{connected: true}, state}
  end

  defp dispatch(%{"op" => "arm", "threadId" => thread, "windowId" => window, "mode" => mode, "topology" => topology, "state" => scenario}, state) when mode in ["full", "lite"] and topology in ["local", "remote"] and scenario in ["held", "delivered", "no_receipt"] do
    false = Map.has_key?(state.threads, thread)
    owner_node = if topology == "remote", do: String.to_existing_atom(System.fetch_env!("RETAINED_PEER_NODE")), else: node()
    session = Data.create_session!(window, mode, owner_node)
    {:ok, owner} = :erpc.call(owner_node, __MODULE__, :start_owner, [session, self(), thread, scenario == "held"])
    item = %{session: session, owner: owner, writers: [], lock: nil, bindings: %{}}
    {%{armed: true, sessionId: session.id, ownerNode: Atom.to_string(owner_node), ownerPid: inspect(owner)}, put_in(state.threads[thread], item)}
  end

  defp dispatch(%{"op" => "snapshot", "threadId" => thread} = command, state) do
    item = Map.fetch!(state.threads, thread)
    bindings = if command["turnId"], do: Map.put(item.bindings, :selector, %{clientThreadId: thread, clientTurnId: command["turnId"]}), else: item.bindings
    # Observe completion before reading accounting: a later worker exit cannot
    # certify a database snapshot taken before its commit.
    owner = owner_snapshot(item)
    registry = cohort(state)
    registries = [%{node: Atom.to_string(node()), registry: registry}]
    registries = if node(item.owner) == node(), do: registries, else: registries ++ [%{node: Atom.to_string(node(item.owner)), registry: :erpc.call(node(item.owner), __MODULE__, :registry_snapshot, [])}]
    rows = Data.rows(item.session.id, bindings)
    verified = Map.new(Enum.filter(rows, &Map.has_key?(&1, :clientTurnId)), &{&1.requestId, Map.take(&1, [:clientThreadId, :clientTurnId])})
    state = put_in(state.threads[thread].bindings, Map.merge(item.bindings, verified))
    {%{rows: rows, owner: owner, registry: registry, registries: registries, writerCount: length(item.writers)}, state}
  end

  defp dispatch(%{"op" => "release", "threadId" => thread, "action" => action}, state) when action in ["deliver", "discard"] do
    item = Map.fetch!(state.threads, thread)
    message = if action == "deliver", do: :deliver, else: :discard
    Enum.each(item.writers, &send(&1, message))
    {%{released: length(item.writers)}, put_in(state.threads[thread].writers, [])}
  end

  defp dispatch(%{"op" => "lock", "threadId" => thread}, state) do
    item = Map.fetch!(state.threads, thread)
    nil = item.lock
    %{requestId: request} = List.last(Data.rows(item.session.id))
    {pid, backend} = Data.lock_request!(request)
    {%{locked: true, backendPid: backend}, put_in(state.threads[thread].lock, pid)}
  end

  defp dispatch(%{"op" => "unlock", "threadId" => thread}, state) do
    pid = state.threads[thread].lock
    send(pid, :unlock)
    {%{unlocked: true}, put_in(state.threads[thread].lock, nil)}
  end

  defp dispatch(%{"op" => "drain"}, %{drain_trace: nil} = state) do
    {:ok, trace} = DrainTrace.start(self(), Process.whereis(ActivityRegistry), fn -> RolloutDrain.drain_for_shutdown(15_000) end, 20_000)

    try do
      case await_drain_epoch(trace, System.monotonic_time(:millisecond) + 5_000) do
        {:started, epoch} -> {%{started: true, epoch: epoch}, %{state | drain: %{status: "pending"}, drain_trace: trace}}
        {:finished, result} -> {%{started: true, finished: true}, %{state | drain: result}}
      end
    rescue
      error ->
        DrainTrace.stop(trace)
        reraise error, __STACKTRACE__
    catch
      kind, reason ->
        DrainTrace.stop(trace)
        :erlang.raise(kind, reason, __STACKTRACE__)
    end
  end

  defp dispatch(%{"op" => "trace_start", "threadId" => thread}, state) do
    nil = state.owner_trace
    owner = Map.fetch!(state.threads, thread).owner
    trace = :erpc.call(node(owner), __MODULE__, :configure_owner_trace, [owner, self(), true])
    Process.send_after(self(), {:stop_owner_trace, owner}, 20_000)
    {%{tracing: true, trace: trace}, %{state | owner_trace: owner, trace_events: []}}
  end

  defp dispatch(%{"op" => "trace_stop"}, state) do
    if state.owner_trace, do: :erpc.call(node(state.owner_trace), __MODULE__, :configure_owner_trace, [state.owner_trace, self(), false])
    {%{events: Enum.reverse(state.trace_events)}, %{state | owner_trace: nil}}
  end

  defp dispatch(%{"op" => "diagnostics", "threadId" => thread}, state) do
    owner = Map.fetch!(state.threads, thread).owner
    {%{owner: :erpc.call(node(owner), __MODULE__, :owner_process_info, [owner]), sql: Data.sql_waits(), events: Enum.reverse(state.trace_events), registry: cohort(state)}, state}
  end

  defp dispatch(%{"op" => "ui", "action" => "arm_query", "requestId" => request}, state) do
    nil = state.ui_barrier
    token = UI.arm_query_barrier(self(), request)
    {%{armed: true}, %{state | ui_barrier: %{token: token, pid: nil}}}
  end

  defp dispatch(%{"op" => "ui", "action" => "query_status"}, state), do: {%{held: not is_nil(state.ui_barrier) and is_pid(state.ui_barrier.pid), pid: if(state.ui_barrier, do: inspect(state.ui_barrier.pid))}, state}

  defp dispatch(%{"op" => "ui", "action" => "release_query"}, state) do
    barrier = state.ui_barrier
    :telemetry.detach(barrier.token)
    if barrier.pid, do: send(barrier.pid, {barrier.token, :release})
    {%{released: true}, %{state | ui_barrier: nil}}
  end

  defp dispatch(%{"op" => "ui"} = command, state), do: {UI.mutate(command), state}

  defp dispatch(_, state), do: {%{error: "fixture_control_unknown"}, state}

  defp await_drain_epoch(trace, deadline) do
    case :sys.get_state(ActivityRegistry).drain do
      %{epoch: epoch} ->
        {:started, fingerprint(epoch)}

      nil ->
        receive do
          {:drain_finished, ^trace, result} -> {:finished, result}
        after
          10 ->
            if System.monotonic_time(:millisecond) >= deadline, do: raise("fixture_drain_epoch_unobserved")
            await_drain_epoch(trace, deadline)
        end
    end
  end

  defp owner_snapshot(item) do
    state = :sys.get_state(item.owner, 5_000)
    active = state.active_turn

    usage =
      case active do
        %{pending_result: {:ok, %{response_usage: usage}}} -> Map.take(usage, [:status, :input_tokens, :output_tokens, :total_tokens])
        _ -> nil
      end

    %{live: true, node: Atom.to_string(node(item.owner)), pid: inspect(item.owner), instanceId: :erpc.call(node(item.owner), Identity, :local, []).instance_id, heldUsage: usage, requestId: active && active.admission_request_id, attemptId: active && active.admission_attempt_id, terminalForwarded: active && active.terminal_forwarded?}
  catch
    :exit, _ -> %{live: false, node: Atom.to_string(node(item.owner)), pid: inspect(item.owner), heldUsage: nil}
  end

  @spec registry_snapshot() :: map()
  def registry_snapshot, do: cohort(%{closed_cohort: nil})

  defp cohort(fixture) do
    state = :sys.get_state(ActivityRegistry)
    drain = state.drain
    entries = Enum.filter(state.activities, fn {_token, entry} -> entry.kind == :receipt_settlement end)
    %{closed: (not is_nil(drain) and drain.receipts_closed?) or not is_nil(fixture.closed_cohort), epoch: if(drain, do: fingerprint(drain.epoch), else: fixture.closed_cohort && fixture.closed_cohort.epoch), pendingProducers: map_size(state.receipt_producers), producersFailed: not is_nil(drain) and drain.receipt_producers_failed?, entries: Enum.map(entries, fn {token, entry} -> %{kind: entry.kind, status: inspect(entry.status), pid: inspect(entry.pid), receiptIdentity: Map.take(entry.receipt_identity, [:request_id, :attempt_id, :replay_generation]), inDrain: not is_nil(drain) and MapSet.member?(drain.tokens, token)} end)}
  end

  @spec configure_owner_trace(pid(), pid(), boolean()) :: map()
  def configure_owner_trace(owner, tracer, enabled) do
    patterns = Enum.map([{:drain, 2}, {:begin_drain_state, 1}, {:drain_now, 2}, {:record_drain_interruption, 1}, {:finish_active_turn, 2}, {:retain_interrupted_receipt, 2}, {:start_owner_receipt, 4}], fn {function, arity} -> {WebsocketOwnerSession, function, arity} end) ++ [{Persistence, :interrupt_codex_session, 2}]

    matched =
      Enum.map(patterns, fn {module, function, arity} ->
        count = :erlang.trace_pattern({module, function, arity}, if(enabled, do: [{:_, [], [{:return_trace}]}], else: false), [:local])
        %{module: Atom.to_string(module), function: Atom.to_string(function), arity: arity, matched: count}
      end)

    if Process.alive?(owner), do: :erlang.trace(owner, enabled, [:call, {:tracer, tracer}])
    %{matched: matched, flags: inspect(:erlang.trace_info(owner, :flags))}
  end

  @spec owner_process_info(pid()) :: map()
  def owner_process_info(owner) do
    previous = :erlang.system_flag(:backtrace_depth, 40)

    try do
      case Process.info(owner, [:current_function, :status, :message_queue_len, :current_stacktrace]) do
        nil -> %{alive: false}
        info -> %{alive: true, status: info[:status], queue: info[:message_queue_len], currentFunction: inspect(info[:current_function]), stack: Enum.map(info[:current_stacktrace], fn {module, function, arity, _location} -> "#{inspect(module)}.#{function}/#{arity}" end)}
      end
    after
      :erlang.system_flag(:backtrace_depth, previous)
    end
  end

  defp record_trace(state, module, function, arity, event) when length(state.trace_events) < 128 do
    %{state | trace_events: [%{module: Atom.to_string(module), function: Atom.to_string(function), arity: arity, event: event, monotonicMs: System.monotonic_time(:millisecond)} | state.trace_events]}
  end

  defp record_trace(state, _module, _function, _arity, _event) do
    if state.owner_trace, do: :erpc.call(node(state.owner_trace), __MODULE__, :configure_owner_trace, [state.owner_trace, self(), false])
    %{state | owner_trace: nil}
  end

  defp stack_location([{module, function, arity, _location} | _]) when is_atom(module) and is_atom(function) do
    "#{inspect(module)}.#{function}/#{if is_integer(arity), do: arity, else: length(arity)}"
  end

  defp stack_location(_stack), do: "fixture_callback"

  defp fingerprint(value), do: value |> :erlang.term_to_binary() |> then(&:crypto.hash(:sha256, &1)) |> Base.encode16(case: :lower)

  defp accept(listener, coordinator) do
    case :gen_tcp.accept(listener) do
      {:ok, socket} ->
        spawn(fn -> serve(socket, coordinator) end)
        accept(listener, coordinator)

      {:error, :closed} ->
        :ok
    end
  end

  defp serve(socket, coordinator) do
    with {:ok, line} <- :gen_tcp.recv(socket, 0, 5_000), {:ok, command} <- CodexPooler.JSON.decode(line) do
      response = GenServer.call(coordinator, {:control, command}, 30_000)
      :gen_tcp.send(socket, CodexPooler.JSON.encode!(response) <> "\n")
    end
  after
    :gen_tcp.close(socket)
  end
end
