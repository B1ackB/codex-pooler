defmodule CodexPooler.Gateway.Runtime.NativeResponseSteering do
  @moduledoc false

  use GenServer

  require Logger

  alias CodexPooler.{Access, Accounting, Repo}
  alias CodexPooler.Accounting.RequestLifecycle.Reservation, as: RequestReservation
  alias CodexPooler.Gateway.Payloads.RequestOptions
  alias CodexPooler.Gateway.Persistence.SessionContinuity, as: ContinuityStore
  alias CodexPooler.Gateway.Routing.SessionContinuity
  alias CodexPooler.Gateway.Runtime.Dispatch.{AccountingReservation, SelectedCandidateContext}
  alias CodexPooler.Gateway.Runtime.Finalization
  alias CodexPooler.Gateway.Transports.ProviderCreditsAdmission
  alias CodexPooler.Gateway.Transports.Websocket.{ActivityRegistry, DiagnosticTaxonomy, ResponseSteer, UpstreamWebsocketSession, WebsocketOwnerSession, WebsocketRequestCallbacks}
  alias CodexPooler.Gateway.Transports.Websocket.WebsocketOwnerSession.Status
  alias CodexPooler.Gateway.Websocket.DeliveryReceiptHandoff
  alias CodexPooler.Gateway.Websocket.DirectCleanup
  alias CodexPooler.Platform.ExecutionIdentity

  @type identity :: %{request_id: Ecto.UUID.t(), attempt_id: Ecto.UUID.t(), replay_generation: non_neg_integer()}
  @type request_identity :: {Ecto.UUID.t(), Ecto.UUID.t()}
  @type result :: {:ok, map()} | {:error, map()}
  @type prepared :: %{request_id: Ecto.UUID.t(), attempt_id: Ecto.UUID.t(), writer: UpstreamWebsocketSession.Request.writer(), frame_observer: UpstreamWebsocketSession.Request.frame_observer(), provider_credits_admission: ProviderCreditsAdmission.Receipt.t() | nil}
  @type delivery_outcome :: :delivered | :aborted

  @spec start(pid()) :: GenServer.on_start()
  def start(socket) when is_pid(socket), do: GenServer.start(__MODULE__, socket)

  @spec prepare(SelectedCandidateContext.t(), map()) :: pid() | nil | {:error, :owner_unavailable | :owner_busy}
  def prepare(context, callbacks) do
    socket = socket_pid(context.request_options)

    if native_relay?(context.request_options) and is_pid(socket) do
      ref = make_ref()
      send(socket, {:native_response_steering_prepare, self(), ref, trim_context(context), callbacks})

      receive do
        {:native_response_steering_prepared, ^ref, lane} when is_pid(lane) -> lane
        {:native_response_steering_prepare_failed, ^ref, reason} -> {:error, reason}
      after
        5_000 -> {:error, :owner_unavailable}
      end
    end
  end

  @spec set_context(pid(), SelectedCandidateContext.t(), map(), pid()) :: :ok | {:error, :owner_busy}
  def set_context(lane, context, callbacks, dispatcher), do: GenServer.call(lane, {:prepare, context, callbacks, dispatcher})

  @spec select_request(pid() | nil, Ecto.UUID.t(), Ecto.UUID.t(), pid(), pid() | nil) :: :ok | {:error, term()}
  def select_request(nil, _request, _attempt, _upstream, _owner), do: :ok
  def select_request(lane, request, attempt, upstream, owner), do: lane_call(fn -> GenServer.call(lane, {:select_request, {request, attempt}, upstream, owner}, :infinity) end)

  @spec dispatch_complete(pid() | nil, request_identity()) :: :ok
  def dispatch_complete(nil, _identity), do: :ok
  def dispatch_complete(lane, identity), do: GenServer.cast(lane, {:dispatch_complete, identity})

  @spec activate(pid(), pid(), pid() | nil) :: :ok | {:error, term()}
  def activate(lane, upstream, owner), do: lane_call(fn -> GenServer.call(lane, {:activate, upstream, owner}, :infinity) end)

  @spec original_result(pid() | nil, request_identity(), result()) :: :ok
  def original_result(nil, _request, _result), do: :ok

  def original_result(lane, request, result) do
    _reply = lane_call(fn -> GenServer.call(lane, {:original_result, request, result}, :infinity) end)
    :ok
  end

  @spec terminal(pid(), Ecto.UUID.t(), map()) :: {:ok, result(), term()} | {:error, term()}
  def terminal(lane, request, finalization), do: lane_call(fn -> GenServer.call(lane, {:terminal, request, finalization}, :infinity) end)

  @spec open_successor(pid(), String.t()) :: {:ok, prepared()} | {:error, term()}
  def open_successor(lane, response_id), do: lane_call(fn -> GenServer.call(lane, {:open, response_id}, :infinity) end)

  @spec frame(pid(), identity(), binary(), UpstreamWebsocketSession.TerminalDiscriminator.t()) :: :ok | {:error, term()}
  def frame(lane, identity, data, discriminator), do: lane_call(fn -> GenServer.call(lane, {:frame, identity, data, discriminator}, :infinity) end)

  @spec control(pid(), binary()) :: :ok
  def control(lane, data), do: GenServer.cast(lane, {:control, data})

  @spec peer_close(pid(), integer() | nil, binary() | nil) :: :ok
  def peer_close(lane, code, reason), do: GenServer.cast(lane, {:peer_close, code, reason})

  @spec delivery_complete(pid(), identity(), delivery_outcome()) :: :ok
  def delivery_complete(lane, identity, outcome) when outcome in [:delivered, :aborted], do: GenServer.cast(lane, {:delivery_complete, identity, outcome})

  @spec delivery_complete(pid(), identity(), delivery_outcome(), DeliveryReceiptHandoff.envelope()) :: :ok
  def delivery_complete(lane, identity, outcome, envelope) when outcome in [:delivered, :aborted], do: GenServer.cast(lane, {:delivery_complete, identity, outcome, envelope})

  @spec closed(pid() | nil, request_identity(), term()) :: :ok
  def closed(nil, _identity, _reason), do: :ok
  def closed(lane, identity, reason), do: GenServer.cast(lane, {:closed, identity, reason})

  @spec cancel(pid(), :client_disconnected | :owner_drained) :: :ok
  def cancel(lane, reason), do: GenServer.cast(lane, {:cancel, reason})

  @spec capture_downstream_idle_timeout(pid(), identity(), String.t()) :: term()
  def capture_downstream_idle_timeout(lane, identity, client_activity) do
    GenServer.call(lane, {:capture_downstream_idle_timeout, identity, client_activity}, 100)
  catch
    :exit, _reason -> {:error, :attribution_unavailable}
  end

  @impl GenServer
  @spec format_status(map()) :: map()
  def format_status(status), do: Status.format(status)
  @impl GenServer
  def init(socket) do
    Process.flag(:sensitive, true)
    Process.flag(:trap_exit, true)
    {:ok, %{socket: socket, socket_monitor: Process.monitor(socket), socket_dead?: false, prepared: %{}, context: nil, callbacks: nil, active: nil, upstream: nil, upstream_monitor: nil, owner: nil, activated?: false, admission_revoked?: false, draining?: false, original_result: nil, activity: nil}}
  end

  @impl GenServer
  def handle_call({:prepare, context, callbacks, dispatcher}, {socket, _tag}, %{socket: socket} = state) when is_pid(dispatcher) do
    {:reply, :ok, prepare_context(state, context, callbacks, dispatcher)}
  end

  def handle_call({:prepare, _context, _callbacks, _dispatcher}, _from, state), do: {:reply, {:error, :owner_busy}, state}

  def handle_call({:capture_downstream_idle_timeout, identity, client_activity}, {socket, _tag}, %{socket: socket, owner: nil, active: context} = state) when not is_nil(context) do
    result =
      if identity(context) == identity do
        request = context.reserved.request
        session = context.request_options.continuity.codex_session
        receipt = Map.merge(identity, %{session_id: session.id, correlation_id: request.correlation_id, api_key_id: request.api_key_id, owner_binding: nil})
        {{:ok, :ok}, receipt}
      else
        {:ok, :stale}
      end

    case result do
      {result, receipt} when is_map(receipt) -> {:reply, result, Map.put(state, :idle_interruption, {receipt, client_activity})}
      result -> {:reply, result, state}
    end
  end

  def handle_call({:capture_downstream_idle_timeout, _identity, _client_activity}, _from, state), do: {:reply, {:ok, :stale}, state}

  def handle_call({:select_request, key, upstream, owner}, _from, %{active: nil, draining?: false, socket_dead?: false} = state) do
    case Map.fetch(state.prepared, key) do
      {:ok, entry} ->
        if is_reference(state.upstream_monitor), do: Process.demonitor(state.upstream_monitor, [:flush])
        state = %{state | context: entry.context, callbacks: entry.callbacks, original_result: entry.result, activated?: false, admission_revoked?: false, upstream: upstream, upstream_monitor: Process.monitor(upstream), owner: owner}
        {:reply, :ok, state}

      :error ->
        {:reply, {:error, :stale_generation}, state}
    end
  end

  def handle_call({:select_request, _key, _upstream, _owner}, _from, state), do: {:reply, {:error, :owner_busy}, state}

  def handle_call({:activate, upstream, owner}, _from, %{admission_revoked?: false, draining?: false, socket_dead?: false} = state) do
    with {:ok, context} <- authorize(state.context),
         {:ok, _receipt} <- ProviderCreditsAdmission.admit(ProviderCreditsAdmission.from_selected(context)) do
      if is_reference(state.upstream_monitor), do: Process.demonitor(state.upstream_monitor, [:flush])
      {:reply, :ok, %{state | context: context, upstream: upstream, upstream_monitor: Process.monitor(upstream), owner: owner, activated?: true}}
    else
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:activate, _upstream, _owner}, _from, state), do: {:reply, {:error, :owner_unavailable}, state}

  def handle_call({:original_result, key, result}, _from, state) do
    prepared = if Map.has_key?(state.prepared, key), do: Map.update!(state.prepared, key, &%{&1 | result: result}), else: state.prepared
    state = %{state | prepared: prepared}
    state = if state.context && request_identity(state.context) == key, do: %{state | original_result: result}, else: state
    {:reply, :ok, state}
  end

  def handle_call({:terminal, request_id, finalization}, from, state) do
    cond do
      state.active && state.active.reserved.request.id == request_id ->
        context = state.active
        result = finalize(context, Map.put(finalization, :native_response_steering_terminal?, true), state.callbacks)
        complete_idle_attribution(state, context)
        acknowledgement = Finalization.Websocket.native_response_steering_acknowledgement(context, finalization, result)
        result = put_acknowledgement(result, acknowledgement)
        state = retain_settlement(%{state | context: context, active: nil, original_result: result}, result, finalization)
        finish_delivery(state, identity(context), result)
        producer_replied? = reply_settled_failed_producer(from, finalization, result, acknowledgement)
        {delivery, state} = await_delivery(state, identity(context))
        state = state |> complete_activity(delivery) |> fence_delivery(delivery)
        if producer_replied?, do: noreply_after_delivery(state), else: reply_after_delivery({:ok, result, acknowledgement}, state)

      state.context && state.context.reserved.request.id == request_id ->
        result = state.original_result || finalize(state.context, Map.put(finalization, :native_response_steering_terminal?, true), state.callbacks)
        complete_idle_attribution(state, state.context)
        acknowledgement = Finalization.Websocket.native_response_steering_acknowledgement(state.context, finalization, result)
        result = put_acknowledgement(result, acknowledgement)
        state = retain_settlement(%{state | original_result: result}, result, finalization)
        reply_after_delivery({:ok, result, acknowledgement}, drain_lifecycle(state))

      true ->
        {:reply, {:error, :stale_generation}, state}
    end
  end

  def handle_call({:open, response_id}, from, state) do
    state = drain_lifecycle(state)
    open_successor(state, response_id, from)
  end

  def handle_call({:frame, identity, data, discriminator}, _from, %{active: context} = state) when not is_nil(context) do
    result =
      if identity(context) == identity do
        relay_successor_frame(state, identity, data, discriminator)
      else
        {:error, :stale_generation}
      end

    {:reply, result, state}
  end

  def handle_call({:frame, _identity, _data, _discriminator}, _from, state), do: {:reply, {:error, :stale_generation}, state}

  @impl GenServer
  def handle_cast({:delivery_complete, _identity, _outcome}, state), do: {:noreply, state}
  def handle_cast({:delivery_complete, _identity, _outcome, _envelope}, state), do: {:noreply, state}

  def handle_cast({:dispatch_complete, key}, state), do: {:noreply, release_prepared(state, key)}

  def handle_cast({:control, data}, state) do
    send(state.socket, {:native_response_steering_control, self(), data})
    {:noreply, state}
  end

  def handle_cast({:peer_close, code, reason}, state) do
    send(state.socket, {:native_response_steering_close, self(), code || 1000, reason || ""})
    {:noreply, state}
  end

  def handle_cast({:closed, key, reason}, %{context: context} = state) when not is_nil(context) do
    if request_identity(context) == key, do: close_producer(state, reason), else: {:noreply, state}
  end

  def handle_cast({:closed, _key, _reason}, state), do: {:noreply, state}

  def handle_cast({:cancel, reason}, state) do
    state = state |> revoke_admission(reason) |> fail_active(reason)
    noreply_after_delivery(state)
  end

  @impl GenServer
  def handle_info({:DOWN, monitor, :process, _pid, _reason}, %{socket_monitor: monitor} = state) do
    state = %{state | socket_dead?: true} |> revoke_admission(:client_disconnected) |> fail_active(:client_disconnected)
    noreply_after_delivery(state)
  end

  def handle_info({:DOWN, monitor, :process, _pid, _reason}, %{upstream_monitor: monitor} = state), do: close_producer(state, :upstream_websocket_closed_before_terminal)

  def handle_info({:DOWN, monitor, :process, _pid, _reason}, state) do
    prepared = Map.reject(state.prepared, fn {_key, entry} -> entry.dispatcher_monitor == monitor end)
    {:noreply, %{state | prepared: prepared}}
  end

  def handle_info({:websocket_activity_cancel, _token, :owner_drained}, state), do: handle_cast({:cancel, :owner_drained}, state)
  def handle_info({:EXIT, _pid, :shutdown}, state), do: {:stop, :normal, state |> Map.put(:socket_dead?, true) |> revoke_admission(:client_disconnected) |> fail_active(:client_disconnected)}
  def handle_info(_message, state), do: {:noreply, state}

  defp open_successor(%{admission_revoked?: false, original_result: {:ok, %{stale_generation?: true}}} = state, response_id, from), do: open_successor(revoke_admission(state, :stale_generation), response_id, from)

  defp open_successor(%{activated?: true, admission_revoked?: false, draining?: false, socket_dead?: false, active: nil, original_result: {:ok, %{}}} = state, response_id, _from) do
    case reserve_successor(state.context, response_id) do
      {:ok, context} ->
        admit_reserved_successor(%{state | active: context}, context)

      {:error, reason} ->
        log_successor_failure(state, state.context, :reserve_and_start_turn, reason)
        send(state.socket, {:native_response_steering_close, self(), 1011, ""})
        reply_after_delivery({:error, reason}, revoke_admission(state, :owner_unavailable))
    end
  end

  defp open_successor(state, _response_id, _from) do
    reason = if state.admission_revoked? or state.socket_dead? or state.draining?, do: :owner_unavailable, else: :owner_busy
    log_successor_failure(state, state.context, :lane_acquisition, reason)
    reply_after_delivery({:error, reason}, state)
  end

  defp admit_reserved_successor(state, context) do
    case register_activity(state) do
      {:ok, activity} ->
        open_admitted_successor(%{state | activity: activity}, context)

      {:error, reason} ->
        log_successor_failure(state, context, :activity_admission, reason)
        close_failed_successor(state, context, reason)
    end
  end

  defp open_admitted_successor(state, context) do
    with {:provider_credits_admission, {:ok, receipt}} <- {:provider_credits_admission, ProviderCreditsAdmission.admit(ProviderCreditsAdmission.from_selected(context))},
         {:lifecycle, {:ok, state}} <- {:lifecycle, successor_lifecycle(state)} do
      enroll_successor_delivery(state, context, receipt)
    else
      {:lifecycle, {:error, reason, state}} ->
        close_failed_successor(state, context, reason)

      {phase, {:error, reason}} ->
        log_successor_failure(state, context, phase, reason)
        close_failed_successor(state, context, reason)
    end
  end

  defp enroll_successor_delivery(state, context, receipt) do
    case DeliveryReceiptHandoff.enroll(state.socket, identity(context)) do
      {:ok, token} ->
        state = Map.put(state, :delivery_receipt_token, token)

        case begin_owner_successor(state, context) do
          :ok -> reply_successor_open(state, context, receipt)
          {:error, reason} -> reject_successor_delivery(state, context, :owner_successor, reason)
        end

      {:error, reason} ->
        reject_successor_delivery(state, context, :delivery_receipt, reason)
    end
  end

  defp reject_successor_delivery(state, context, phase, reason) do
    log_successor_failure(state, context, phase, reason)
    close_failed_successor(state, context, reason)
  end

  defp reply_successor_open(state, context, receipt) do
    identity = identity(context)
    observation = %{request_id: identity.request_id, attempt_id: identity.attempt_id, client_request_id: context.request_options.request_metadata.client_request_id, mode: RequestOptions.model_serving_mode(context.request_options)}
    prepared = Map.merge(identity, %{writer: bind_writer(self(), identity, observation), frame_observer: WebsocketRequestCallbacks.frame_observer(context.identity, observation), provider_credits_admission: receipt})
    unless is_pid(state.owner), do: send(state.socket, {:native_response_steering_open, self(), identity})
    {:reply, {:ok, prepared}, state}
  end

  defp close_failed_successor(state, context, reason) do
    state = abandon_delivery_receipt(state)
    state = compensate_open_failure(state, context, reason)
    send(state.socket, {:native_response_steering_close, self(), 1011, ""})
    reply_after_delivery({:error, reason}, state)
  end

  defp relay_successor_frame(%{owner: owner}, identity, data, discriminator) when is_pid(owner), do: WebsocketOwnerSession.relay_steering_frame(owner, self(), identity, data, discriminator)

  defp relay_successor_frame(state, identity, data, _discriminator) do
    send(state.socket, {:native_response_steering_frame, self(), identity, data})
    :ok
  end

  defp bind_writer(lane, identity, observation), do: WebsocketRequestCallbacks.observing_writer(fn data, discriminator -> frame(lane, identity, data, discriminator) end, observation)

  defp close_producer(state, reason) do
    state = state |> revoke_admission(reason) |> fail_active(reason)
    if is_reference(state.upstream_monitor), do: Process.demonitor(state.upstream_monitor, [:flush])
    noreply_after_delivery(%{state | context: nil, callbacks: nil, upstream: nil, upstream_monitor: nil})
  end

  defp finalize(context, finalization, callbacks) do
    finalization = finalization |> Map.put(:started, context.started || System.monotonic_time(:millisecond)) |> Map.put(:callbacks, callbacks)
    if Map.has_key?(finalization, :terminal), do: Finalization.finalize_terminal_websocket_response(context, finalization), else: Finalization.finalize_failed_websocket_response(context, finalization)
  end

  defp fail_active(%{active: nil} = state, _reason), do: complete_activity(state)

  defp fail_active(state, reason) do
    context = state.active
    result = finalize(context, %{reason: reason, headers: [], body: ""}, state.callbacks)

    complete_idle_attribution(state, context)
    finish_delivery(state, identity(context), result)
    {delivery, state} = await_delivery(%{state | active: nil, context: context, original_result: result}, identity(context))
    state |> complete_activity(delivery) |> fence_delivery(delivery)
  end

  defp complete_idle_attribution(%{idle_interruption: {receipt, activity}}, context) do
    if Map.take(receipt, [:request_id, :attempt_id, :replay_generation]) == identity(context) do
      case DirectCleanup.record_settled_downstream_idle_timeout(receipt, activity) do
        {:error, _reason} -> Logger.warning("websocket downstream idle timeout attribution unavailable")
        _recorded_or_stale -> :ok
      end
    end
  rescue
    _error in [DBConnection.ConnectionError, Postgrex.Error] -> Logger.warning("websocket downstream idle timeout attribution unavailable")
  catch
    :exit, _reason -> Logger.warning("websocket downstream idle timeout attribution unavailable")
  end

  defp complete_idle_attribution(_state, _context), do: :ok

  defp finish_delivery(%{owner: owner} = state, identity, result) when is_pid(owner) do
    case owner_call(fn -> WebsocketOwnerSession.complete_steering_successor(owner, self(), identity, result) end) do
      :ok -> :ok
      {:error, _reason} -> send(state.socket, {:native_response_steering_done, self(), identity, result})
    end

    :ok
  end

  defp finish_delivery(state, identity, result) do
    send(state.socket, {:native_response_steering_done, self(), identity, result})
    :ok
  end

  defp await_delivery(state, identity) do
    state = drain_lifecycle(state)

    if state.socket_dead? do
      _result = DeliveryReceiptHandoff.settle_pending()
      {:aborted, state}
    else
      receive_delivery(state, identity)
    end
  end

  defp receive_delivery(state, identity) do
    receive_delivery(state, identity, System.monotonic_time(:millisecond) + 15_000)
  end

  defp receive_delivery(state, identity, deadline) do
    socket_monitor = state.socket_monitor
    socket = state.socket

    receive do
      {:DOWN, ^socket_monitor, :process, _socket, _reason} ->
        _result = DeliveryReceiptHandoff.settle_pending()
        {:aborted, %{state | socket_dead?: true} |> revoke_admission(:client_disconnected)}

      {:"$gen_cast", {:delivery_complete, ^identity, outcome, %{registry: _registry, token: token, context: %{} = context, receipt: %{} = _receipt} = envelope}} when outcome in [:delivered, :aborted] and is_reference(token) ->
        if token == Map.get(state, :delivery_receipt_token) and Map.take(context, [:request_id, :attempt_id, :replay_generation]) == identity do
          case DeliveryReceiptHandoff.settle(envelope) do
            {:error, :stale_delivery} ->
              receive_delivery(state, identity, deadline)

            result ->
              outcome = if result == :ok, do: outcome, else: :aborted
              {outcome, state |> Map.delete(:delivery_receipt_token) |> drain_lifecycle()}
          end
        else
          receive_delivery(state, identity, deadline)
        end

      {:"$gen_cast", {:delivery_complete, ^identity, outcome}} when outcome in [:delivered, :aborted] ->
        {outcome, state |> abandon_delivery_receipt() |> drain_lifecycle()}

      {:"$gen_call", {^socket, _tag} = from, {:prepare, context, callbacks, dispatcher}} when is_pid(dispatcher) ->
        state = prepare_context(state, context, callbacks, dispatcher)
        GenServer.reply(from, :ok)
        receive_delivery(state, identity, deadline)
    after
      max(deadline - System.monotonic_time(:millisecond), 0) ->
        send(state.socket, {:native_response_steering_close, self(), 1011, ""})
        {:aborted, revoke_admission(state, :client_disconnected)}
    end
  end

  defp abandon_delivery_receipt(state) do
    if token = Map.get(state, :delivery_receipt_token), do: DeliveryReceiptHandoff.abandon(token)
    Map.delete(state, :delivery_receipt_token)
  end

  defp begin_owner_successor(%{owner: owner} = state, context) when is_pid(owner), do: owner_call(fn -> WebsocketOwnerSession.begin_steering_successor(owner, self(), state.socket, identity(context), context.reserved.codex_turn.id) end)
  defp begin_owner_successor(_state, _context), do: :ok

  defp owner_call(call) do
    call.()
  catch
    :exit, {:timeout, {GenServer, :call, _args}} -> {:error, :owner_forward_timeout}
    :exit, {reason, {GenServer, :call, _args}} when reason in [:noproc, :normal, :shutdown, :killed] -> {:error, :owner_unavailable}
    :exit, {{:shutdown, _reason}, {GenServer, :call, _args}} -> {:error, :owner_unavailable}
    :exit, {{:nodedown, _node}, {GenServer, :call, _args}} -> {:error, :owner_unavailable}
  end

  defp notify_owner_open_failure(%{owner: owner}, context, result) when is_pid(owner) do
    request = :gen_server.send_request(owner, {:complete_steering_successor, self(), identity(context), result})
    _abandoned_response = :gen_server.receive_response(request, 0)
    :ok
  end

  defp notify_owner_open_failure(_state, _context, _result), do: :ok

  defp register_activity(%{owner: owner}) when is_pid(owner), do: {:ok, nil}

  defp register_activity(_state) do
    case ActivityRegistry.register(:direct) do
      {:ok, token} ->
        case ActivityRegistry.admit(token) do
          :ok ->
            {:ok, token}

          {:error, reason} ->
            ActivityRegistry.complete(token, :aborted)
            {:error, reason}
        end
    end
  end

  defp complete_activity(state), do: complete_activity(state, :aborted)

  defp complete_activity(state, delivery) do
    if is_reference(state.activity), do: ActivityRegistry.complete(state.activity, if(delivery == :delivered, do: :completed, else: :aborted))
    ExecutionIdentity.complete()
    %{state | activity: nil}
  end

  defp reserve_successor(context, response_id) do
    context = %{context | request_options: successor_options(context.request_options, response_id)}
    payload = Map.put(context.payload, "input", [])
    claim = "native-ws-steer:" <> Base.encode16(:crypto.hash(:sha256, context.reserved.request.id <> response_id), case: :lower)

    # The admission coordinator owns discovery before session/owner and key
    # locks. Accounting's nested reservation requires that same transaction.
    ContinuityStore.mailbox_admission_transaction(
      fn ->
        attrs = AccountingReservation.attrs(context.auth, payload, context.endpoint, context.request_options, context.route_state, claim)
        RequestReservation.mailbox_admission_session_ids(context.auth, context.model, attrs)
      end,
      fn ->
        attrs = AccountingReservation.attrs(context.auth, payload, context.endpoint, context.request_options, context.route_state, claim)
        :ok = RequestReservation.revalidate_mailbox_admission_sessions!(context.auth, context.model, attrs)
        context = authorize_context!(context)
        options = context.request_options
        attrs = AccountingReservation.attrs(context.auth, payload, context.endpoint, options, context.route_state, claim)
        attrs = Map.put(attrs, :requested_model, context.reserved.request.requested_model)
        attrs = %{attrs | request_metadata: Map.put(attrs.request_metadata, "routing", context.reserved.request.request_metadata["routing"])}
        attrs = Map.put(attrs, :request_metadata, Map.put(attrs.request_metadata, "native_websocket_response_steering", %{"predecessor_request_id" => context.reserved.request.id, "response_id_fingerprint" => ResponseSteer.fingerprint(response_id)}))
        attrs = Map.put(attrs, :reservation_estimate, AccountingReservation.reservation_estimate(context.route_state))

        with {:ok, reserved} <- Accounting.reserve(context.auth, context.model, payload, attrs),
             {:ok, reserved} <- SessionContinuity.start_turn(reserved, options),
             {:ok, attempt} <- Accounting.create_attempt(reserved.request, context.assignment, %{model: context.model, pricing_snapshot: Map.get(reserved, :pricing_snapshot), upstream_identity: context.identity, response_metadata: Map.merge(context.routing_attempt_metadata, %{"pool_upstream_assignment_id" => context.assignment.id, "upstream_identity_id" => context.identity.id, "native_websocket_response_steering" => true})}) do
          %{context | reserved: reserved, attempt: attempt, request_options: options, payload: payload, allow_retry?: false, retry_count: 0, started: System.monotonic_time(:millisecond), routing_circuit_admission: nil, client_retry_dispatch_authority: nil}
        else
          {:error, reason} -> Repo.rollback(reason)
        end
      end,
      RequestReservation.mailbox_admission_exhausted_error()
    )
  end

  defp log_successor_failure(state, context, phase, reason) do
    request_id = if context, do: context.reserved.request.id
    attempt_id = if context && context.attempt, do: context.attempt.id
    session = if context, do: context.request_options.continuity.codex_session
    session_id = if session, do: session.id
    topology = if is_pid(state.owner), do: :owner, else: :direct

    Logger.warning(
      "native websocket response steer successor acquisition failed " <>
        "operation=open_successor phase=#{phase} outcome=failed topology=#{topology} " <>
        "reason_type=#{successor_reason_type(reason)} reason_code=#{successor_reason_code(reason)} " <>
        "request_id=#{DiagnosticTaxonomy.safe_correlator(request_id)} " <>
        "attempt_id=#{DiagnosticTaxonomy.safe_correlator(attempt_id)} " <>
        "codex_session_id=#{DiagnosticTaxonomy.safe_correlator(session_id)}"
    )
  end

  defp successor_reason_type(%Ecto.Changeset{}), do: :changeset
  defp successor_reason_type(reason) when is_atom(reason), do: :atom
  defp successor_reason_type(reason) when is_map(reason), do: :map
  defp successor_reason_type(reason) when is_tuple(reason), do: :tuple
  defp successor_reason_type(reason) when is_binary(reason), do: :binary
  defp successor_reason_type(_reason), do: :unclassified

  defp successor_reason_code(%Ecto.Changeset{}), do: "changeset_error"
  defp successor_reason_code(%{reason: :provider_credits_policy_denied}), do: "provider_credits_policy_denied"
  defp successor_reason_code(reason), do: DiagnosticTaxonomy.reason_code(reason) || "unclassified_error"

  defp compensate_open_failure(state, context, reason) do
    finalization =
      case reason do
        %{reason: :provider_credits_policy_denied} = denial ->
          code = if "provider_credits_disabled" in denial.reason_codes, do: :provider_credits_disabled, else: :provider_credit_capacity_unverified
          %{reason: code, headers: [], body: ""}

        code when is_atom(code) ->
          %{reason: code, headers: [], body: ""}
      end

    result = finalize(context, finalization, state.callbacks)
    state = %{state | active: nil, original_result: result} |> complete_activity(:aborted) |> revoke_admission(reason)
    notify_owner_open_failure(state, context, result)
    state
  end

  defp successor_options(options, response_id) do
    # Optional continuity setters retain a valid field on a nil update. A
    # provider-created request must instead drop its predecessor's proofs.
    continuity = %{options.continuity | semantic_turn_key: nil, turn_claim_key: nil, request_claim_key: nil, replay_claim_digest: nil}
    options = %{options | continuity: continuity, native_client_retry_witness: nil, native_compaction_admission: nil, native_compaction_reservation: nil, first_compact_collection: nil, extra: Map.drop(options.extra, [:native_turn_progress, :native_turn_position, :socket_last_completed_native_response])}

    options
    |> RequestOptions.put_continuity(previous_response_id: nil, response_id: response_id, upstream_previous_response_id?: false)
    |> RequestOptions.put_runtime_context(direct_cleanup: nil, owner_cleanup: nil, gateway_debug_payload: nil, reason_held_request_id: nil, replay_authorization_binding: nil, replay_lifecycle_binding: nil, replay_generation: nil, native_replay_binding: nil, native_replay_proof: nil, replay_provisional_token: nil, compaction_retry_submit_hold: nil)
  end

  defp authorize(nil), do: {:error, :owner_unavailable}

  defp authorize(context), do: Repo.transaction(fn -> authorize_context!(context) end)

  defp authorize_context!(context) do
    options = lock_session_before_authorization(context.request_options)

    case Access.authorize_api_key_runtime_turn(context.auth.api_key, options.runtime.api_key_runtime_epoch) do
      {:ok, %{api_key: api_key}} -> %{context | auth: Map.put(context.auth, :api_key, api_key), request_options: options}
      {:error, reason} -> Repo.rollback(reason)
    end
  end

  defp put_acknowledgement({:ok, result}, {:ok, _binding, _receipt} = acknowledgement), do: {:ok, Map.put(result, :native_response_steering_acknowledgement, acknowledgement)}
  defp put_acknowledgement(result, :none), do: result

  # A failed producer must retire while the lane still owns the socket's delivery wait.
  # Accounting failures and provider terminals keep their existing acknowledgement fence.
  defp reply_settled_failed_producer(from, finalization, {:error, %{code: code}} = result, :none)
       when code != "gateway_accounting_failed" and not is_map_key(finalization, :terminal) do
    GenServer.reply(from, {:ok, result, :none})
    true
  end

  defp reply_settled_failed_producer(_from, _finalization, _result, _acknowledgement), do: false

  defp retain_settlement(state, {:ok, %{stale_generation?: true}}, _finalization), do: revoke_admission(state, :stale_generation)
  defp retain_settlement(state, {:ok, %{}}, %{terminal: terminal}) when terminal in ["response.completed", "response.done", "response.incomplete"], do: state
  defp retain_settlement(state, _result, _finalization), do: revoke_admission(state, :upstream_websocket_closed_before_terminal)

  defp fence_delivery(state, :delivered), do: drain_lifecycle(state)
  defp fence_delivery(state, :aborted), do: state |> drain_lifecycle() |> revoke_admission(:client_disconnected)

  defp revoke_admission(state, reason) do
    # A settled ordinary response only retains authority for a possible late
    # steer; its producing connection still belongs to the reusable owner.
    context = state.active || state.context

    if is_pid(state.upstream) and context do
      if state.active || state.activated?,
        do: send(state.upstream, {:upstream_websocket_cancel_steering, self(), request_identity(context), reason}),
        else: send(state.upstream, {:upstream_websocket_release_steering, self(), request_identity(context)})
    end

    %{state | activated?: false, admission_revoked?: true, draining?: state.draining? or reason == :owner_drained}
  end

  defp drain_lifecycle(state) do
    socket_monitor = state.socket_monitor

    receive do
      {:DOWN, ^socket_monitor, :process, _socket, _reason} ->
        %{state | socket_dead?: true} |> revoke_admission(:client_disconnected) |> drain_lifecycle()

      {:"$gen_cast", {:cancel, reason}} ->
        state |> revoke_admission(reason) |> drain_lifecycle()

      {:websocket_activity_cancel, _token, :owner_drained} ->
        state |> revoke_admission(:owner_drained) |> drain_lifecycle()
    after
      0 -> state
    end
  end

  defp successor_lifecycle(state) do
    state = drain_lifecycle(state)
    if state.admission_revoked? or state.socket_dead? or state.draining?, do: {:error, :owner_unavailable, state}, else: {:ok, state}
  end

  defp reply_after_delivery(reply, %{socket_dead?: true} = state) do
    reply_pending_terminals(state)
    {:stop, :normal, without_acknowledgement(reply), state}
  end

  defp reply_after_delivery(reply, %{admission_revoked?: true} = state), do: {:reply, without_acknowledgement(reply), state}
  defp reply_after_delivery(reply, state), do: {:reply, reply, state}
  defp without_acknowledgement({:ok, {:ok, result}, _ack}), do: {:ok, {:ok, Map.delete(result, :native_response_steering_acknowledgement)}, :none}
  defp without_acknowledgement(reply), do: reply

  defp noreply_after_delivery(%{socket_dead?: true} = state) do
    reply_pending_terminals(state)
    {:stop, :normal, state}
  end

  defp noreply_after_delivery(state), do: {:noreply, state}

  defp reply_pending_terminals(state) do
    request = if state.context, do: state.context.reserved.request.id

    receive do
      {:"$gen_call", from, {:terminal, ^request, _finalization}} when not is_nil(state.original_result) ->
        GenServer.reply(from, {:ok, state.original_result, :none})
        reply_pending_terminals(state)
    after
      0 -> :ok
    end
  end

  defp lane_call(call) do
    call.()
  catch
    :exit, {reason, {GenServer, :call, _args}} when reason in [:noproc, :normal, :shutdown, :killed] -> {:error, :owner_unavailable}
    :exit, {{:shutdown, _reason}, {GenServer, :call, _args}} -> {:error, :owner_unavailable}
    :exit, {{:nodedown, _node}, {GenServer, :call, _args}} -> {:error, :owner_unavailable}
  end

  defp prepare_context(state, context, callbacks, dispatcher) do
    key = request_identity(context)
    state = release_prepared(state, key)
    entry = %{context: context, callbacks: callbacks, result: nil, dispatcher_monitor: Process.monitor(dispatcher)}
    %{state | prepared: Map.put(state.prepared, key, entry)}
  end

  defp release_prepared(state, key) do
    case Map.pop(state.prepared, key) do
      {nil, _prepared} ->
        state

      {entry, prepared} ->
        Process.demonitor(entry.dispatcher_monitor, [:flush])
        %{state | prepared: prepared}
    end
  end

  defp request_identity(context), do: {context.reserved.request.id, context.attempt.id}

  # Session/owner locks precede the reservation advisory mutex and key reader.
  defp lock_session_before_authorization(options) do
    :ok = ContinuityStore.validate_session_owner_witness_for_reservation(options)

    if is_nil(options.runtime.session_owner_witness) and is_map(options.continuity.codex_session) do
      session = ContinuityStore.lock_codex_session_for_turn(options.continuity.codex_session)
      RequestOptions.put_continuity(options, codex_session: session)
    else
      options
    end
  end

  defp identity(context), do: %{request_id: context.reserved.request.id, attempt_id: context.attempt.id, replay_generation: context.attempt.replay_generation || 0}

  defp trim_context(context) do
    route_state = %{context.route_state | visible_model_context: %{}, visible_models: []}

    %{
      context
      | payload: Map.take(context.payload, ["model", "reasoning", "max_output_tokens", "service_tier"]),
        request_options: RequestOptions.put_runtime_context(context.request_options, gateway_debug_payload: nil),
        route_state: route_state
    }
  end

  defp native_relay?(options), do: options.transport.transport == "websocket" and options.transport.websocket_delivery_mode == :relay and is_nil(options.openai_compatibility.source_endpoint) and not options.openai_compatibility.public_openai_responses_stream
  # The socket captures this response-task context. An owner's downstream
  # identifies transport delivery, not authority to handle socket callbacks.
  @spec socket_pid(RequestOptions.t()) :: pid() | nil
  defp socket_pid(%RequestOptions{runtime: %{direct_cleanup: %DirectCleanup{parent: parent}}}) when is_pid(parent), do: parent
  defp socket_pid(_options), do: nil
end
