defmodule CodexPoolerWeb.Runtime.BackendCodexWebsocketSteeringCompletionTest do
  # Synthetic adversarial lifecycle coverage on a real Pooler websocket. The
  # provider accepts steering, completes the original and starts a separately
  # accounted successor; only the successor is then cut, revoked or refused.
  use CodexPoolerWeb.ConnCase, async: false

  import Ecto.Query
  import CodexPoolerWeb.Runtime.BackendCodexTestSupport

  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport,
    only: [await_socket_connection_state!: 2, model_serving_scope: 0, released_client_frame: 2, set_model_serving_mode!: 3, with_info_log: 1]

  alias CodexPooler.Access
  alias CodexPooler.Accounting.{Attempt, LedgerEntry, Request}
  alias CodexPooler.Accounts.{Scope, User}
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway.OperationalSettings
  alias CodexPooler.InstanceSettings
  alias CodexPooler.Repo
  alias CodexPoolerWeb.Runtime.WebsocketCleanupFence
  alias Ecto.Adapters.SQL.Sandbox

  @moduletag capture_log: true
  # A detection budget, not a scenario delay: successful drains finish on frames,
  # actual finalization/receipt rows and connection cleanup signals.
  @detection_timeout_ms 15_000
  @receive_timeout_ms 2_000
  @turn_path "/backend-api/codex/responses"
  @steer_text "synthetic steering completion input sentinel"
  @refusal_message "The experimental native turn lane cannot accept stateful WebSocket messages while a native turn is running. Start a new independent response.create turn instead."
  @original_usage %{"input_tokens" => 31, "output_tokens" => 13, "total_tokens" => 44}
  @successor_usage %{"input_tokens" => 5, "output_tokens" => 7, "total_tokens" => 12}

  for forwarding <- [:off, :on] do
    @tag forwarding: forwarding
    test "owner forwarding #{forwarding}: a successor TCP cut closes its waiting client without changing the original", ctx do
      assert_transport_failure!(ctx.forwarding, :tcp_close)
    end

    @tag forwarding: forwarding
    @tag slow: "exercises the configured two-second upstream receive timeout after a confirmed successor frame"
    test "owner forwarding #{forwarding}: a successor receive timeout closes its waiting client without changing the original", ctx do
      assert_transport_failure!(ctx.forwarding, :receive_timeout)
    end

    @tag forwarding: forwarding
    test "owner forwarding #{forwarding}: a successor preserves the raw native-lane refusal and the provider Close", ctx do
      assert_provider_refusal!(ctx.forwarding)
    end

    for revocation <- [:pause, :revoke, :firewall] do
      @tag forwarding: forwarding, revocation: revocation
      test "owner forwarding #{forwarding}: #{revocation} closes 1008 immediately after the successor drains", ctx do
        assert_revoked_drain!(ctx.forwarding, ctx.revocation)
      end
    end
  end

  test "successor delivery logs remain captured when publication follows settled request visibility" do
    # The paused receipt writer owns a checkout. Separate connections let this
    # test observe settlement without waiting on that writer's connection.
    on_exit(fn -> Sandbox.mode(Repo, :manual) end)
    :ok = Sandbox.mode(Repo, :auto)
    assert_transport_failure!(:off, :tcp_close, hold_delivery_log?: true)
  end

  defp assert_transport_failure!(forwarding, failure, opts \\ []) do
    put_owner_forwarding!(forwarding)
    previous_settings = if failure == :receive_timeout, do: CodexPooler.TestAppEnv.restore_on_exit(OperationalSettings)
    fixture = start_successor!(successor_frames: :partial, committed?: opts[:hold_delivery_log?])
    if failure == :receive_timeout, do: set_receive_timeout!(previous_settings)

    {{client, [original, successor]}, log} =
      with_info_log(fn ->
        receive_timeout_ms = if failure == :receive_timeout, do: @receive_timeout_ms
        {client, handler} = open_successor!(fixture, 4, receive_timeout_ms)
        [original, successor] = assert_active_successor!(fixture, client)
        assert_successful_response!(fixture.setup, original, @original_usage)
        if opts[:hold_delivery_log?], do: hold_delivery_log_until_receipt_read!(successor.id)
        handler_monitor = Process.monitor(handler)
        assert :ok = FakeUpstream.release_frame(fixture.upstream, fixture.hold)

        if failure == :tcp_close do
          # The existing fake's abrupt-close callback kills the owning transport
          # without writing a WebSocket Close. It must be unparked first.
          send(handler, :fake_upstream_abrupt_close_websocket)
          assert_receive {:DOWN, ^handler_monitor, :process, ^handler, :killed}, @detection_timeout_ms
        end

        {client, frames, code, reason} = receive_until_close!(client)
        assert frames == []
        assert {code, reason} == {1001, "upstream connection closed"}
        drop!(client)

        if failure == :receive_timeout do
          assert_receive {:DOWN, ^handler_monitor, :process, ^handler, _reason}, @detection_timeout_ms
        end

        [settled_original, settled_successor] = await_settled_rows!(fixture.setup)
        assert settled_original.id == original.id
        assert settled_successor.id == successor.id
        {client, [settled_original, settled_successor]}
      end)

    assert_successful_response!(fixture.setup, original, @original_usage)
    expected_code = if failure == :receive_timeout, do: "stream_idle_timeout", else: "upstream_stream_error"
    assert_unknown_failure!(fixture.setup, successor, expected_code, 502)
    assert %{"outcome" => "aborted", "terminal_class" => "none", "pushed_at" => nil, "frames_after_visible" => 2, "highest_frame_class" => "delta"} = await_delivery!(successor)
    if opts[:hold_delivery_log?], do: assert_receive({:delivery_log_released, request_id} when request_id == successor.id, @detection_timeout_ms)
    assert_single_delivery_log!(log, original, "delivered", "response.completed")
    assert_single_delivery_log!(log, successor, "aborted", "none")
    assert_independent_responses!(fixture, original, successor)
    assert client.frames == []
    assert :ok = FakeUpstream.verify!(fixture.upstream)
  end

  defp assert_revoked_drain!(forwarding, revocation) do
    put_owner_forwarding!(forwarding)
    fixture = start_successor!(successor_frames: :completed)

    {{client, [original, successor]}, log} =
      with_info_log(fn ->
        {client, handler} = open_successor!(fixture, 4)
        [original, successor] = assert_active_successor!(fixture, client)
        assert_successful_response!(fixture.setup, original, @original_usage)
        state = await_socket_connection_state!(client.socket, &(MapSet.size(&1.tasks) == 0))
        identity = state.native_response_steering_active.identity
        assert identity.request_id == successor.id
        assert [attempt] = request_attempts(successor)
        assert identity.attempt_id == attempt.id
        assert identity.replay_generation == (attempt.replay_generation || 0)
        assert state.api_key_runtime_epoch == fixture.setup.api_key.runtime_revocation_epoch
        cancel_authorization_timer!(state)

        client = send_frame!(client, queued_continuation(fixture))
        queued = await_socket_connection_state!(client.socket, &(:queue.len(&1.queued_response_payloads) == 1))
        assert queued.native_response_steering_active.identity == identity
        revoke!(fixture.setup, revocation)
        revoked_key = if revocation == :firewall, do: :firewall_revoked?, else: :api_key_revoked?
        revoked = await_socket_connection_state!(client.socket, &Map.get(&1, revoked_key, false))
        if revocation != :firewall, do: assert(revoked.api_key_disabling_epoch == fixture.setup.api_key.runtime_revocation_epoch + 1)
        assert revoked.api_key_expiry_check.timer == state.api_key_expiry_check.timer
        assert Process.read_timer(revoked.api_key_expiry_check.timer) == false
        assert revoked.native_response_steering_active.identity == identity
        assert MapSet.size(revoked.tasks) == 0
        assert :queue.is_empty(revoked.queued_response_payloads)
        refute Map.get(revoked, :socket_stopped?, false)
        refute Map.get(revoked, :api_key_close_sent?, false)
        refute Map.get(revoked, :firewall_close_sent?, false)

        # No more client frame or authorization timer can cause the close. The
        # held provider terminal is the only work left to drain.
        assert :ok = FakeUpstream.release_frame(fixture.upstream, fixture.hold)
        assert_receive {:fake_upstream_frame_barrier, 5, ^handler, hold}, @detection_timeout_ms
        assert hold == fixture.hold
        assert :ok = FakeUpstream.release_frame(fixture.upstream, fixture.hold)
        {client, frames, code, reason} = receive_until_close!(client)
        assert_frames!(frames, [completed(fixture.successor_id, @successor_usage)])
        expected_reason = if revocation == :firewall, do: "client IP is no longer allowed", else: "api key is no longer active"
        assert {code, reason} == {1008, expected_reason}
        drop!(client)
        rows = await_settled_rows!(fixture.setup)
        assert Enum.map(rows, & &1.id) == [original.id, successor.id]
        {client, rows}
      end)

    assert_successful_response!(fixture.setup, original, @original_usage)
    assert_successful_response!(fixture.setup, successor, @successor_usage)
    assert %{"outcome" => "delivered", "terminal_class" => "response.completed", "frames_after_visible" => 3} = await_delivery!(successor)
    assert_single_delivery_log!(log, original, "delivered", "response.completed")
    assert_single_delivery_log!(log, successor, "delivered", "response.completed")
    if revocation == :firewall, do: assert(log =~ "ingress firewall denied")
    assert_independent_responses!(fixture, original, successor)
    assert client.frames == []
    assert :ok = FakeUpstream.verify!(fixture.upstream)
  end

  defp assert_provider_refusal!(forwarding) do
    put_owner_forwarding!(forwarding)
    fixture = start_successor!(successor_frames: :refused)

    {[original, successor], log} =
      with_info_log(fn ->
        {client, handler} = open_successor!(fixture, 3)
        [original, successor] = assert_active_successor!(fixture, client)
        assert_successful_response!(fixture.setup, original, @original_usage)
        assert :ok = FakeUpstream.release_frame(fixture.upstream, fixture.hold)
        assert_receive {:fake_upstream_frame_barrier, 4, ^handler, hold}, @detection_timeout_ms
        assert hold == fixture.hold
        {client, [refusal]} = receive_n!(client, 1)
        assert_frames!([refusal], [native_lane_refusal()])
        state = await_socket_connection_state!(client.socket, &(get_in(&1, [:native_response_steering_active, :evidence, :terminal_class]) == "error"))
        assert state.native_response_steering_active.identity.request_id == successor.id
        close_ref = make_ref()
        assert :ok = FakeUpstream.close_websocket_connection(fixture.upstream, 1, close_ref: close_ref, notify: self(), code: 1000, reason: "")
        assert :ok = FakeUpstream.release_frame(fixture.upstream, fixture.hold)
        assert_receive {:fake_upstream_websocket_peer_closed, 1, ^close_ref}, @detection_timeout_ms
        {client, trailing, code, reason} = receive_until_close!(client)
        assert trailing == []
        assert {code, reason} == {1000, ""}
        drop!(client)
        await_settled_rows!(fixture.setup)
      end)

    assert_successful_response!(fixture.setup, original, @original_usage)
    assert_unknown_failure!(fixture.setup, successor, "unsupported_native_inflight_message", 400)
    assert %{"outcome" => "delivered", "terminal_class" => "error", "frames_after_visible" => 2, "highest_frame_class" => "terminal", "pushed_at" => pushed_at} = await_delivery!(successor)
    assert {:ok, _written_at, 0} = DateTime.from_iso8601(pushed_at)
    assert_single_delivery_log!(log, original, "delivered", "response.completed")
    assert_single_delivery_log!(log, successor, "delivered", "error")
    assert_independent_responses!(fixture, original, successor)
    assert :ok = FakeUpstream.verify!(fixture.upstream)
  end

  defp start_successor!(opts) do
    suffix = System.unique_integer([:positive])
    original_id = "resp_synthetic_completion_original_#{suffix}"
    successor_id = "resp_synthetic_completion_successor_#{suffix}"
    hold = make_ref()
    successor = [created(successor_id)]

    successor =
      case Keyword.fetch!(opts, :successor_frames) do
        :partial -> successor ++ [delta(successor_id)]
        :completed -> successor ++ [delta(successor_id), completed(successor_id, @successor_usage)]
        :refused -> successor ++ [native_lane_refusal()]
      end

    response = FakeUpstream.websocket_steerable([created(original_id)], notify: self(), ref: hold, response_id: original_id, terminal_frames: [completed(original_id, @original_usage)], successor_frames: successor, batches: :separate)
    expected = FakeUpstream.expect_request(method: "WEBSOCKET", path: @turn_path, websocket_connection_ordinal: 1, json: [valid: true, equals: %{"type" => "response.create"}], respond: response)
    upstream = start_upstream(FakeUpstream.strict_sequence([expected]))
    setup = successor_gateway_setup!(upstream, opts[:committed?])
    set_model_serving_mode!(model_serving_scope(), setup, "full")
    {_server, port} = start_public_endpoint_with_server!()
    %{upstream: upstream, setup: setup, port: port, hold: hold, original_id: original_id, successor_id: successor_id, successor_frames: successor, thread: Ecto.UUID.generate(), turn_id: Ecto.UUID.generate()}
  end

  defp successor_gateway_setup!(upstream, true) do
    # Partial fixture setup rolls back; cleanup is registered before any of
    # the complete graph commits, including an interrupted commit response.
    {:ok, setup} =
      Repo.transaction(fn ->
        setup = gateway_setup(upstream)
        register_unboxed_pool_cleanup!(setup)
        setup
      end)

    setup
  end

  defp successor_gateway_setup!(upstream, _boxed), do: gateway_setup(upstream)

  defp open_successor!(fixture, prefix_count, receive_timeout_ms \\ nil) do
    client = connect!(fixture)

    if receive_timeout_ms do
      state = await_socket_connection_state!(client.socket, &Map.has_key?(&1, :opts))
      assert state.opts.timeout_config.receive_timeout_ms == receive_timeout_ms
    end

    opener = released_client_frame(fixture.setup, fixture.thread).(native_text_input("synthetic steering completion opener"), fixture.turn_id, %{"instructions" => "synthetic full instructions", "tools" => [], "store" => false})
    client = send_frame!(client, opener)
    hold = fixture.hold
    assert_receive {:fake_upstream_steerable_open, handler, ^hold}, @detection_timeout_ms
    {client, [original_created]} = receive_n!(client, 1)
    assert_frames!([original_created], [created(fixture.original_id)])
    steer = CodexPooler.JSON.encode!(%{"type" => "response.steer", "previous_response_id" => fixture.original_id, "input" => native_text_input(@steer_text)})
    client = send_frame!(client, steer)
    expected = [accepted(fixture.original_id), completed(fixture.original_id, @original_usage)] ++ fixture.successor_frames

    {client, frames} =
      expected
      |> Enum.take(prefix_count)
      |> Enum.with_index()
      |> Enum.reduce({client, []}, fn {expected_frame, ordinal}, {client, frames} ->
        assert_receive {:fake_upstream_frame_barrier, ^ordinal, ^handler, ^hold}, @detection_timeout_ms
        assert :ok = FakeUpstream.release_frame(fixture.upstream, hold)
        {client, [frame]} = receive_n!(client, 1)
        assert_frames!([frame], [expected_frame])
        {client, [frame | frames]}
      end)

    assert_frames!(Enum.reverse(frames), Enum.take(expected, prefix_count))
    assert_receive {:fake_upstream_frame_barrier, ^prefix_count, ^handler, ^hold}, @detection_timeout_ms
    assert_receive {:fake_upstream_steered, ^handler, ^hold}, @detection_timeout_ms
    {client, handler}
  end

  defp assert_active_successor!(fixture, client) do
    rows =
      await_rows!(fixture.setup, fn
        [original, successor] -> original.status == "succeeded" and successor.status == "in_progress"
        _other -> false
      end)

    [original, successor] = rows
    state = await_socket_connection_state!(client.socket, &(get_in(&1, [:native_response_steering_active, :identity, :request_id]) == successor.id))
    assert state.native_response_steering_active.identity.request_id != original.id
    assert is_nil(successor.completed_at)
    assert [attempt] = request_attempts(successor)
    assert is_nil(attempt.completed_at)
    assert [%LedgerEntry{entry_kind: "reservation"}] = request_ledger(successor)
    rows
  end

  defp assert_successful_response!(setup, row, usage) do
    assert {row.status, row.usage_status, row.response_status_code, row.last_error_code} == {"succeeded", "usage_known", 200, nil}
    assert row.completed_at != nil
    assert [attempt] = request_attempts(row)
    assert {attempt.status, attempt.usage_status, attempt.network_error_code} == {"succeeded", "usage_known", nil}
    assert_attempt_identity!(setup, row, attempt)
    {_reservation, settlement} = assert_ledger!(row, attempt)
    assert settlement.usage_status == "usage_known"
    assert Map.take(settlement, [:input_tokens, :output_tokens, :total_tokens]) == %{input_tokens: usage["input_tokens"], output_tokens: usage["output_tokens"], total_tokens: usage["total_tokens"]}
    assert %{"outcome" => "delivered", "terminal_class" => "response.completed", "pushed_at" => pushed_at} = await_delivery!(row)
    assert {:ok, _written_at, 0} = DateTime.from_iso8601(pushed_at)
  end

  defp assert_unknown_failure!(setup, row, code, status) do
    assert {row.status, row.usage_status, row.response_status_code, row.last_error_code} == {"failed", "usage_unknown", status, code}
    assert row.completed_at != nil
    assert row.retry_count == 0
    assert [attempt] = request_attempts(row)
    assert {attempt.status, attempt.usage_status, attempt.network_error_code} == {"failed", "usage_unknown", code}
    assert_attempt_identity!(setup, row, attempt)
    {reservation, settlement} = assert_ledger!(row, attempt)
    assert settlement.usage_status == "usage_unknown"
    assert settlement.total_tokens == reservation.total_tokens
    assert settlement.details["estimated_from_reserve"] == true
  end

  defp assert_attempt_identity!(setup, row, attempt) do
    assert row.transport == "websocket"
    assert row.endpoint == @turn_path
    assert attempt.transport == "websocket"
    assert attempt.attempt_number == 1
    assert attempt.pool_upstream_assignment_id == setup.assignment.id
    assert attempt.upstream_identity_id == setup.identity.id
    assert attempt.completed_at != nil
  end

  defp assert_ledger!(row, attempt) do
    entries = request_ledger(row)
    assert Enum.frequencies_by(entries, & &1.entry_kind) == %{"reservation" => 1, "release" => 1, "settlement" => 1}
    assert Enum.all?(entries, &(&1.amount_status == "recorded"))
    assert [reservation] = Enum.filter(entries, &(&1.entry_kind == "reservation"))
    assert [release] = Enum.filter(entries, &(&1.entry_kind == "release"))
    assert [settlement] = Enum.filter(entries, &(&1.entry_kind == "settlement"))
    assert reservation.total_tokens > 0
    assert release.total_tokens == reservation.total_tokens
    assert release.attempt_id == attempt.id
    assert settlement.attempt_id == attempt.id
    assert release.details["reservation_source_event_id"] == reservation.source_event_id
    assert release.details["released_by_source_event_id"] == settlement.source_event_id
    {reservation, settlement}
  end

  defp assert_independent_responses!(fixture, original, successor) do
    assert original.id != successor.id
    assert original.correlation_id != successor.correlation_id
    assert "codex-turn:" <> _original_claim = original.correlation_id
    assert "native-ws-steer:" <> _successor_claim = successor.correlation_id
    assert successor.request_metadata["native_websocket_response_steering"]["predecessor_request_id"] == original.id
    assert [original_attempt] = request_attempts(original)
    assert [successor_attempt] = request_attempts(successor)
    assert original_attempt.id != successor_attempt.id
    metadata = inspect({original.request_metadata, successor.request_metadata, original_attempt.response_metadata, successor_attempt.response_metadata, Enum.map(request_ledger(original) ++ request_ledger(successor), & &1.details)})
    for sentinel <- [fixture.original_id, fixture.successor_id, @steer_text, @refusal_message], do: refute(metadata =~ sentinel)
    assert [%{websocket_connection_id: 1, json: %{"type" => "response.create"}}] = FakeUpstream.requests(fixture.upstream)
    assert [%{websocket_connection_id: 1, json: %{"type" => "response.steer"}}] = FakeUpstream.websocket_steers(fixture.upstream)
    assert FakeUpstream.physical_counts(fixture.upstream).websocket_generation == 1
    assert FakeUpstream.physical_counts(fixture.upstream).http_generation == 0
  end

  defp assert_single_delivery_log!(log, row, outcome, terminal) do
    lines = log |> String.split("\n", trim: true) |> Enum.filter(&String.contains?(&1, "downstream terminal pushed request_id=#{row.id} "))
    assert [line] = lines
    assert line =~ "outcome=#{outcome} terminal_class=#{terminal} "
  end

  defp hold_delivery_log_until_receipt_read!(request_id) do
    # Before the fix this read happened only after with_info_log returned,
    # placing the real emission outside capture deterministically. Observing
    # the durable receipt inside capture releases and captures the same log.
    owner = self()
    ref = make_ref()
    filter_id = :steering_completion_delivery_log_hold
    handler_id = {__MODULE__, ref}
    dumped_id = Ecto.UUID.dump!(request_id)

    on_exit(fn ->
      :logger.remove_primary_filter(filter_id)
      :telemetry.detach(handler_id)
    end)

    :ok =
      :logger.add_primary_filter(
        filter_id,
        {fn
           %{msg: {:string, chardata}} = event, _config ->
             if String.contains?(IO.chardata_to_string(chardata), "downstream terminal pushed request_id=#{request_id} ") do
               monitor = Process.monitor(owner)
               send(owner, {:delivery_log_held, ref, self()})

               receive do
                 {:release_delivery_log, ^ref} -> :ok
                 {:DOWN, ^monitor, :process, ^owner, _reason} -> :ok
               after
                 @detection_timeout_ms -> raise "delivery log hold was not released by its receipt read"
               end

               Process.demonitor(monitor, [:flush])
             end

             event

           event, _config ->
             event
         end, nil}
      )

    :ok = :telemetry.attach(handler_id, [:codex_pooler, :repo, :query], &__MODULE__.release_delivery_log_on_read/4, %{owner: owner, ref: ref, request_id: request_id, dumped_id: dumped_id})
  end

  @doc false
  def release_delivery_log_on_read(_event, _measurements, metadata, %{owner: owner, ref: ref, request_id: request_id, dumped_id: dumped_id}) do
    if self() == owner and String.contains?(metadata.query, ~s(FROM "attempts")) and dumped_id in (metadata.params || []) do
      receive do
        {:delivery_log_held, ^ref, emitter} ->
          send(emitter, {:release_delivery_log, ref})
          send(owner, {:delivery_log_released, request_id})
      after
        0 -> :ok
      end
    end
  end

  defp queued_continuation(fixture) do
    released_client_frame(fixture.setup, fixture.thread).([%{"type" => "function_call_output", "call_id" => "call_synthetic_completion_queued", "output" => "synthetic queued tool output"}], Ecto.UUID.generate(), %{"previous_response_id" => fixture.successor_id})
  end

  defp revoke!(_setup, :firewall) do
    assert {:ok, _settings} = InstanceSettings.update_system_settings(InstanceSettings.current(), %{"ingress" => %{"firewall_allowlist" => ["203.0.113.10/32"]}})
  end

  defp revoke!(setup, operation) when operation in [:pause, :revoke] do
    scope = setup.api_key.created_by_user_id |> then(&Repo.get!(User, &1)) |> Scope.for_user(["instance_owner"])
    result = if operation == :pause, do: Access.pause_api_key(scope, setup.api_key), else: Access.revoke_api_key(scope, setup.api_key)
    assert {:ok, disabled} = result
    assert disabled.runtime_revocation_epoch == setup.api_key.runtime_revocation_epoch + 1
  end

  defp cancel_authorization_timer!(state) do
    # Real key events perform the revocation. Cancelling this unrelated safety
    # reread rules out a later timer accidentally making a broken drain pass.
    assert remaining_ms = Process.cancel_timer(state.api_key_expiry_check.timer)
    assert is_integer(remaining_ms) and remaining_ms > @detection_timeout_ms
    assert is_nil(Map.get(state, :api_key_reread_retry))
  end

  defp set_receive_timeout!(previous) do
    # OperationalSettings uses its test override unless this existing seam is
    # enabled; a DB/cache update alone does not select the request's timeout.
    Application.put_env(:codex_pooler, OperationalSettings, Keyword.put(previous, :use_instance_settings?, true))
    assert {:ok, settings} = InstanceSettings.update_system_settings(InstanceSettings.current(), %{"gateway" => %{"upstream_receive_timeout_ms" => @receive_timeout_ms}})
    assert settings.gateway.upstream_receive_timeout_ms == @receive_timeout_ms
    assert OperationalSettings.current().upstream_receive_timeout_ms == @receive_timeout_ms
  end

  defp put_owner_forwarding!(forwarding) do
    CodexPooler.TestAppEnv.restore_on_exit(:websocket_owner_forwarding_enabled)
    Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, forwarding == :on)
  end

  defp connect!(fixture) do
    before = WebsocketCleanupFence.listener_sockets()
    headers = [{"session-id", fixture.thread}, {"thread-id", fixture.thread}, {"x-codex-window-id", "#{fixture.thread}:0"}, {"originator", "codex_cli_rs"}]
    {conn, websocket, ref, _headers} = public_websocket_connect_with_request_headers!(fixture.port, fixture.setup, fixture.thread, @turn_path, headers)
    socket = WebsocketCleanupFence.await_new_listener_socket!(before)
    %{conn: conn, websocket: websocket, ref: ref, socket: socket, frames: []}
  end

  defp drop!(client) do
    Mint.HTTP.close(client.conn)
    :ok = WebsocketCleanupFence.await_listener_socket_cleanup!(client.socket)
  end

  defp send_frame!(client, frame) do
    {conn, websocket} = public_websocket_send_text!(client.conn, client.websocket, client.ref, frame)
    %{client | conn: conn, websocket: websocket}
  end

  defp receive_n!(client, count), do: receive_n!(client, count, [])
  defp receive_n!(client, 0, frames), do: {client, Enum.reverse(frames)}

  defp receive_n!(client, count, frames) do
    {client, frame} = receive_frame!(client)

    case frame do
      {:text, raw} -> receive_n!(client, count - 1, [raw | frames])
      {:close, code, _reason} -> flunk("native websocket closed #{code} before the expected successor frame")
    end
  end

  defp receive_until_close!(client, frames \\ []) do
    {client, frame} = receive_frame!(client)

    case frame do
      {:close, code, reason} -> {client, Enum.reverse(frames), code, reason}
      {:text, raw} -> receive_until_close!(client, [raw | frames])
    end
  end

  # Retain every trailing frame and Close instead of dropping a coalesced Close
  # while reading a terminal. Only this Mint connection's messages are consumed.
  defp receive_frame!(%{frames: [frame | rest]} = client), do: {%{client | frames: rest}, frame}

  defp receive_frame!(client) do
    message = receive_mint_socket_message!(client.conn, @detection_timeout_ms, "timed out waiting for successor completion or Close")

    case Mint.WebSocket.stream(client.conn, message) do
      {:ok, conn, responses} ->
        {websocket, frames} = Enum.reduce(responses, {client.websocket, []}, &decode_response(&1, &2, client.ref))
        receive_frame!(%{client | conn: conn, websocket: websocket, frames: frames})

      {:error, _conn, reason, _responses} ->
        flunk("native successor websocket receive failed: #{inspect(reason)}")

      :unknown ->
        receive_frame!(client)
    end
  end

  defp decode_response({:data, ref, data}, {websocket, frames}, ref) do
    assert {:ok, websocket, decoded} = Mint.WebSocket.decode(websocket, data)

    visible =
      Enum.filter(decoded, fn
        {:text, raw} -> not String.starts_with?(event_type(raw) || "", "codex.")
        {:close, _code, _reason} -> true
        _control -> false
      end)

    {websocket, frames ++ visible}
  end

  defp decode_response(_response, acc, _ref), do: acc

  defp assert_frames!(received, expected) do
    assert Enum.map(received, &event_type/1) == Enum.map(expected, &event_type/1)
    assert Enum.map(received, &:crypto.hash(:sha256, &1)) == Enum.map(expected, &:crypto.hash(:sha256, &1))
  end

  defp event_type(raw), do: CodexPooler.JSON.decode!(raw)["type"]
  defp created(id), do: CodexPooler.JSON.encode!(%{"type" => "response.created", "response" => %{"id" => id, "status" => "in_progress", "output" => []}})
  defp completed(id, usage), do: CodexPooler.JSON.encode!(%{"type" => "response.completed", "response" => %{"id" => id, "status" => "completed", "output" => [], "usage" => usage}})
  defp delta(id), do: CodexPooler.JSON.encode!(%{"type" => "response.output_text.delta", "item_id" => "msg_#{id}", "output_index" => 0, "content_index" => 0, "delta" => "synthetic successor partial output"})
  defp accepted(id), do: CodexPooler.JSON.encode!(%{"type" => "response.steer.accepted", "sequence_number" => 1, "steer" => %{"id" => "steer_synthetic_342", "previous_response_id" => id}})
  defp native_lane_refusal, do: CodexPooler.JSON.encode!(%{"type" => "error", "status" => 400, "error" => %{"type" => "invalid_request_error", "code" => "unsupported_native_inflight_message", "message" => @refusal_message}})
  defp pool_requests(setup), do: Repo.all(from(row in Request, where: row.pool_id == ^setup.pool.id, order_by: [asc: row.admitted_at]))
  defp request_attempts(row), do: Repo.all(from(attempt in Attempt, where: attempt.request_id == ^row.id))
  defp request_ledger(row), do: Repo.all(from(entry in LedgerEntry, where: entry.request_id == ^row.id))

  defp await_settled_rows!(setup) do
    rows = await_rows!(setup, &(length(&1) == 2 and Enum.all?(&1, fn row -> row.completed_at != nil end)))
    # The native lane publishes delivery after request settlement and socket
    # cleanup. Its log precedes receipt persistence, so keep capture open until
    # each receipt is visible rather than treating completed_at as a log fence.
    Enum.each(rows, &await_delivery!/1)
    rows
  end

  defp await_rows!(setup, ready?, deadline \\ nil) do
    deadline = deadline || System.monotonic_time(:millisecond) + @detection_timeout_ms
    rows = pool_requests(setup)

    cond do
      ready?.(rows) ->
        rows

      System.monotonic_time(:millisecond) >= deadline ->
        flunk("successor requests did not reach their expected lifecycle phase: #{inspect(Enum.map(rows, &{&1.status, &1.last_error_code}))}")

      true ->
        receive do
        after
          5 -> await_rows!(setup, ready?, deadline)
        end
    end
  end

  defp await_delivery!(row, deadline \\ nil) do
    deadline = deadline || System.monotonic_time(:millisecond) + @detection_timeout_ms

    receipt =
      case request_attempts(row) do
        [attempt] -> Map.get(attempt.response_metadata || %{}, "downstream_delivery")
        _other -> nil
      end

    cond do
      is_map(receipt) ->
        receipt

      System.monotonic_time(:millisecond) >= deadline ->
        flunk("response #{row.id} has no independent downstream delivery receipt")

      true ->
        receive do
        after
          5 -> await_delivery!(row, deadline)
        end
    end
  end
end
