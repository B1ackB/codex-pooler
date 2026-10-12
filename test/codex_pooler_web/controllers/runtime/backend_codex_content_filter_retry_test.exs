defmodule CodexPoolerWeb.Runtime.BackendCodexContentFilterRetryTest do
  use CodexPoolerWeb.ConnCase, async: false
  import Ecto.Query
  import CodexPoolerWeb.Runtime.BackendCodexTestSupport, only: [gateway_setup: 2, gateway_upstream: 4, prime_routing_quota!: 1, register_unboxed_pool_cleanup!: 1, native_text_input: 1, start_public_endpoint!: 0, start_upstream: 1, public_websocket_connect!: 3, public_websocket_send_text!: 4, public_websocket_receive_text!: 3, first_event_terminal_sse: 2, first_event_terminal_payload: 2]
  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport, only: [model_serving_scope: 0, set_model_serving_mode!: 3, stop_websocket_owner_session: 1]
  alias CodexPooler.Accounting.{Attempt, LedgerEntry, NativeContentFilterRetry, Request, RequestClientRetryLink}
  alias CodexPooler.Accounts.Scope
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway.Persistence.{CodexSession, RoutingCircuitState}
  alias CodexPooler.Jobs.TokenRefreshWorker
  alias CodexPooler.Repo
  alias CodexPooler.Upstreams
  alias CodexPooler.Upstreams.Schemas.UpstreamIdentity
  alias Ecto.Adapters.SQL.Sandbox
  @moduletag capture_log: true
  @path "/backend-api/codex/responses"
  @budget 15_000

  for expires? <- [false, true] do
    @tag expires?: expires?, slow: "real PostgreSQL receipt writer and retry contend on separate connections"
    test "HTTP retry waits for durable CF receipt and checks lock-time expiry #{expires?}", context do
      CodexPooler.DataCase.stop_sandbox(context.sandbox_owner, context.sandbox_settings_cache)
      on_exit(fn -> Sandbox.mode(Repo, :manual) end)
      :ok = Sandbox.mode(Repo, :auto)
      terminal = %{"type" => "response.incomplete", "response" => %{"id" => "resp_synthetic_delayed", "status" => "incomplete", "incomplete_details" => %{"reason" => "content_filter"}, "output" => [], "usage" => %{"input_tokens" => 1, "output_tokens" => 1, "total_tokens" => 2}}}
      completed = %{"type" => "response.completed", "response" => %{"id" => "resp_synthetic_after_receipt", "status" => "completed", "output" => [], "usage" => %{"input_tokens" => 1, "output_tokens" => 1, "total_tokens" => 2}}}
      upstream = start_upstream(FakeUpstream.strict_sequence([FakeUpstream.raw_response(event(terminal), headers: [{"content-type", "text/event-stream"}]), FakeUpstream.raw_response(event(completed), headers: [{"content-type", "text/event-stream"}])]))
      setup = gateway_setup(upstream, compact?: true)
      register_unboxed_pool_cleanup!(setup)
      set_model_serving_mode!(model_serving_scope(), setup, "full")
      setup = Map.put(setup, :serving_mode, "full")
      port = start_public_endpoint!()
      thread = Ecto.UUID.generate()
      original = payload(setup, thread, native_text_input("synthetic delayed receipt"), 0)
      lock_key = System.unique_integer([:positive])
      trigger = "cf_receipt_#{lock_key}"
      # The writer holds the Request lock at this gate. Expire it in that same
      # transaction only after the retry is observed waiting for the writer.
      Repo.query!("CREATE FUNCTION #{trigger}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.response_metadata::jsonb ? 'downstream_delivery' AND OLD.response_metadata::jsonb ? 'native_content_filter_terminal' AND EXISTS (SELECT 1 FROM requests WHERE id = NEW.request_id AND pool_id = '#{setup.pool.id}'::uuid) THEN PERFORM pg_advisory_xact_lock(#{lock_key}); IF #{context.expires?} THEN UPDATE requests SET completed_at = clock_timestamp() - interval '31 seconds' WHERE id = NEW.request_id; END IF; END IF; RETURN NEW; END $$")
      Repo.query!("CREATE TRIGGER #{trigger} BEFORE UPDATE OF response_metadata ON attempts FOR EACH ROW EXECUTE FUNCTION #{trigger}()")

      on_exit(fn ->
        Repo.query!("DROP TRIGGER IF EXISTS #{trigger} ON attempts")
        Repo.query!("DROP FUNCTION IF EXISTS #{trigger}()")
      end)

      parent = self()

      holder =
        Task.async(fn ->
          Repo.transaction(fn ->
            Repo.query!("SELECT pg_advisory_xact_lock($1)", [lock_key])
            %{rows: [[backend]]} = Repo.query!("SELECT pg_backend_pid()")
            send(parent, {:receipt_gate, backend})

            receive do
              :release -> :ok
            after
              2 * @budget -> raise "receipt gate release missing"
            end
          end)
        end)

      assert_receive {:receipt_gate, holder_backend}, @budget
      {conn, reference} = start_request(port, setup, original, thread)
      {conn, _terminal} = read_http_terminal(conn, reference, "")
      first = await_latest_settled(setup, System.monotonic_time(:millisecond) + @budget)

      refute retry_expired?(first.id)

      attempt = Repo.get_by!(Attempt, request_id: first.id)
      assert attempt.response_metadata["native_content_filter_terminal"]["reason"] == "content_filter"
      refute attempt.response_metadata["downstream_delivery"]
      writer_backend = await_relation_waiter(holder_backend, "attempts", System.monotonic_time(:millisecond) + @budget)
      successor = Map.update!(original, "input", &(&1 ++ [guidance()]))
      headers = [{"authorization", setup.authorization}, {"content-type", "application/json"}, {"session-id", thread}, {"thread-id", thread}, {"x-codex-window-id", "#{thread}:0"}, {"x-codex-turn-metadata", original["client_metadata"]["x-codex-turn-metadata"]}, {"originator", "codex_cli_rs"}]
      contender = Task.async(fn -> Req.post!("http://127.0.0.1:#{port}#{@path}", headers: headers, json: successor, retry: false, receive_timeout: @budget).status end)

      try do
        # Receipt reconciliation and retry both lock the session before the attempt.
        retry_backend = await_relation_waiter(writer_backend, "codex_sessions", System.monotonic_time(:millisecond) + @budget)
        assert length(Enum.uniq([holder_backend, writer_backend, retry_backend])) == 3
        assert FakeUpstream.count(upstream) == 1
        assert Repo.aggregate(from(r in Request, where: r.pool_id == ^setup.pool.id), :count) == 1
        refute retry_expired?(first.id)
        refute Repo.get_by!(Attempt, request_id: first.id).response_metadata["downstream_delivery"]
        send(holder.pid, :release)
        assert {:ok, :ok} = Task.await(holder, @budget)
        assert Task.await(contender, @budget) == if(context.expires?, do: 409, else: 200)
        assert FakeUpstream.count(upstream) == if(context.expires?, do: 1, else: 2)
        assert retry_expired?(first.id) == context.expires?
        assert Repo.get_by!(Attempt, request_id: first.id).response_metadata["downstream_delivery"]["outcome"] == "delivered"
        assert Repo.aggregate(from(entry in LedgerEntry, where: entry.request_id == ^first.id and entry.entry_kind == "settlement"), :count) == 1
      after
        send(holder.pid, :release)
        Task.shutdown(holder, :brutal_kill)
        Task.shutdown(contender, :brutal_kill)
        Mint.HTTP.close(conn)
      end
    end
  end

  defp retry_expired?(request_id) do
    %{rows: [[expired?]]} = Repo.query!("SELECT clock_timestamp() > completed_at + interval '30 seconds' FROM requests WHERE id = $1", [Ecto.UUID.dump!(request_id)])
    expired?
  end

  defp read_http_terminal(conn, reference, acc) do
    assert {:ok, conn, messages} = Mint.HTTP.recv(conn, 0, @budget)

    acc =
      Enum.reduce(messages, acc, fn
        {:data, ^reference, bytes}, acc -> acc <> bytes
        _, acc -> acc
      end)

    if String.contains?(acc, "response.incomplete") and String.ends_with?(acc, "\n\n"), do: {conn, true}, else: read_http_terminal(conn, reference, acc)
  end

  defp await_relation_waiter(holder, relation, deadline) do
    %{rows: rows} = Repo.query!("SELECT DISTINCT a.pid FROM pg_stat_activity a JOIN pg_locks l ON l.pid = a.pid WHERE $1 = ANY(pg_blocking_pids(a.pid)) AND l.relation = $2::text::regclass", [holder, relation])

    cond do
      length(rows) == 1 ->
        hd(hd(rows))

      System.monotonic_time(:millisecond) > deadline ->
        flunk("expected PostgreSQL relation waiter missing")

      true ->
        Process.sleep(10)
        await_relation_waiter(holder, relation, deadline)
    end
  end

  for mode <- ["full", "lite"], forwarding? <- [false, true], retained? <- [false, true] do
    @tag mode: mode, forwarding?: forwarding?, retained?: retained?
    test "#{mode} websocket owner #{forwarding?} content-filter retains #{retained?}", context do
      CodexPooler.TestAppEnv.restore_on_exit(:websocket_owner_forwarding_enabled)
      Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, context.forwarding?)
      output = if context.retained?, do: [%{"type" => "reasoning", "id" => "rs_synthetic", "summary" => [], "encrypted_content" => "synthetic"}], else: []
      terminal = %{"type" => "response.incomplete", "response" => %{"id" => "resp_synthetic_cf", "status" => "incomplete", "incomplete_details" => %{"reason" => "content_filter"}, "output" => output, "usage" => %{"input_tokens" => 10, "output_tokens" => 1, "total_tokens" => 11}}}
      completed = %{"type" => "response.completed", "response" => %{"id" => "resp_synthetic_complete", "status" => "completed", "output" => [], "usage" => %{"input_tokens" => 10, "output_tokens" => 1, "total_tokens" => 11}}}
      frames = Enum.map(output, &%{"type" => "response.output_item.done", "item" => &1}) ++ [terminal]
      upstream = start_upstream(FakeUpstream.strict_sequence([FakeUpstream.websocket_text_frames(Enum.map(frames, &CodexPooler.JSON.encode!/1)), FakeUpstream.websocket_text_frames([CodexPooler.JSON.encode!(completed)])]))
      setup = gateway_setup(upstream, compact?: true)
      stop_owner_sessions_on_exit(setup)
      set_model_serving_mode!(model_serving_scope(), setup, context.mode)
      port = start_public_endpoint!()
      thread = Ecto.UUID.generate()
      input = native_text_input("synthetic")
      original = payload(setup, thread, input, 0) |> Map.put("type", "response.create")
      original = if context.mode == "lite", do: put_in(original, ["client_metadata", "ws_request_header_x_openai_internal_codex_responses_lite"], "true"), else: original
      {conn, websocket, ref} = public_websocket_connect!(port, setup, thread)
      on_exit(fn -> Mint.HTTP.close(conn) end)
      {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, CodexPooler.JSON.encode!(original))
      {conn, _websocket, terminal} = receive_terminal(conn, websocket, ref)
      assert terminal["type"] == "response.incomplete"
      Mint.HTTP.close(conn)
      first = await_latest_settled(setup, System.monotonic_time(:millisecond) + @budget)
      await_delivery(first, System.monotonic_time(:millisecond) + @budget, %{type: terminal["type"], reason: get_in(terminal, ["response", "incomplete_details", "reason"])})
      {conn, websocket, ref} = public_websocket_connect!(port, setup, thread)
      successor = Map.put(original, "input", input ++ output ++ [guidance()])
      {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, CodexPooler.JSON.encode!(successor))
      {conn, _websocket, terminal} = receive_terminal(conn, websocket, ref)
      Mint.HTTP.close(conn)
      assert terminal["type"] == "response.completed"
      assert FakeUpstream.count(upstream) == 2
      assert Repo.aggregate(from(r in Request, where: r.pool_id == ^setup.pool.id), :count) == 2
    end
  end

  # Codex closes its connection right after reading a content-filter terminal. The socket confirms what it pushed at its next callback and again at `terminate/2`, and a close that wins that race takes the connection's port with it, where the driver queue can no longer be read (findings#303 row 303-4). The race is held open at Bandit's write of the terminal: a telemetry handler runs in the socket's own connection process right after that write, parks it until the client's close has taken the port, and only then lets it reach its next callback.
  for mode <- ["full", "lite"], forwarding? <- [false, true] do
    @tag mode: mode, forwarding?: forwarding?
    test "#{mode} websocket owner #{forwarding?} content-filter terminal stays delivered when the client closes before the socket's next callback", context do
      CodexPooler.TestAppEnv.restore_on_exit(:websocket_owner_forwarding_enabled)
      Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, context.forwarding?)
      output = [%{"type" => "reasoning", "id" => "rs_synthetic", "summary" => [], "encrypted_content" => "synthetic"}]
      terminal = %{"type" => "response.incomplete", "response" => %{"id" => "resp_synthetic_cf", "status" => "incomplete", "incomplete_details" => %{"reason" => "content_filter"}, "output" => output, "usage" => %{"input_tokens" => 10, "output_tokens" => 1, "total_tokens" => 11}}}
      completed = %{"type" => "response.completed", "response" => %{"id" => "resp_synthetic_complete", "status" => "completed", "output" => [], "usage" => %{"input_tokens" => 10, "output_tokens" => 1, "total_tokens" => 11}}}
      frames = Enum.map(output, &%{"type" => "response.output_item.done", "item" => &1}) ++ [terminal]
      upstream = start_upstream(FakeUpstream.strict_sequence([FakeUpstream.websocket_text_frames(Enum.map(frames, &CodexPooler.JSON.encode!/1)), FakeUpstream.websocket_text_frames([CodexPooler.JSON.encode!(completed)])]))
      setup = gateway_setup(upstream, compact?: true)
      stop_owner_sessions_on_exit(setup)
      set_model_serving_mode!(model_serving_scope(), setup, context.mode)
      port = start_public_endpoint!()
      thread = Ecto.UUID.generate()
      input = native_text_input("synthetic")
      original = payload(setup, thread, input, 0) |> Map.put("type", "response.create")
      original = if context.mode == "lite", do: put_in(original, ["client_metadata", "ws_request_header_x_openai_internal_codex_responses_lite"], "true"), else: original
      hold = hold_after_terminal_write!(setup)
      {conn, websocket, ref} = public_websocket_connect!(port, setup, thread)
      on_exit(fn -> Mint.HTTP.close(conn) end)
      {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, CodexPooler.JSON.encode!(original))
      {conn, _websocket, terminal} = receive_terminal(conn, websocket, ref)
      assert terminal["type"] == "response.incomplete"
      assert_receive {:terminal_written, ^hold, socket}, @budget
      port_monitor = Port.monitor(connection_port!(socket))
      Mint.HTTP.close(conn)
      assert_receive {:DOWN, ^port_monitor, :port, _port, _reason}, @budget
      send(socket, {hold, :release})
      first = await_latest_settled(setup, System.monotonic_time(:millisecond) + @budget)
      await_delivery(first, System.monotonic_time(:millisecond) + @budget, %{type: terminal["type"], reason: get_in(terminal, ["response", "incomplete_details", "reason"])})
      {conn, websocket, ref} = public_websocket_connect!(port, setup, thread)
      successor = Map.put(original, "input", input ++ output ++ [guidance()])
      {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, CodexPooler.JSON.encode!(successor))
      {conn, _websocket, terminal} = receive_terminal(conn, websocket, ref)
      Mint.HTTP.close(conn)
      assert terminal["type"] == "response.completed"
      assert FakeUpstream.count(upstream) == 2
    end
  end

  # The same close can also be processed between Bandit's write of the terminal and the write watch's reading right after it: the watch then finds the port gone and cannot tell whether the terminal reached the kernel, the terminal stayed unconfirmed and the receipt `aborted`, so the guided retry answered 409 duplicate_turn (findings#315: Drone 1815 and 1839, both output shapes, forwarding on and off). This hold runs before the watch's own handler and parks the socket until the client's close has taken the port; only then does the watch read.
  for mode <- ["full", "lite"], forwarding? <- [false, true], retained? <- [false, true] do
    @tag mode: mode, forwarding?: forwarding?, retained?: retained?
    test "#{mode} websocket owner #{forwarding?} retains #{retained?} content-filter terminal stays delivered when the client closes before the write watch reads the queue", context do
      CodexPooler.TestAppEnv.restore_on_exit(:websocket_owner_forwarding_enabled)
      Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, context.forwarding?)
      output = if context.retained?, do: [%{"type" => "reasoning", "id" => "rs_synthetic", "summary" => [], "encrypted_content" => "synthetic"}], else: []
      terminal = %{"type" => "response.incomplete", "response" => %{"id" => "resp_synthetic_cf", "status" => "incomplete", "incomplete_details" => %{"reason" => "content_filter"}, "output" => output, "usage" => %{"input_tokens" => 10, "output_tokens" => 1, "total_tokens" => 11}}}
      completed = %{"type" => "response.completed", "response" => %{"id" => "resp_synthetic_complete", "status" => "completed", "output" => [], "usage" => %{"input_tokens" => 10, "output_tokens" => 1, "total_tokens" => 11}}}
      frames = Enum.map(output, &%{"type" => "response.output_item.done", "item" => &1}) ++ [terminal]
      upstream = start_upstream(FakeUpstream.strict_sequence([FakeUpstream.websocket_text_frames(Enum.map(frames, &CodexPooler.JSON.encode!/1)), FakeUpstream.websocket_text_frames([CodexPooler.JSON.encode!(completed)])]))
      setup = gateway_setup(upstream, compact?: true)
      stop_owner_sessions_on_exit(setup)
      set_model_serving_mode!(model_serving_scope(), setup, context.mode)
      port = start_public_endpoint!()
      thread = Ecto.UUID.generate()
      input = native_text_input("synthetic")
      original = payload(setup, thread, input, 0) |> Map.put("type", "response.create")
      original = if context.mode == "lite", do: put_in(original, ["client_metadata", "ws_request_header_x_openai_internal_codex_responses_lite"], "true"), else: original
      hold = hold_before_write_watch_read!(setup)
      {conn, websocket, ref} = public_websocket_connect!(port, setup, thread)
      on_exit(fn -> Mint.HTTP.close(conn) end)
      {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, CodexPooler.JSON.encode!(original))
      {conn, _websocket, terminal} = receive_terminal(conn, websocket, ref)
      assert terminal["type"] == "response.incomplete"
      assert_receive {:terminal_written, ^hold, socket}, @budget
      port_monitor = Port.monitor(connection_port!(socket))
      Mint.HTTP.close(conn)
      assert_receive {:DOWN, ^port_monitor, :port, _port, _reason}, @budget
      send(socket, {hold, :release})
      first = await_latest_settled(setup, System.monotonic_time(:millisecond) + @budget)
      await_delivery(first, System.monotonic_time(:millisecond) + @budget, %{type: terminal["type"], reason: get_in(terminal, ["response", "incomplete_details", "reason"])})
      {conn, websocket, ref} = public_websocket_connect!(port, setup, thread)
      successor = Map.put(original, "input", input ++ output ++ [guidance()])
      {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, CodexPooler.JSON.encode!(successor))
      {conn, _websocket, terminal} = receive_terminal(conn, websocket, ref)
      Mint.HTTP.close(conn)
      assert terminal["type"] == "response.completed"
      assert FakeUpstream.count(upstream) == 2
    end
  end

  # The released client resends a turn identically only when it never read its content-filter terminal (a cut connection, then the HTTPS fallback): after a terminal it read, every sampling retry carries the guidance (Codex rust-v0.160.1 `responses_retry.rs`). Such a predecessor admits only its verified guided retry, and dispatch requires that retry's binding for every successor linked to it, so an identical resend admitted under the identical-resend rule used to be linked and then refused at dispatch with 500 `gateway_accounting_failed`, its request left `in_progress` (findings#316). It is refused before it is linked, as a resend of a finished turn.
  for mode <- ["full", "lite"], forwarding? <- [false, true] do
    @tag mode: mode, forwarding?: forwarding?
    test "#{mode} websocket owner #{forwarding?} identical resend after a content-filter terminal is refused before it is linked", context do
      CodexPooler.TestAppEnv.restore_on_exit(:websocket_owner_forwarding_enabled)
      Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, context.forwarding?)
      terminal = %{"type" => "response.incomplete", "response" => %{"id" => "resp_synthetic_cf", "status" => "incomplete", "incomplete_details" => %{"reason" => "content_filter"}, "output" => [], "usage" => %{"input_tokens" => 10, "output_tokens" => 1, "total_tokens" => 11}}}
      completed = %{"type" => "response.completed", "response" => %{"id" => "resp_synthetic_complete", "status" => "completed", "output" => [], "usage" => %{"input_tokens" => 10, "output_tokens" => 1, "total_tokens" => 11}}}
      upstream = start_upstream(FakeUpstream.strict_sequence([FakeUpstream.websocket_text_frames([CodexPooler.JSON.encode!(terminal)]), FakeUpstream.websocket_text_frames([CodexPooler.JSON.encode!(completed)])]))
      setup = gateway_setup(upstream, compact?: true)
      stop_owner_sessions_on_exit(setup)
      set_model_serving_mode!(model_serving_scope(), setup, context.mode)
      port = start_public_endpoint!()
      thread = Ecto.UUID.generate()
      original = payload(setup, thread, native_text_input("synthetic"), 0) |> Map.put("type", "response.create")
      original = if context.mode == "lite", do: put_in(original, ["client_metadata", "ws_request_header_x_openai_internal_codex_responses_lite"], "true"), else: original
      {conn, websocket, ref} = public_websocket_connect!(port, setup, thread)
      on_exit(fn -> Mint.HTTP.close(conn) end)
      {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, CodexPooler.JSON.encode!(original))
      {conn, _websocket, terminal} = receive_terminal(conn, websocket, ref)
      assert terminal["type"] == "response.incomplete"
      Mint.HTTP.close(conn)
      first = await_latest_settled(setup, System.monotonic_time(:millisecond) + @budget)
      await_delivery(first, System.monotonic_time(:millisecond) + @budget, %{type: terminal["type"], reason: get_in(terminal, ["response", "incomplete_details", "reason"])})
      {conn, websocket, ref} = public_websocket_connect!(port, setup, thread)
      on_exit(fn -> Mint.HTTP.close(conn) end)
      {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, CodexPooler.JSON.encode!(original))
      {conn, _websocket, refusal} = receive_terminal(conn, websocket, ref)
      Mint.HTTP.close(conn)
      assert %{"type" => "error", "status" => 409, "error" => %{"code" => "duplicate_turn"}} = refusal
      assert_refused_before_linking!(setup, upstream, first)
    end
  end

  for mode <- ["full", "lite"] do
    @tag mode: mode
    test "#{mode} HTTP identical resend after a content-filter terminal is refused before it is linked", context do
      CodexPooler.DataCase.stop_sandbox(context.sandbox_owner, context.sandbox_settings_cache)
      on_exit(fn -> Sandbox.mode(Repo, :manual) end)
      :ok = Sandbox.mode(Repo, :auto)
      terminal = %{"type" => "response.incomplete", "response" => %{"id" => "resp_synthetic_cf", "status" => "incomplete", "incomplete_details" => %{"reason" => "content_filter"}, "output" => [], "usage" => %{"input_tokens" => 10, "output_tokens" => 1, "total_tokens" => 11}}}
      completed = %{"type" => "response.completed", "response" => %{"id" => "resp_synthetic_complete", "status" => "completed", "output" => [], "usage" => %{"input_tokens" => 10, "output_tokens" => 1, "total_tokens" => 11}}}
      upstream = start_upstream(FakeUpstream.strict_sequence([FakeUpstream.raw_response(event(terminal), headers: [{"content-type", "text/event-stream"}]), FakeUpstream.raw_response(event(completed), headers: [{"content-type", "text/event-stream"}])]))
      setup = gateway_setup(upstream, compact?: true)
      register_unboxed_pool_cleanup!(setup)
      set_model_serving_mode!(model_serving_scope(), setup, context.mode)
      setup = Map.put(setup, :serving_mode, context.mode)
      port = start_public_endpoint!()
      thread = Ecto.UUID.generate()
      original = payload(setup, thread, native_text_input("synthetic"), 0)
      assert {200, _} = post(port, setup, original, thread)
      first = await_latest_settled(setup, System.monotonic_time(:millisecond) + @budget)
      assert Repo.get_by!(Attempt, request_id: first.id).response_metadata["native_content_filter_terminal"]["reason"] == "content_filter"
      assert {409, body} = post(port, setup, original, thread)
      assert %{"error" => %{"code" => "duplicate_turn"}} = CodexPooler.JSON.decode!(body)
      assert_refused_before_linking!(setup, upstream, first)
    end
  end

  # A verified guided retry carries the binding of its predecessor's attempt, and dispatch refuses it on any other assignment: the provider reads retained reasoning only on the account that produced it (`ContentFilterRetryPin`). Moving the session's affinity to a second account between the terminal and the retry used to route the retry there, where dispatch refused it although the bound account could serve it (findings#318). Routing pins it to the bound account.
  for mode <- ["full", "lite"] do
    @tag mode: mode
    test "#{mode} HTTP guided retry is served on its bound account after the session's affinity moved", context do
      CodexPooler.DataCase.stop_sandbox(context.sandbox_owner, context.sandbox_settings_cache)
      on_exit(fn -> Sandbox.mode(Repo, :manual) end)
      :ok = Sandbox.mode(Repo, :auto)
      terminal = %{"type" => "response.incomplete", "response" => %{"id" => "resp_synthetic_cf", "status" => "incomplete", "incomplete_details" => %{"reason" => "content_filter"}, "output" => [], "usage" => %{"input_tokens" => 10, "output_tokens" => 1, "total_tokens" => 11}}}
      completed = %{"type" => "response.completed", "response" => %{"id" => "resp_synthetic_complete", "status" => "completed", "output" => [], "usage" => %{"input_tokens" => 10, "output_tokens" => 1, "total_tokens" => 11}}}
      upstream = start_upstream(FakeUpstream.strict_sequence([FakeUpstream.raw_response(event(terminal), headers: [{"content-type", "text/event-stream"}]), FakeUpstream.raw_response(event(completed), headers: [{"content-type", "text/event-stream"}])]))
      other_upstream = start_upstream(FakeUpstream.json_response(%{"id" => "must_not_run"}))
      setup = gateway_setup(upstream, compact?: true)
      register_unboxed_pool_cleanup!(setup)
      set_model_serving_mode!(model_serving_scope(), setup, context.mode)
      setup = Map.put(setup, :serving_mode, context.mode)
      port = start_public_endpoint!()
      thread = Ecto.UUID.generate()
      input = native_text_input("synthetic")
      original = payload(setup, thread, input, 0)
      assert {200, _} = post(port, setup, original, thread)
      first = await_latest_settled(setup, System.monotonic_time(:millisecond) + @budget)
      other = add_pool_account!(setup, other_upstream)
      move_affinity!(first, other.assignment)
      assert {200, _} = post(port, setup, Map.put(original, "input", input ++ [guidance()]), thread)
      assert FakeUpstream.count(upstream) == 2
      assert FakeUpstream.count(other_upstream) == 0
      assert_served_on!(setup, first, setup.assignment)
    end
  end

  for mode <- ["full", "lite"], forwarding? <- [false, true] do
    @tag mode: mode, forwarding?: forwarding?
    test "#{mode} websocket owner #{forwarding?} guided retry is served on its bound account after the session's affinity moved", context do
      CodexPooler.TestAppEnv.restore_on_exit(:websocket_owner_forwarding_enabled)
      Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, context.forwarding?)
      CodexPooler.DataCase.stop_sandbox(context.sandbox_owner, context.sandbox_settings_cache)
      on_exit(fn -> Sandbox.mode(Repo, :manual) end)
      :ok = Sandbox.mode(Repo, :auto)
      terminal = %{"type" => "response.incomplete", "response" => %{"id" => "resp_synthetic_cf", "status" => "incomplete", "incomplete_details" => %{"reason" => "content_filter"}, "output" => [], "usage" => %{"input_tokens" => 10, "output_tokens" => 1, "total_tokens" => 11}}}
      completed = %{"type" => "response.completed", "response" => %{"id" => "resp_synthetic_complete", "status" => "completed", "output" => [], "usage" => %{"input_tokens" => 10, "output_tokens" => 1, "total_tokens" => 11}}}
      # The provider drops the connection after the terminal, so the guided retry needs a new handshake with or without an owner session and routing decides where it goes.
      upstream = start_upstream(FakeUpstream.strict_sequence([FakeUpstream.websocket_text_frames_then_abrupt_close([CodexPooler.JSON.encode!(terminal)]), FakeUpstream.websocket_text_frames([CodexPooler.JSON.encode!(completed)])]))
      other_upstream = start_upstream(FakeUpstream.json_response(%{"id" => "must_not_run"}))
      setup = gateway_setup(upstream, compact?: true)
      register_unboxed_pool_cleanup!(setup)
      stop_owner_sessions_on_exit(setup)
      set_model_serving_mode!(model_serving_scope(), setup, context.mode)
      port = start_public_endpoint!()
      thread = Ecto.UUID.generate()
      input = native_text_input("synthetic")
      original = websocket_payload(setup, thread, input, context.mode)
      {first, terminal} = websocket_turn!(port, setup, thread, original)
      assert terminal["type"] == "response.incomplete"
      await_delivery(first, System.monotonic_time(:millisecond) + @budget, %{type: terminal["type"], reason: get_in(terminal, ["response", "incomplete_details", "reason"])})
      other = add_pool_account!(setup, other_upstream)
      move_affinity!(first, other.assignment)
      {_served, frame} = websocket_turn!(port, setup, thread, Map.put(original, "input", input ++ [guidance()]))
      assert %{"type" => "response.completed"} = frame
      assert FakeUpstream.count(upstream) == 2
      assert FakeUpstream.count(other_upstream) == 0
      assert_served_on!(setup, first, setup.assignment)
    end
  end

  # The pin applies only to a request that carries a valid binding: an ordinary request of the same session still follows the session's affinity to another account (findings#318).
  test "an ordinary request follows the session's affinity to another account", context do
    CodexPooler.DataCase.stop_sandbox(context.sandbox_owner, context.sandbox_settings_cache)
    on_exit(fn -> Sandbox.mode(Repo, :manual) end)
    :ok = Sandbox.mode(Repo, :auto)
    completed = %{"type" => "response.completed", "response" => %{"id" => "resp_synthetic_complete", "status" => "completed", "output" => [], "usage" => %{"input_tokens" => 10, "output_tokens" => 1, "total_tokens" => 11}}}
    upstream = start_upstream(FakeUpstream.strict_sequence([FakeUpstream.raw_response(event(completed), headers: [{"content-type", "text/event-stream"}])]))
    other_upstream = start_upstream(FakeUpstream.strict_sequence([FakeUpstream.raw_response(event(completed), headers: [{"content-type", "text/event-stream"}])]))
    setup = gateway_setup(upstream, compact?: true)
    register_unboxed_pool_cleanup!(setup)
    set_model_serving_mode!(model_serving_scope(), setup, "full")
    setup = Map.put(setup, :serving_mode, "full")
    port = start_public_endpoint!()
    thread = Ecto.UUID.generate()
    assert {200, _} = post(port, setup, payload(setup, thread, native_text_input("synthetic"), 0), thread)
    first = await_latest_settled(setup, System.monotonic_time(:millisecond) + @budget)
    other = add_pool_account!(setup, other_upstream)
    move_affinity!(first, other.assignment)
    next = payload(setup, thread, native_text_input("synthetic next turn"), 0, "synthetic_next_turn")
    assert {200, _} = post(port, setup, next, thread)
    assert FakeUpstream.count(upstream) == 1
    assert FakeUpstream.count(other_upstream) == 1
    served = await_latest_settled(setup, System.monotonic_time(:millisecond) + @budget)
    assert served.id != first.id
    assert [%Attempt{status: "succeeded", pool_upstream_assignment_id: assignment_id}] = Repo.all(from a in Attempt, where: a.request_id == ^served.id)
    assert assignment_id == other.assignment.id
  end

  # Routing selects the bound account, and attempt creation checks the binding again, so the pin cannot prevent every binding refusal at dispatch. Here attempt creation waits for the bound assignment's row while the predecessor stops being the settled request the binding names. That refusal used to answer 500 `gateway_accounting_failed` and leave the request `in_progress` (findings#316); it is finalized before any attempt, its reservation released, and answered 409.
  test "a guided retry refused at dispatch by its binding is finalized and answered 409", context do
    CodexPooler.DataCase.stop_sandbox(context.sandbox_owner, context.sandbox_settings_cache)
    on_exit(fn -> Sandbox.mode(Repo, :manual) end)
    :ok = Sandbox.mode(Repo, :auto)
    terminal = %{"type" => "response.incomplete", "response" => %{"id" => "resp_synthetic_cf", "status" => "incomplete", "incomplete_details" => %{"reason" => "content_filter"}, "output" => [], "usage" => %{"input_tokens" => 10, "output_tokens" => 1, "total_tokens" => 11}}}
    upstream = start_upstream(FakeUpstream.strict_sequence([FakeUpstream.raw_response(event(terminal), headers: [{"content-type", "text/event-stream"}])]))
    setup = gateway_setup(upstream, compact?: true)
    register_unboxed_pool_cleanup!(setup)
    set_model_serving_mode!(model_serving_scope(), setup, "full")
    setup = Map.put(setup, :serving_mode, "full")
    port = start_public_endpoint!()
    thread = Ecto.UUID.generate()
    input = native_text_input("synthetic")
    original = payload(setup, thread, input, 0)
    assert {200, _} = post(port, setup, original, thread)
    first = await_latest_settled(setup, System.monotonic_time(:millisecond) + @budget)
    parent = self()

    # `FOR NO KEY UPDATE` waits out the `FOR SHARE` attempt creation takes on the assignment (`ReferenceLocks`), not the key-share locks of foreign-key checks.
    holder =
      Task.async(fn ->
        Repo.transaction(fn ->
          Repo.query!("SELECT id FROM pool_upstream_assignments WHERE id = $1 FOR NO KEY UPDATE", [Ecto.UUID.dump!(setup.assignment.id)])
          %{rows: [[backend]]} = Repo.query!("SELECT pg_backend_pid()")
          send(parent, {:assignment_locked, backend})

          receive do
            :release -> :ok
          after
            2 * @budget -> raise "assignment lock release missing"
          end
        end)
      end)

    assert_receive {:assignment_locked, holder_backend}, @budget
    retry = Task.async(fn -> req_post!(port, setup, Map.put(original, "input", input ++ [guidance()]), thread) end)

    try do
      _attempt_creation = await_relation_waiter(holder_backend, "pool_upstream_assignments", System.monotonic_time(:millisecond) + @budget)
      [successor] = Repo.all(from r in Request, where: r.pool_id == ^setup.pool.id and r.id != ^first.id)
      assert successor.request_metadata["native_content_filter_binding"]["assignment_id"] == setup.assignment.id
      {1, _} = Repo.update_all(from(r in Request, where: r.id == ^first.id), set: [status: "failed"])
      send(holder.pid, :release)
      assert {:ok, :ok} = Task.await(holder, @budget)
      response = Task.await(retry, @budget)
      assert response.status == 409
      assert %{"error" => %{"code" => "duplicate_turn"}} = response.body
    after
      send(holder.pid, :release)
      Task.shutdown(holder, :brutal_kill)
      Task.shutdown(retry, :brutal_kill)
    end

    assert FakeUpstream.count(upstream) == 1
    [successor] = Repo.all(from r in Request, where: r.pool_id == ^setup.pool.id and r.id != ^first.id)
    assert %Request{status: "failed", response_status_code: 409, last_error_code: "invalid_content_filter_retry_binding", completed_at: %DateTime{}} = successor
    refute Repo.exists?(from a in Attempt, where: a.request_id == ^successor.id)
    assert Repo.exists?(from l in RequestClientRetryLink, where: l.predecessor_request_id == ^first.id and l.successor_request_id == ^successor.id)
    assert [{"release", nil, "routing_rejected"}, {"reservation", nil, nil}] = ledger_entries(successor)
  end

  # A guided retry stays on its bound account (findings#318). When route filtering excluded that account (an open circuit), the retry is refused with the retryable 503 before any attempt: its reservation is released and the refused row gives up its claim and client-retry link, so the client's next retry, which Codex rust-v0.160.1 sends after a 503 (`UnexpectedStatus` has a retry delay, `Retry-After` honoured), chains onto the content-filtered request again and is served there once the account is eligible. Two such retries admit one: the other meets the successor the first one claimed.
  for mode <- ["full", "lite"] do
    @tag mode: mode
    test "#{mode} HTTP guided retry whose bound account is excluded is refused 503 and served there once it is eligible", context do
      CodexPooler.DataCase.stop_sandbox(context.sandbox_owner, context.sandbox_settings_cache)
      on_exit(fn -> Sandbox.mode(Repo, :manual) end)
      :ok = Sandbox.mode(Repo, :auto)
      terminal = %{"type" => "response.incomplete", "response" => %{"id" => "resp_synthetic_cf", "status" => "incomplete", "incomplete_details" => %{"reason" => "content_filter"}, "output" => [], "usage" => %{"input_tokens" => 10, "output_tokens" => 1, "total_tokens" => 11}}}
      completed = %{"type" => "response.completed", "response" => %{"id" => "resp_synthetic_complete", "status" => "completed", "output" => [], "usage" => %{"input_tokens" => 10, "output_tokens" => 1, "total_tokens" => 11}}}
      upstream = start_upstream(FakeUpstream.strict_sequence([FakeUpstream.raw_response(event(terminal), headers: [{"content-type", "text/event-stream"}]), FakeUpstream.raw_response(event(completed), headers: [{"content-type", "text/event-stream"}])]))
      other_upstream = start_upstream(FakeUpstream.json_response(%{"id" => "must_not_run"}))
      setup = gateway_setup(upstream, compact?: true)
      register_unboxed_pool_cleanup!(setup)
      set_model_serving_mode!(model_serving_scope(), setup, context.mode)
      setup = Map.put(setup, :serving_mode, context.mode)
      port = start_public_endpoint!()
      thread = Ecto.UUID.generate()
      input = native_text_input("synthetic")
      original = payload(setup, thread, input, 0)
      assert {200, _} = post(port, setup, original, thread)
      first = await_latest_settled(setup, System.monotonic_time(:millisecond) + @budget)
      other = add_pool_account!(setup, other_upstream)
      move_affinity!(first, other.assignment)
      circuit = open_circuit!(setup, "proxy_stream")
      retry = Map.put(original, "input", input ++ [guidance()])
      refusal = req_post!(port, setup, retry, thread)
      assert refusal.status == 503
      assert %{"error" => %{"code" => "no_eligible_backend"}} = refusal.body
      assert [seconds] = Req.Response.get_header(refusal, "retry-after")
      assert String.to_integer(seconds) in 1..60
      assert FakeUpstream.count(upstream) == 1
      assert FakeUpstream.count(other_upstream) == 0
      [refused] = Repo.all(from r in Request, where: r.pool_id == ^setup.pool.id and r.id != ^first.id)
      assert %Request{status: "failed", response_status_code: 503, last_error_code: "no_eligible_backend", completed_at: %DateTime{}} = refused
      refute Repo.exists?(from a in Attempt, where: a.request_id == ^refused.id)
      assert [{"release", nil, "routing_rejected"}, {"reservation", nil, nil}] = ledger_entries(refused)
      assert_claim_released!(first, refused)
      assert %RoutingCircuitState{status: "open", metadata: %{"probe_in_flight_count" => 0}} = Repo.reload!(circuit)
      Repo.delete!(circuit)

      if context.mode == "full" do
        race_successor(port, setup, retry, thread, first.request_metadata["codex_session_id"])
      else
        assert {200, _} = post(port, setup, retry, thread)
      end

      assert FakeUpstream.count(upstream) == 2
      assert FakeUpstream.count(other_upstream) == 0
      assert_served_on!(setup, first, setup.assignment)
    end
  end

  for mode <- ["full", "lite"], forwarding? <- [false, true] do
    @tag mode: mode, forwarding?: forwarding?
    test "#{mode} websocket owner #{forwarding?} guided retry whose bound account is excluded is refused 503 and served there once it is eligible", context do
      CodexPooler.TestAppEnv.restore_on_exit(:websocket_owner_forwarding_enabled)
      Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, context.forwarding?)
      CodexPooler.DataCase.stop_sandbox(context.sandbox_owner, context.sandbox_settings_cache)
      on_exit(fn -> Sandbox.mode(Repo, :manual) end)
      :ok = Sandbox.mode(Repo, :auto)
      terminal = %{"type" => "response.incomplete", "response" => %{"id" => "resp_synthetic_cf", "status" => "incomplete", "incomplete_details" => %{"reason" => "content_filter"}, "output" => [], "usage" => %{"input_tokens" => 10, "output_tokens" => 1, "total_tokens" => 11}}}
      completed = %{"type" => "response.completed", "response" => %{"id" => "resp_synthetic_complete", "status" => "completed", "output" => [], "usage" => %{"input_tokens" => 10, "output_tokens" => 1, "total_tokens" => 11}}}
      upstream = start_upstream(FakeUpstream.strict_sequence([FakeUpstream.websocket_text_frames_then_abrupt_close([CodexPooler.JSON.encode!(terminal)]), FakeUpstream.websocket_text_frames([CodexPooler.JSON.encode!(completed)])]))
      other_upstream = start_upstream(FakeUpstream.json_response(%{"id" => "must_not_run"}))
      setup = gateway_setup(upstream, compact?: true)
      register_unboxed_pool_cleanup!(setup)
      stop_owner_sessions_on_exit(setup)
      set_model_serving_mode!(model_serving_scope(), setup, context.mode)
      port = start_public_endpoint!()
      thread = Ecto.UUID.generate()
      input = native_text_input("synthetic")
      original = websocket_payload(setup, thread, input, context.mode)
      {first, terminal} = websocket_turn!(port, setup, thread, original)
      assert terminal["type"] == "response.incomplete"
      await_delivery(first, System.monotonic_time(:millisecond) + @budget, %{type: terminal["type"], reason: get_in(terminal, ["response", "incomplete_details", "reason"])})
      other = add_pool_account!(setup, other_upstream)
      move_affinity!(first, other.assignment)
      circuit = open_circuit!(setup, "proxy_websocket")
      retry = Map.put(original, "input", input ++ [guidance()])
      {refused, refusal} = websocket_turn!(port, setup, thread, retry)
      assert %{"type" => "error", "status" => 503, "error" => %{"code" => "no_eligible_backend"}} = refusal
      assert String.to_integer(refusal["headers"]["retry-after"]) in 1..60
      assert FakeUpstream.count(upstream) == 1
      assert FakeUpstream.count(other_upstream) == 0
      assert refused.id != first.id
      assert %Request{status: "failed", response_status_code: 503, last_error_code: "no_eligible_backend", completed_at: %DateTime{}} = refused
      refute Repo.exists?(from a in Attempt, where: a.request_id == ^refused.id)
      assert [{"release", nil, "routing_rejected"}, {"reservation", nil, nil}] = ledger_entries(refused)
      assert_claim_released!(first, refused)
      assert %RoutingCircuitState{status: "open", metadata: %{"probe_in_flight_count" => 0}} = Repo.reload!(circuit)
      Repo.delete!(circuit)
      {_served, frame} = websocket_turn!(port, setup, thread, retry)
      assert %{"type" => "response.completed"} = frame
      assert FakeUpstream.count(upstream) == 2
      assert FakeUpstream.count(other_upstream) == 0
      assert_served_on!(setup, first, setup.assignment)
    end
  end

  # A guided retry can fail on its account before any output (a first-event `server_error`), and the client retries it. That retry is the exact retry of a zero-output node and carries no binding, so it followed the session's affinity to another account, which drops the retained reasoning without an error; with owner forwarding on, the content-filter preflight refused it and ended the turn. It inherits the guided retry's account as a routing-only pin (`native_content_filter_pin`) and is served there; dispatch validation still reads only the binding (findings#318 row 318-2).
  for mode <- ["full", "lite"] do
    @tag mode: mode
    test "#{mode} HTTP retry of a guided retry that failed on its account stays on that account", context do
      CodexPooler.DataCase.stop_sandbox(context.sandbox_owner, context.sandbox_settings_cache)
      on_exit(fn -> Sandbox.mode(Repo, :manual) end)
      :ok = Sandbox.mode(Repo, :auto)
      terminal = %{"type" => "response.incomplete", "response" => %{"id" => "resp_synthetic_cf", "status" => "incomplete", "incomplete_details" => %{"reason" => "content_filter"}, "output" => [], "usage" => %{"input_tokens" => 10, "output_tokens" => 1, "total_tokens" => 11}}}
      completed = %{"type" => "response.completed", "response" => %{"id" => "resp_synthetic_complete", "status" => "completed", "output" => [], "usage" => %{"input_tokens" => 10, "output_tokens" => 1, "total_tokens" => 11}}}
      upstream = start_upstream(FakeUpstream.strict_sequence([FakeUpstream.raw_response(event(terminal), headers: [{"content-type", "text/event-stream"}]), first_event_terminal_sse("response.failed", "server_error"), FakeUpstream.raw_response(event(completed), headers: [{"content-type", "text/event-stream"}])]))
      # The other account could serve the retry, so only routing keeps it off.
      other_upstream = start_upstream(FakeUpstream.raw_response(event(completed), headers: [{"content-type", "text/event-stream"}]))
      setup = gateway_setup(upstream, compact?: true)
      register_unboxed_pool_cleanup!(setup)
      set_model_serving_mode!(model_serving_scope(), setup, context.mode)
      setup = Map.put(setup, :serving_mode, context.mode)
      port = start_public_endpoint!()
      thread = Ecto.UUID.generate()
      input = native_text_input("synthetic")
      original = payload(setup, thread, input, 0)
      assert {200, _} = post(port, setup, original, thread)
      first = await_latest_settled(setup, System.monotonic_time(:millisecond) + @budget)
      other = add_pool_account!(setup, other_upstream)
      move_affinity!(first, other.assignment)
      retry = Map.put(original, "input", input ++ [guidance()])
      assert {200, body} = post(port, setup, retry, thread)
      assert body =~ "server_error"
      guided = await_latest_settled(setup, System.monotonic_time(:millisecond) + @budget)
      assert %Request{status: "failed", last_error_code: "server_error"} = guided
      assert {200, _} = post(port, setup, retry, thread)
      assert FakeUpstream.count(upstream) == 3
      assert FakeUpstream.count(other_upstream) == 0
      assert_pinned_retry_served!(setup, guided)
    end
  end

  for mode <- ["full", "lite"], forwarding? <- [false, true] do
    @tag mode: mode, forwarding?: forwarding?
    test "#{mode} websocket owner #{forwarding?} retry of a guided retry that failed on its account stays on that account", context do
      CodexPooler.TestAppEnv.restore_on_exit(:websocket_owner_forwarding_enabled)
      Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, context.forwarding?)
      CodexPooler.DataCase.stop_sandbox(context.sandbox_owner, context.sandbox_settings_cache)
      on_exit(fn -> Sandbox.mode(Repo, :manual) end)
      :ok = Sandbox.mode(Repo, :auto)
      terminal = %{"type" => "response.incomplete", "response" => %{"id" => "resp_synthetic_cf", "status" => "incomplete", "incomplete_details" => %{"reason" => "content_filter"}, "output" => [], "usage" => %{"input_tokens" => 10, "output_tokens" => 1, "total_tokens" => 11}}}
      completed = %{"type" => "response.completed", "response" => %{"id" => "resp_synthetic_complete", "status" => "completed", "output" => [], "usage" => %{"input_tokens" => 10, "output_tokens" => 1, "total_tokens" => 11}}}
      {_type, failed} = first_event_terminal_payload("response.failed", "server_error")
      # The provider drops the connection after each terminal, so every retry needs a new handshake and routing decides where it goes.
      upstream = start_upstream(FakeUpstream.strict_sequence([FakeUpstream.websocket_text_frames_then_abrupt_close([CodexPooler.JSON.encode!(terminal)]), FakeUpstream.websocket_text_frames_then_abrupt_close([CodexPooler.JSON.encode!(failed)]), FakeUpstream.websocket_text_frames([CodexPooler.JSON.encode!(completed)])]))
      # The other account could serve the retry, so only routing keeps it off.
      other_upstream = start_upstream(FakeUpstream.websocket_text_frames([CodexPooler.JSON.encode!(completed)]))
      setup = gateway_setup(upstream, compact?: true)
      register_unboxed_pool_cleanup!(setup)
      stop_owner_sessions_on_exit(setup)
      set_model_serving_mode!(model_serving_scope(), setup, context.mode)
      port = start_public_endpoint!()
      thread = Ecto.UUID.generate()
      input = native_text_input("synthetic")
      original = websocket_payload(setup, thread, input, context.mode)
      {first, terminal} = websocket_turn!(port, setup, thread, original)
      assert terminal["type"] == "response.incomplete"
      await_delivery(first, System.monotonic_time(:millisecond) + @budget, %{type: terminal["type"], reason: get_in(terminal, ["response", "incomplete_details", "reason"])})
      other = add_pool_account!(setup, other_upstream)
      move_affinity!(first, other.assignment)
      retry = Map.put(original, "input", input ++ [guidance()])
      {guided, frame} = websocket_turn!(port, setup, thread, retry)
      assert %{"type" => "response.failed"} = frame
      assert %Request{status: "failed", last_error_code: "server_error"} = guided
      {_served, frame} = websocket_turn!(port, setup, thread, retry)
      assert %{"type" => "response.completed"} = frame
      assert FakeUpstream.count(upstream) == 3
      assert FakeUpstream.count(other_upstream) == 0
      assert_pinned_retry_served!(setup, guided)
    end
  end

  # A retry that inherited the pin is refused like a guided retry when its account is excluded: the retryable 503 before any attempt, and the refused row gives up its claim and link, so the client's next retry chains onto the failed guided retry again and is served on the account once it is eligible.
  test "a pinned retry whose account is excluded is refused 503 and served there once it is eligible", context do
    CodexPooler.DataCase.stop_sandbox(context.sandbox_owner, context.sandbox_settings_cache)
    on_exit(fn -> Sandbox.mode(Repo, :manual) end)
    :ok = Sandbox.mode(Repo, :auto)
    terminal = %{"type" => "response.incomplete", "response" => %{"id" => "resp_synthetic_cf", "status" => "incomplete", "incomplete_details" => %{"reason" => "content_filter"}, "output" => [], "usage" => %{"input_tokens" => 10, "output_tokens" => 1, "total_tokens" => 11}}}
    completed = %{"type" => "response.completed", "response" => %{"id" => "resp_synthetic_complete", "status" => "completed", "output" => [], "usage" => %{"input_tokens" => 10, "output_tokens" => 1, "total_tokens" => 11}}}
    upstream = start_upstream(FakeUpstream.strict_sequence([FakeUpstream.raw_response(event(terminal), headers: [{"content-type", "text/event-stream"}]), first_event_terminal_sse("response.failed", "server_error"), FakeUpstream.raw_response(event(completed), headers: [{"content-type", "text/event-stream"}])]))
    other_upstream = start_upstream(FakeUpstream.raw_response(event(completed), headers: [{"content-type", "text/event-stream"}]))
    setup = gateway_setup(upstream, compact?: true)
    register_unboxed_pool_cleanup!(setup)
    set_model_serving_mode!(model_serving_scope(), setup, "full")
    setup = Map.put(setup, :serving_mode, "full")
    port = start_public_endpoint!()
    thread = Ecto.UUID.generate()
    input = native_text_input("synthetic")
    original = payload(setup, thread, input, 0)
    assert {200, _} = post(port, setup, original, thread)
    first = await_latest_settled(setup, System.monotonic_time(:millisecond) + @budget)
    other = add_pool_account!(setup, other_upstream)
    move_affinity!(first, other.assignment)
    retry = Map.put(original, "input", input ++ [guidance()])
    assert {200, _} = post(port, setup, retry, thread)
    guided = await_latest_settled(setup, System.monotonic_time(:millisecond) + @budget)
    assert %Request{status: "failed", last_error_code: "server_error"} = guided
    circuit = open_circuit!(setup, "proxy_stream")
    refusal = req_post!(port, setup, retry, thread)
    assert refusal.status == 503
    assert %{"error" => %{"code" => "no_eligible_backend"}} = refusal.body
    refused = await_latest_settled(setup, System.monotonic_time(:millisecond) + @budget)
    assert %Request{status: "failed", response_status_code: 503, last_error_code: "no_eligible_backend"} = refused
    assert refused.request_metadata["native_content_filter_pin"]["assignment_id"] == setup.assignment.id
    refute Repo.exists?(from a in Attempt, where: a.request_id == ^refused.id)
    assert [{"release", nil, "routing_rejected"}, {"reservation", nil, nil}] = ledger_entries(refused)
    assert_claim_released!(guided, refused)
    Repo.delete!(circuit)
    assert {200, _} = post(port, setup, retry, thread)
    assert FakeUpstream.count(upstream) == 3
    assert FakeUpstream.count(other_upstream) == 0
    assert_pinned_retry_served!(setup, guided)
  end

  # A guided retry whose attempt meets a provider 401 refreshes its account's access token and retries on the same assignment, where its binding is checked again. The refresh renews the credential of the same provider account, so with the predecessor settled the retry is served there; the refresh used to advance the credential epoch the binding compared, and that alone refused every such retry 409 (findings#330). A predecessor that stops being the settled request the binding names while the refresh is held refuses the retry: that refusal used to answer 500 `gateway_accounting_failed` and leave the request `in_progress` (findings#316); it is finalized after the attempt the retry never replaced and answered 409.
  for mode <- ["full", "lite"], predecessor <- [:settled, :failed] do
    @tag mode: mode, predecessor: predecessor
    test "#{mode} HTTP guided retry after a 401 and its account's token refresh, predecessor #{predecessor}", context do
      CodexPooler.DataCase.stop_sandbox(context.sandbox_owner, context.sandbox_settings_cache)
      on_exit(fn -> Sandbox.mode(Repo, :manual) end)
      :ok = Sandbox.mode(Repo, :auto)
      ref = make_ref()
      terminal = %{"type" => "response.incomplete", "response" => %{"id" => "resp_synthetic_cf", "status" => "incomplete", "incomplete_details" => %{"reason" => "content_filter"}, "output" => [], "usage" => %{"input_tokens" => 10, "output_tokens" => 1, "total_tokens" => 11}}}
      completed = %{"type" => "response.completed", "response" => %{"id" => "resp_synthetic_after_refresh", "status" => "completed", "output" => [], "usage" => %{"input_tokens" => 10, "output_tokens" => 1, "total_tokens" => 11}}}
      unauthorized = FakeUpstream.json_response(%{"error" => %{"code" => "invalid_api_key", "message" => "synthetic", "type" => "invalid_request_error"}}, 401)
      refreshed = FakeUpstream.barrier_json_response(%{"access_token" => "synthetic-refreshed-token", "expires_in" => 3600}, notify: self(), release_ref: ref)
      served = if context.predecessor == :settled, do: [FakeUpstream.raw_response(event(completed), headers: [{"content-type", "text/event-stream"}])], else: []
      # provenance: synthetic_adversarial
      upstream = start_upstream(FakeUpstream.strict_sequence([FakeUpstream.raw_response(event(terminal), headers: [{"content-type", "text/event-stream"}]), FakeUpstream.expect_request(method: "POST", path: @path, respond: unauthorized), FakeUpstream.expect_request(method: "POST", path: "/oauth/token", respond: refreshed)] ++ served))
      setup = gateway_setup(upstream, compact?: true)
      register_unboxed_pool_cleanup!(setup)
      assert {:ok, _secret} = Upstreams.store_encrypted_secret(setup.identity, %{secret_kind: "refresh_token", plaintext: "synthetic-refresh-token"})
      set_model_serving_mode!(model_serving_scope(), setup, context.mode)
      setup = Map.put(setup, :serving_mode, context.mode)
      port = start_public_endpoint!()
      thread = Ecto.UUID.generate()
      input = native_text_input("synthetic")
      original = payload(setup, thread, input, 0)
      assert {200, _} = post(port, setup, original, thread)
      first = await_latest_settled(setup, System.monotonic_time(:millisecond) + @budget)
      retry = Task.async(fn -> req_post!(port, setup, Map.put(original, "input", input ++ [guidance()]), thread) end)
      assert_receive {:fake_upstream_timeout_barrier, :before_headers, provider, ^ref}, @budget
      on_exit(fn -> send(provider, {:fake_upstream_release_timeout, ref}) end)
      if context.predecessor == :failed, do: {1, _} = Repo.update_all(from(r in Request, where: r.id == ^first.id), set: [status: "failed"])
      send(provider, {:fake_upstream_release_timeout, ref})
      response = Task.await(retry, @budget)
      [successor] = Repo.all(from r in Request, where: r.pool_id == ^setup.pool.id and r.id != ^first.id)
      assert_epoch_advanced_past_binding!(setup, successor)

      case context.predecessor do
        :settled ->
          assert response.status == 200
          assert response.body =~ "response.completed"
          assert FakeUpstream.count(upstream) == 4
          assert_refreshed_retry_served!(setup, first)

        :failed ->
          assert response.status == 409
          assert %{"error" => %{"code" => "duplicate_turn"}} = response.body
          assert FakeUpstream.count(upstream) == 3
          assert %Request{status: "failed", response_status_code: 409, last_error_code: "invalid_content_filter_retry_binding", completed_at: %DateTime{}} = successor
          assert [%Attempt{id: attempt_id, status: "retryable_failed"}] = Repo.all(from a in Attempt, where: a.request_id == ^successor.id)
          assert [{"release", ^attempt_id}, {"reservation", nil}] = Enum.sort(Repo.all(from l in LedgerEntry, where: l.request_id == ^successor.id, select: {l.entry_kind, l.attempt_id}))
      end
    end
  end

  # The websocket form: the guided retry's upstream handshake meets a 401, the access token is refreshed, and the same-assignment retry that follows is served on the refreshed credential, or refused when the predecessor stopped being the settled request the binding names while the refresh was held (findings#316, findings#330).
  for mode <- ["full", "lite"], forwarding? <- [false, true], predecessor <- [:settled, :failed] do
    @tag mode: mode, forwarding?: forwarding?, predecessor: predecessor
    test "#{mode} websocket owner #{forwarding?} guided retry after a 401 and its account's token refresh, predecessor #{predecessor}", context do
      CodexPooler.TestAppEnv.restore_on_exit(:websocket_owner_forwarding_enabled)
      Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, context.forwarding?)
      CodexPooler.DataCase.stop_sandbox(context.sandbox_owner, context.sandbox_settings_cache)
      on_exit(fn -> Sandbox.mode(Repo, :manual) end)
      :ok = Sandbox.mode(Repo, :auto)
      ref = make_ref()
      terminal = %{"type" => "response.incomplete", "response" => %{"id" => "resp_synthetic_cf", "status" => "incomplete", "incomplete_details" => %{"reason" => "content_filter"}, "output" => [], "usage" => %{"input_tokens" => 10, "output_tokens" => 1, "total_tokens" => 11}}}
      completed = %{"type" => "response.completed", "response" => %{"id" => "resp_synthetic_after_refresh", "status" => "completed", "output" => [], "usage" => %{"input_tokens" => 10, "output_tokens" => 1, "total_tokens" => 11}}}
      unauthorized = FakeUpstream.websocket_upgrade_error(%{"error" => %{"code" => "invalid_api_key"}}, status: 401, headers: [{"x-openai-authorization-error", "invalid_api_key"}])
      refreshed = FakeUpstream.barrier_json_response(%{"access_token" => "synthetic-refreshed-token", "expires_in" => 3600}, notify: self(), release_ref: ref)
      served = if context.predecessor == :settled, do: [FakeUpstream.websocket_text_frames([CodexPooler.JSON.encode!(completed)])], else: []
      # provenance: synthetic_adversarial
      # The provider drops the connection after the terminal, so the guided retry needs a new handshake with or without an owner session.
      upstream = start_upstream(FakeUpstream.strict_sequence([FakeUpstream.websocket_text_frames_then_abrupt_close([CodexPooler.JSON.encode!(terminal)]), FakeUpstream.expect_request(method: "GET", respond: unauthorized), FakeUpstream.expect_request(method: "POST", path: "/oauth/token", respond: refreshed)] ++ served))
      setup = gateway_setup(upstream, compact?: true)
      register_unboxed_pool_cleanup!(setup)
      stop_owner_sessions_on_exit(setup)
      assert {:ok, _secret} = Upstreams.store_encrypted_secret(setup.identity, %{secret_kind: "refresh_token", plaintext: "synthetic-refresh-token"})
      set_model_serving_mode!(model_serving_scope(), setup, context.mode)
      port = start_public_endpoint!()
      thread = Ecto.UUID.generate()
      input = native_text_input("synthetic")
      original = websocket_payload(setup, thread, input, context.mode)
      {first, terminal} = websocket_turn!(port, setup, thread, original)
      assert terminal["type"] == "response.incomplete"
      await_delivery(first, System.monotonic_time(:millisecond) + @budget, %{type: terminal["type"], reason: get_in(terminal, ["response", "incomplete_details", "reason"])})
      {conn, websocket, ref_ws} = public_websocket_connect!(port, setup, thread)
      on_exit(fn -> Mint.HTTP.close(conn) end)
      {conn, websocket} = public_websocket_send_text!(conn, websocket, ref_ws, CodexPooler.JSON.encode!(Map.put(original, "input", input ++ [guidance()])))
      assert_receive {:fake_upstream_timeout_barrier, :before_headers, provider, ^ref}, @budget
      on_exit(fn -> send(provider, {:fake_upstream_release_timeout, ref}) end)
      if context.predecessor == :failed, do: {1, _} = Repo.update_all(from(r in Request, where: r.id == ^first.id), set: [status: "failed"])
      send(provider, {:fake_upstream_release_timeout, ref})
      {conn, _websocket, frame} = receive_terminal(conn, websocket, ref_ws)
      Mint.HTTP.close(conn)
      successor = await_latest_settled(setup, System.monotonic_time(:millisecond) + @budget)
      assert successor.id != first.id
      assert_epoch_advanced_past_binding!(setup, successor)

      case context.predecessor do
        :settled ->
          assert %{"type" => "response.completed"} = frame
          # The refused handshake is not a recorded request.
          assert [{"WEBSOCKET", @path}, {"POST", "/oauth/token"}, {"WEBSOCKET", @path}] = Enum.map(FakeUpstream.requests(upstream), &{&1.method, &1.path})
          assert_refreshed_retry_served!(setup, first)

        :failed ->
          assert %{"type" => "error", "status" => 409, "error" => %{"code" => "duplicate_turn"}} = frame
          assert [{"WEBSOCKET", @path}, {"POST", "/oauth/token"}] = Enum.map(FakeUpstream.requests(upstream), &{&1.method, &1.path})
          assert %Request{status: "failed", response_status_code: 409, last_error_code: "invalid_content_filter_retry_binding"} = successor
          assert [%Attempt{id: attempt_id, status: "retryable_failed"}] = Repo.all(from a in Attempt, where: a.request_id == ^successor.id)
          assert [{"release", ^attempt_id}, {"reservation", nil}] = Enum.sort(Repo.all(from l in LedgerEntry, where: l.request_id == ^successor.id, select: {l.entry_kind, l.attempt_id}))
      end
    end
  end

  # A token refresh of the bound account between the content-filter terminal and the guided retry (here the scheduled refresh job, which runs the refresh a 401 starts) renews the credential that produced the retained reasoning, so the retry is admitted and served there. The refresh advanced the credential epoch the binding records, and admission (`NativeContentFilterRetry.current_source?/1`) refused every such retry 409 (findings#330). An operator's re-import of the account's credential starts a new credential, and the retry is still refused before it is linked.
  for transport <- [:http, :websocket_owner_false, :websocket_owner_true], mode <- ["full", "lite"], change <- [:scheduled_refresh, :reimport] do
    @tag transport: transport, mode: mode, change: change
    test "#{mode} #{transport} guided retry after a #{change} of its account's credential", context do
      CodexPooler.TestAppEnv.restore_on_exit(:websocket_owner_forwarding_enabled)
      Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, context.transport == :websocket_owner_true)
      CodexPooler.DataCase.stop_sandbox(context.sandbox_owner, context.sandbox_settings_cache)
      on_exit(fn -> Sandbox.mode(Repo, :manual) end)
      :ok = Sandbox.mode(Repo, :auto)
      terminal = %{"type" => "response.incomplete", "response" => %{"id" => "resp_synthetic_cf", "status" => "incomplete", "incomplete_details" => %{"reason" => "content_filter"}, "output" => [], "usage" => %{"input_tokens" => 10, "output_tokens" => 1, "total_tokens" => 11}}}
      completed = %{"type" => "response.completed", "response" => %{"id" => "resp_synthetic_after_change", "status" => "completed", "output" => [], "usage" => %{"input_tokens" => 10, "output_tokens" => 1, "total_tokens" => 11}}}
      refresh = if context.change == :scheduled_refresh, do: [FakeUpstream.expect_request(method: "POST", path: "/oauth/token", respond: FakeUpstream.json_response(%{"access_token" => "synthetic-refreshed-token", "expires_in" => 3600}))], else: []
      served = if context.change == :scheduled_refresh, do: [credential_reply(context.transport, completed)], else: []
      # provenance: synthetic_adversarial
      upstream = start_upstream(FakeUpstream.strict_sequence([credential_reply(context.transport, terminal, :then_close)] ++ refresh ++ served))
      setup = gateway_setup(upstream, compact?: true)
      register_unboxed_pool_cleanup!(setup)
      if context.transport != :http, do: stop_owner_sessions_on_exit(setup)
      assert {:ok, _secret} = Upstreams.store_encrypted_secret(setup.identity, %{secret_kind: "refresh_token", plaintext: "synthetic-refresh-token"})
      set_model_serving_mode!(model_serving_scope(), setup, context.mode)
      setup = Map.put(setup, :serving_mode, context.mode)
      port = start_public_endpoint!()
      thread = Ecto.UUID.generate()
      input = native_text_input("synthetic")
      original = if context.transport == :http, do: payload(setup, thread, input, 0), else: websocket_payload(setup, thread, input, context.mode)
      assert {:incomplete, first} = credential_turn!(context.transport, port, setup, thread, original)
      bound_epoch = Repo.get!(UpstreamIdentity, setup.identity.id).metadata["credential_epoch"] || 1
      change_credential!(context.change, setup)
      assert Repo.get!(UpstreamIdentity, setup.identity.id).metadata["credential_epoch"] > bound_epoch
      outcome = credential_turn!(context.transport, port, setup, thread, Map.put(original, "input", input ++ [guidance()]))

      case context.change do
        :scheduled_refresh ->
          assert {:completed, _served} = outcome
          assert FakeUpstream.count(upstream) == 3
          assert_refreshed_retry_served!(setup, first)

        :reimport ->
          assert {:duplicate_turn, _row} = outcome
          assert FakeUpstream.count(upstream) == 1
          assert [%Request{id: id}] = Repo.all(from r in Request, where: r.pool_id == ^setup.pool.id)
          assert id == first.id
          refute Repo.exists?(from l in RequestClientRetryLink, where: l.predecessor_request_id == ^first.id)
      end
    end
  end

  # With owner forwarding on, the socket hands its turns to an owner session that outlives the test. Stop the Pool's owners at exit before its rows go: registered after `register_unboxed_pool_cleanup!/1`, the stop runs before that cleanup, and always before the sandbox owner stops.
  defp stop_owner_sessions_on_exit(setup), do: on_exit(fn -> for id <- Repo.all(from(s in CodexSession, where: s.pool_id == ^setup.pool.id, select: s.id)), do: stop_websocket_owner_session(id) end)

  defp credential_reply(transport, event, close \\ :keep)
  defp credential_reply(:http, data, _close), do: FakeUpstream.raw_response(event(data), headers: [{"content-type", "text/event-stream"}])
  # The provider drops the connection after the terminal, so the guided retry needs a new handshake with or without an owner session.
  defp credential_reply(_websocket, data, :then_close), do: FakeUpstream.websocket_text_frames_then_abrupt_close([CodexPooler.JSON.encode!(data)])
  defp credential_reply(_websocket, data, :keep), do: FakeUpstream.websocket_text_frames([CodexPooler.JSON.encode!(data)])

  # One request over the transport under test: its outcome and the settled request row.
  defp credential_turn!(:http, port, setup, thread, payload) do
    {status, body} = post(port, setup, payload, thread)
    row = await_latest_settled(setup, System.monotonic_time(:millisecond) + @budget)

    cond do
      status == 200 and body =~ "response.incomplete" -> {:incomplete, row}
      status == 200 and body =~ "response.completed" -> {:completed, row}
      status == 409 -> {CodexPooler.JSON.decode!(body)["error"]["code"] |> String.to_existing_atom(), row}
      true -> {{status, body}, row}
    end
  end

  defp credential_turn!(_websocket, port, setup, thread, payload) do
    {row, frame} = websocket_turn!(port, setup, thread, payload)

    case frame do
      %{"type" => "response.incomplete"} -> {:incomplete, row}
      %{"type" => "response.completed"} -> {:completed, row}
      %{"type" => "error", "status" => 409, "error" => %{"code" => "duplicate_turn"}} -> {:duplicate_turn, row}
      other -> {other, row}
    end
  end

  # The scheduled refresh: the job's own code, through the provider's token endpoint.
  defp change_credential!(:scheduled_refresh, setup),
    do: assert(:ok = TokenRefreshWorker.perform(%Oban.Job{args: %{"upstream_identity_id" => setup.identity.id, "trigger_kind" => "scheduled"}}))

  # An operator's Codex `auth.json` import of the same provider account into its identity.
  defp change_credential!(:reimport, setup) do
    %{user: owner} = CodexPooler.AccountsFixtures.bootstrap_owner_fixture()
    account_id = Repo.get!(UpstreamIdentity, setup.identity.id).chatgpt_account_id
    jwt = fn claims -> Enum.map_join([%{"alg" => "none", "typ" => "JWT"}, claims, "synthetic-signature"], ".", &Base.url_encode64(CodexPooler.JSON.encode!(&1), padding: false)) end
    id_token = jwt.(%{"email" => "synthetic@example.com", "https://api.openai.com/auth" => %{"chatgpt_account_id" => account_id, "chatgpt_user_id" => "user_synthetic", "chatgpt_plan_type" => "pro"}})
    access_token = jwt.(%{"exp" => DateTime.utc_now() |> DateTime.add(3600) |> DateTime.to_unix()})
    auth = CodexPooler.JSON.encode!(%{"auth_mode" => "chatgpt", "tokens" => %{"id_token" => id_token, "access_token" => access_token, "refresh_token" => "synthetic-reimported-refresh-token", "account_id" => account_id}})
    assert {:ok, %{status: :existing, identity: %{id: identity_id}}} = Upstreams.import_codex_auth_json(Scope.for_user(owner), setup.pool, auth)
    assert identity_id == setup.identity.id
  end

  # The token refresh advanced the account's credential epoch past the one the retry's binding recorded at the content-filter terminal.
  defp assert_epoch_advanced_past_binding!(setup, successor) do
    assert %{"credential_epoch" => bound} = successor.request_metadata["native_content_filter_binding"]
    assert Repo.get!(UpstreamIdentity, setup.identity.id).metadata["credential_epoch"] > bound
  end

  # The guided retry, linked to the content-filtered request, succeeded on the account's one assignment after its first attempt met the 401.
  defp assert_refreshed_retry_served!(setup, first) do
    [served] = Repo.all(from r in Request, where: r.pool_id == ^setup.pool.id and r.id != ^first.id)
    assert %Request{status: "succeeded"} = served
    assert [served.id] == Repo.all(from l in RequestClientRetryLink, where: l.predecessor_request_id == ^first.id, select: l.successor_request_id)
    assert [%Attempt{status: "succeeded", pool_upstream_assignment_id: assignment_id} | _] = Repo.all(from a in Attempt, where: a.request_id == ^served.id, order_by: [desc: a.attempt_number])
    assert assignment_id == setup.assignment.id
  end

  defp websocket_payload(setup, thread, input, mode) do
    original = payload(setup, thread, input, 0) |> Map.put("type", "response.create")
    if mode == "lite", do: put_in(original, ["client_metadata", "ws_request_header_x_openai_internal_codex_responses_lite"], "true"), else: original
  end

  # One request on a fresh downstream connection, closed once its terminal or error frame arrives: the settled request row and that frame.
  defp websocket_turn!(port, setup, thread, payload) do
    {conn, websocket, ref} = public_websocket_connect!(port, setup, thread)
    on_exit(fn -> Mint.HTTP.close(conn) end)
    {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, CodexPooler.JSON.encode!(payload))
    {conn, _websocket, frame} = receive_terminal(conn, websocket, ref)
    Mint.HTTP.close(conn)
    {await_latest_settled(setup, System.monotonic_time(:millisecond) + @budget), frame}
  end

  # The one admitted guided retry that succeeded, linked to the content-filtered request and served on `assignment`.
  defp assert_served_on!(setup, first, assignment, deadline \\ nil) do
    deadline = deadline || System.monotonic_time(:millisecond) + @budget

    case Repo.all(from r in Request, where: r.pool_id == ^setup.pool.id and r.id != ^first.id and r.status == "succeeded" and not is_nil(r.completed_at)) do
      [served] ->
        assert [%Attempt{status: "succeeded", pool_upstream_assignment_id: assignment_id}] = Repo.all(from a in Attempt, where: a.request_id == ^served.id)
        assert assignment_id == assignment.id
        assert [served.id] == Repo.all(from l in RequestClientRetryLink, where: l.predecessor_request_id == ^first.id, select: l.successor_request_id)

      [] ->
        assert System.monotonic_time(:millisecond) < deadline, "guided retry never settled"
        Process.sleep(10)
        assert_served_on!(setup, first, assignment, deadline)
    end
  end

  # The one request linked to the failed guided retry succeeded on the guided retry's account, with the inherited routing-only pin and no binding.
  defp assert_pinned_retry_served!(setup, guided, deadline \\ nil) do
    deadline = deadline || System.monotonic_time(:millisecond) + @budget

    case Repo.all(from r in Request, join: l in RequestClientRetryLink, on: l.successor_request_id == r.id, where: l.predecessor_request_id == ^guided.id, select: r) do
      [%Request{status: "succeeded", completed_at: %DateTime{}} = served] ->
        assert served.request_metadata["native_content_filter_pin"] == %{"version" => 1, "assignment_id" => setup.assignment.id, "identity_id" => setup.identity.id}
        refute Map.has_key?(served.request_metadata, "native_content_filter_binding")
        assert [%Attempt{status: "succeeded", pool_upstream_assignment_id: assignment_id}] = Repo.all(from a in Attempt, where: a.request_id == ^served.id)
        assert assignment_id == setup.assignment.id

      _pending ->
        assert System.monotonic_time(:millisecond) < deadline, "pinned retry never settled"
        Process.sleep(10)
        assert_pinned_retry_served!(setup, guided, deadline)
    end
  end

  # The refused retry kept its accounting rows but gave up its claim and its link, so the content-filtered request names no successor.
  defp assert_claim_released!(first, refused) do
    refused = Repo.reload!(refused)
    assert is_binary(refused.request_metadata["released_turn_claim"])
    assert refused.correlation_id != refused.request_metadata["released_turn_claim"]
    refute Repo.exists?(from l in RequestClientRetryLink, where: l.predecessor_request_id == ^first.id or l.successor_request_id == ^refused.id)
  end

  defp ledger_entries(request), do: Enum.sort(Repo.all(from l in LedgerEntry, where: l.request_id == ^request.id, select: {l.entry_kind, l.attempt_id, fragment("?->>'pre_attempt_phase'", l.details)}))

  defp move_affinity!(%Request{request_metadata: %{"codex_session_id" => session_id}}, assignment) do
    {1, _} = Repo.update_all(from(session in CodexSession, where: session.id == ^session_id), set: [pool_upstream_assignment_id: assignment.id])
    :ok
  end

  defp open_circuit!(setup, route_class) do
    now = DateTime.utc_now() |> DateTime.truncate(:microsecond)
    Repo.insert!(%RoutingCircuitState{pool_id: setup.pool.id, pool_upstream_assignment_id: setup.assignment.id, upstream_identity_id: setup.identity.id, model_identifier: setup.model.exposed_model_id, route_class: route_class, status: "open", reason_code: "upstream_network_error", failure_count: 3, success_count: 0, opened_at: now, next_probe_at: DateTime.add(now, 30, :second), metadata: %{"probe_in_flight_count" => 0}, created_at: now, updated_at: now})
  end

  defp req_post!(port, setup, payload, thread) do
    metadata = payload["client_metadata"]["x-codex-turn-metadata"]
    window = CodexPooler.JSON.decode!(metadata)["window_number"]
    headers = [{"authorization", setup.authorization}, {"content-type", "application/json"}, {"session-id", thread}, {"thread-id", thread}, {"x-codex-window-id", "#{thread}:#{window}"}, {"x-codex-turn-metadata", metadata}, {"originator", "codex_cli_rs"}]
    headers = if setup.serving_mode == "lite", do: [{"x-openai-internal-codex-responses-lite", "true"} | headers], else: headers
    Req.post!("http://127.0.0.1:#{port}#{@path}", headers: headers, json: payload, retry: false, receive_timeout: @budget)
  end

  defp assert_refused_before_linking!(setup, upstream, first) do
    assert FakeUpstream.count(upstream) == 1
    assert [%Request{id: id}] = Repo.all(from r in Request, where: r.pool_id == ^setup.pool.id)
    assert id == first.id
    refute Repo.exists?(from l in RequestClientRetryLink, where: l.predecessor_request_id == ^first.id)
  end

  # A second active account of the Pool serving the same model, primed for routing.
  defp add_pool_account!(setup, upstream) do
    account = gateway_upstream(setup.pool, upstream, "upstream-token-other", compact?: true)
    prime_routing_quota!(account.identity)
    source = setup.model.metadata["source_assignment_models"][setup.assignment.id]
    metadata = setup.model.metadata |> Map.update!("source_assignment_ids", &(&1 ++ [account.assignment.id])) |> put_in(["source_assignment_models", account.assignment.id], source)
    setup.model |> Ecto.Changeset.change(metadata: metadata) |> Repo.update!()
    account
  end

  for mode <- ["full", "lite"], retained? <- [false, true], resume? <- [false, true] do
    @tag mode: mode, retained?: retained?, resume?: resume?
    test "#{mode} content-filter retry retains complete output #{retained?} after compaction #{resume?}", context do
      CodexPooler.DataCase.stop_sandbox(context.sandbox_owner, context.sandbox_settings_cache)
      on_exit(fn -> Sandbox.mode(Repo, :manual) end)
      :ok = Sandbox.mode(Repo, :auto)
      output = if context.retained?, do: [%{"type" => "reasoning", "id" => "rs_synthetic", "summary" => [], "encrypted_content" => "synthetic"}], else: []
      terminal = %{"type" => "response.incomplete", "response" => %{"id" => "resp_synthetic_cf", "status" => "incomplete", "incomplete_details" => %{"reason" => "content_filter"}, "output" => output, "usage" => %{"input_tokens" => 10, "output_tokens" => 1, "total_tokens" => 11}}}
      completed = %{"type" => "response.completed", "response" => %{"id" => "resp_synthetic_complete", "status" => "completed", "output" => [], "usage" => %{"input_tokens" => 10, "output_tokens" => 1, "total_tokens" => 11}}}
      chunk = Enum.map_join(output, &event(%{"type" => "response.output_item.done", "item" => &1})) <> event(terminal)
      upstream = start_upstream(FakeUpstream.strict_sequence([FakeUpstream.raw_response(chunk, headers: [{"content-type", "text/event-stream"}]), FakeUpstream.raw_response(event(completed), headers: [{"content-type", "text/event-stream"}])]))
      setup = gateway_setup(upstream, compact?: true)
      register_unboxed_pool_cleanup!(setup)
      set_model_serving_mode!(model_serving_scope(), setup, context.mode)
      setup = Map.put(setup, :serving_mode, context.mode)
      port = start_public_endpoint!()
      thread = Ecto.UUID.generate()
      input = native_text_input("synthetic")
      input = if context.resume?, do: input ++ [%{"type" => "compaction", "encrypted_content" => "synthetic-compaction"}], else: input
      original = payload(setup, thread, input, 0)
      assert {200, _} = post(port, setup, original, thread)
      first = await_latest_settled(setup, System.monotonic_time(:millisecond) + @budget)
      assert first.status == "succeeded"
      guidance = guidance()
      successor = Map.put(original, "input", input ++ output ++ [guidance])

      for changed <- [Map.put(successor, "instructions", "synthetic changed options"), put_in(successor, ["input", Access.at(0), "content"], [%{"type" => "input_text", "text" => "synthetic changed original input"}])] do
        assert {409, _} = post(port, setup, changed, thread)
        assert FakeUpstream.count(upstream) == 1
      end

      first_attempt = Repo.get_by!(Attempt, request_id: first.id)
      setup.model |> Ecto.Changeset.change(upstream_model_id: "synthetic-changed-mapping") |> Repo.update!()
      assert {409, _} = post(port, setup, successor, thread)
      assert FakeUpstream.count(upstream) == 1
      Repo.get!(CodexPooler.Catalog.Model, setup.model.id) |> Ecto.Changeset.change(upstream_model_id: setup.model.upstream_model_id) |> Repo.update!()

      first |> Ecto.Changeset.change(completed_at: DateTime.add(first.completed_at, -31, :second)) |> Repo.update!()
      assert {409, _} = post(port, setup, successor, thread)
      Repo.get!(Request, first.id) |> Ecto.Changeset.change(completed_at: first.completed_at) |> Repo.update!()
      identity = Repo.get!(UpstreamIdentity, setup.identity.id)
      identity |> Ecto.Changeset.change(metadata: Map.put(identity.metadata, "credential_epoch", 2)) |> Repo.update!()
      assert {409, _} = post(port, setup, successor, thread)
      Repo.get!(UpstreamIdentity, identity.id) |> Ecto.Changeset.change(metadata: identity.metadata) |> Repo.update!()
      key = Repo.get!(CodexPooler.Access.APIKey, setup.api_key.id)
      key |> Ecto.Changeset.change(runtime_revocation_epoch: key.runtime_revocation_epoch + 1) |> Repo.update!()
      {status, _} = post(port, setup, successor, thread)
      assert status in [401, 403, 409]
      Repo.get!(CodexPooler.Access.APIKey, key.id) |> Ecto.Changeset.change(runtime_revocation_epoch: key.runtime_revocation_epoch) |> Repo.update!()
      assert FakeUpstream.count(upstream) == 1

      for mutation <- [:marker_missing, :marker_version, :reason, :delivery, :output_missing, :output_saturated, :attempt_source, :poisoned] do
        metadata = corrupt_proof(first_attempt.response_metadata, mutation)
        first_attempt |> Ecto.Changeset.change(response_metadata: metadata) |> Repo.update!()
        assert {409, _} = post(port, setup, successor, thread)
        assert FakeUpstream.count(upstream) == 1
        assert Repo.aggregate(from(r in Request, where: r.pool_id == ^setup.pool.id), :count) == 1
        Repo.get!(Attempt, first_attempt.id) |> Ecto.Changeset.change(response_metadata: first_attempt.response_metadata) |> Repo.update!()
      end

      if context.mode == "full" and not context.retained? do
        race_successor(port, setup, successor, thread, first.request_metadata["codex_session_id"])
      else
        assert {200, _} = post(port, setup, successor, thread)
      end

      assert FakeUpstream.count(upstream) == 2
      requests = Repo.all(from r in Request, where: r.pool_id == ^setup.pool.id)
      assert length(requests) == 2
      admitted = Enum.find(requests, &(&1.id != first.id))
      binding = admitted.request_metadata["native_content_filter_binding"]
      scope = %{assignment_id: binding["assignment_id"], identity_id: binding["identity_id"], credential_epoch: binding["credential_epoch"], serving_mode: binding["serving_mode"], effective_model: binding["effective_model"], upstream_model: binding["upstream_model"]}
      assert NativeContentFilterRetry.dispatch_allowed?(admitted, scope)

      for key <- [:assignment_id, :identity_id, :credential_epoch, :serving_mode, :effective_model, :upstream_model] do
        value = if key == :credential_epoch, do: 2, else: "changed"
        refute NativeContentFilterRetry.dispatch_allowed?(admitted, Map.put(scope, key, value))
      end

      stripped = admitted |> Ecto.Changeset.change(request_metadata: Map.delete(admitted.request_metadata, "native_content_filter_binding")) |> Repo.update!()
      refute NativeContentFilterRetry.dispatch_allowed?(stripped, scope)
      Repo.get!(Request, admitted.id) |> Ecto.Changeset.change(request_metadata: admitted.request_metadata) |> Repo.update!()

      for request <- requests do
        assert Repo.aggregate(from(a in Attempt, where: a.request_id == ^request.id), :count) == 1
        assert Repo.aggregate(from(l in CodexPooler.Accounting.LedgerEntry, where: l.request_id == ^request.id and l.entry_kind == "settlement"), :count) == 1
      end
    end
  end

  defp corrupt_proof(metadata, :marker_missing), do: Map.delete(metadata, "native_content_filter_terminal")
  defp corrupt_proof(metadata, :marker_version), do: put_in(metadata, ["native_content_filter_terminal", "version"], 2)
  defp corrupt_proof(metadata, :reason), do: put_in(metadata, ["native_content_filter_terminal", "reason"], "interrupted")
  defp corrupt_proof(metadata, :delivery), do: put_in(metadata, ["downstream_delivery", "outcome"], "aborted")
  defp corrupt_proof(metadata, :output_missing), do: Map.delete(metadata, "native_http_resume_progress")
  defp corrupt_proof(metadata, :output_saturated), do: put_in(metadata, ["native_client_retry_observation", "output_item_done_count_saturated"], true)
  defp corrupt_proof(metadata, :attempt_source), do: put_in(metadata, ["native_content_filter_source", "attempt_id"], Ecto.UUID.generate())
  defp corrupt_proof(metadata, :poisoned), do: Map.put(metadata, "native_client_retry_authority_loss", %{"version" => 1, "authority_lost_reason" => "malformed_event"})

  defp race_successor(port, setup, payload, thread, session_id) do
    parent = self()

    holder =
      Task.async(fn ->
        Repo.transaction(fn ->
          Repo.query!("SELECT id FROM codex_sessions WHERE id = $1 FOR UPDATE", [Ecto.UUID.dump!(session_id)])
          %{rows: [[backend]]} = Repo.query!("SELECT pg_backend_pid()")
          send(parent, {:session_locked, backend})

          receive do
            :release -> :ok
          after
            2 * @budget -> raise "holder release missing"
          end
        end)
      end)

    assert_receive {:session_locked, holder_backend}, @budget
    headers = [{"authorization", setup.authorization}, {"content-type", "application/json"}, {"session-id", thread}, {"thread-id", thread}, {"x-codex-window-id", "#{thread}:0"}, {"x-codex-turn-metadata", payload["client_metadata"]["x-codex-turn-metadata"]}, {"originator", "codex_cli_rs"}]
    contenders = for _ <- 1..2, do: Task.async(fn -> Req.post!("http://127.0.0.1:#{port}#{@path}", headers: headers, json: payload, retry: false, receive_timeout: @budget).status end)

    try do
      backends = await_blocked_backends(holder_backend, System.monotonic_time(:millisecond) + @budget)
      assert length(Enum.uniq(backends)) == 2
      assert holder_backend not in backends
      send(holder.pid, :release)
      assert {:ok, :ok} = Task.await(holder, @budget)
      assert Enum.sort(Enum.map(contenders, &Task.await(&1, @budget))) == [200, 409]
      assert {409, _} = post(port, setup, payload, thread)
    after
      send(holder.pid, :release)
      Task.shutdown(holder, :brutal_kill)
      Enum.each(contenders, &Task.shutdown(&1, :brutal_kill))
    end
  end

  defp await_blocked_backends(holder, deadline) do
    %{rows: rows} = Repo.query!("WITH RECURSIVE blocked(pid) AS (SELECT a.pid FROM pg_stat_activity a WHERE $1 = ANY(pg_blocking_pids(a.pid)) UNION SELECT a.pid FROM pg_stat_activity a JOIN blocked b ON b.pid = ANY(pg_blocking_pids(a.pid))) SELECT DISTINCT b.pid FROM blocked b JOIN pg_locks l ON l.pid = b.pid WHERE l.relation = 'codex_sessions'::regclass", [holder])

    cond do
      length(rows) == 2 ->
        List.flatten(rows)

      System.monotonic_time(:millisecond) > deadline ->
        flunk("two independent PostgreSQL contenders did not reach the held session")

      true ->
        Process.sleep(10)
        await_blocked_backends(holder, deadline)
    end
  end

  defp guidance, do: %{"type" => "message", "role" => "developer", "content" => [%{"type" => "input_text", "text" => "<content_filter_guidance>\nsynthetic guidance\n</content_filter_guidance>"}]}

  defp receive_terminal(conn, websocket, ref) do
    {conn, websocket, frame} = public_websocket_receive_text!(conn, websocket, ref)

    case CodexPooler.JSON.decode!(frame) do
      %{"type" => type} = terminal when type in ["response.completed", "response.incomplete", "response.failed", "error"] -> {conn, websocket, terminal}
      _progress -> receive_terminal(conn, websocket, ref)
    end
  end

  # Runs in the socket's connection process right after Bandit wrote a frame (ThousandIsland reports each successful write synchronously, in that process): the frame that carries the content-filter terminal parks the process until the test releases it, or until the test is gone. The socket is the one process of the listener subscribed to its Pool's events. With `hold_after_terminal_write!/1` the write watch's own send handler, attached when the application boots, must read the write's driver queue before this one parks the process, so the arm holds the race between that reading and the socket's next callback; `hold_before_write_watch_read!/1` puts the watch's handler behind this one, so the watch reads only after the client's close took the port (findings#315). Telemetry calls handlers in attachment order without promising it (`:telemetry.persist/0` reverses it), so each hold asserts its order, which `:telemetry.list_handlers/1` reports as dispatched.
  def park_after_terminal_write(_event, %{data: data}, _metadata, %{test: test, topic: topic, hold: hold}) do
    if String.contains?(IO.iodata_to_binary(data), "response.incomplete") and List.keymember?(Registry.lookup(CodexPooler.PubSub, topic), self(), 0) do
      test_monitor = Process.monitor(test)
      send(test, {:terminal_written, hold, self()})

      receive do
        {^hold, :release} -> :ok
        {:DOWN, ^test_monitor, :process, ^test, _reason} -> :ok
      end

      Process.demonitor(test_monitor, [:flush])
    end

    :ok
  end

  defp hold_after_terminal_write!(setup) do
    hold = make_ref()
    handler_id = {__MODULE__, hold}
    on_exit(fn -> :telemetry.detach(handler_id) end)
    config = %{test: self(), topic: CodexPooler.Events.pubsub_topic(setup.pool.id, "pools"), hold: hold}
    :ok = :telemetry.attach(handler_id, [:thousand_island, :connection, :send], &__MODULE__.park_after_terminal_write/4, config)
    watch = {CodexPoolerWeb.WebsocketDownstreamWriteWatch, :send}
    assert [^watch, ^handler_id] = for(%{id: id} <- :telemetry.list_handlers([:thousand_island, :connection, :send]), id in [watch, handler_id], do: id)
    hold
  end

  # Telemetry has no handler priority, so the write watch's send handler is detached and attached again behind the hold; the module is synchronous, so no other test writes in between, and the watch stays attached when the test ends.
  defp hold_before_write_watch_read!(setup) do
    hold = make_ref()
    handler_id = {__MODULE__, hold}
    watch = {CodexPoolerWeb.WebsocketDownstreamWriteWatch, :send}

    on_exit(fn ->
      :telemetry.detach(handler_id)
      :ok = CodexPoolerWeb.WebsocketDownstreamWriteWatch.attach()
    end)

    config = %{test: self(), topic: CodexPooler.Events.pubsub_topic(setup.pool.id, "pools"), hold: hold}
    :ok = :telemetry.detach(watch)
    :ok = :telemetry.attach(handler_id, [:thousand_island, :connection, :send], &__MODULE__.park_after_terminal_write/4, config)
    :ok = CodexPoolerWeb.WebsocketDownstreamWriteWatch.attach()
    assert [^handler_id, ^watch] = for(%{id: id} <- :telemetry.list_handlers([:thousand_island, :connection, :send]), id in [watch, handler_id], do: id)
    hold
  end

  defp connection_port!(socket) do
    {:links, links} = Process.info(socket, :links)
    Enum.find(links, &(is_port(&1) and Port.info(&1, :name) == {:name, ~c"tcp_inet"})) || flunk("the websocket connection process owns no TCP port")
  end

  defp await_delivery(%Request{id: request_id} = selected_request, deadline, observed_terminal) do
    attempt = Repo.get_by!(Attempt, request_id: request_id)

    cond do
      get_in(attempt.response_metadata, ["downstream_delivery", "outcome"]) == "delivered" ->
        :ok

      System.monotonic_time(:millisecond) > deadline ->
        request = Repo.get!(Request, request_id)
        metadata = attempt.response_metadata || %{}
        receipt = metadata["downstream_delivery"] || %{}
        source = metadata["native_content_filter_source"] || %{}
        latest_attempt = Repo.one(from a in Attempt, where: a.request_id == ^request_id, order_by: [desc: a.attempt_number], limit: 1)
        pool_request_count = Repo.aggregate(from(r in Request, where: r.pool_id == ^selected_request.pool_id), :count)
        flunk("delivery receipt not delivered: " <> CodexPooler.JSON.encode!(%{observed_terminal: observed_terminal, selected_request_completed_at: selected_request.completed_at, pool_request_count: pool_request_count, attempt_request_match: attempt.request_id == request_id, attempt_number: attempt.attempt_number, latest_attempt_match: latest_attempt && latest_attempt.id == attempt.id, request_id: request_id, request_status: request.status, request_error: request.last_error_code, attempt_id: attempt.id, attempt_status: attempt.status, replay_generation: attempt.replay_generation, receipt_present: Map.has_key?(metadata, "downstream_delivery"), outcome: receipt["outcome"], terminal_class: receipt["terminal_class"], highest_frame_class: receipt["highest_frame_class"], incomplete_reason: receipt["incomplete_reason"], completed_items: receipt["completed_items"], digest_count: length(receipt["completed_item_digests"] || []), write_failure: receipt["write_failure"], source_attempt_match: source["attempt_id"] == attempt.id, marker_reason: get_in(metadata, ["native_content_filter_terminal", "reason"])}))

      true ->
        Process.sleep(10)
        await_delivery(selected_request, deadline, observed_terminal)
    end
  end

  defp payload(setup, thread, input, window, turn_id \\ "synthetic_turn") do
    %{"model" => setup.model.exposed_model_id, "input" => input, "stream" => true, "store" => false, "client_metadata" => %{"x-codex-turn-metadata" => CodexPooler.JSON.encode!(%{"thread_id" => thread, "session_id" => thread, "turn_id" => turn_id, "request_kind" => "turn", "agent_name" => "/root", "window_id" => "#{thread}:#{window}", "window_number" => window})}}
  end

  defp event(data), do: "event: #{data["type"]}\ndata: " <> CodexPooler.JSON.encode!(data) <> "\n\n"

  defp start_request(port, setup, payload, thread) do
    {:ok, conn} = Mint.HTTP.connect(:http, "127.0.0.1", port, mode: :passive)
    on_exit(fn -> Mint.HTTP.close(conn) end)
    metadata = payload["client_metadata"]["x-codex-turn-metadata"]
    window = CodexPooler.JSON.decode!(metadata)["window_number"]
    headers = [{"authorization", setup.authorization}, {"content-type", "application/json"}, {"session-id", thread}, {"thread-id", thread}, {"x-codex-window-id", "#{thread}:#{window}"}, {"x-codex-turn-metadata", metadata}, {"originator", "codex_cli_rs"}]
    headers = if setup.serving_mode == "lite", do: [{"x-openai-internal-codex-responses-lite", "true"} | headers], else: headers
    {:ok, conn, ref} = Mint.HTTP.request(conn, "POST", @path, headers, CodexPooler.JSON.encode!(payload))
    {conn, ref}
  end

  defp post(port, setup, payload, thread) do
    {conn, ref} = start_request(port, setup, payload, thread)

    try do
      all(conn, ref, nil, "")
    after
      Mint.HTTP.close(conn)
    end
  end

  defp all(conn, ref, status, body) do
    assert {:ok, conn, responses} = Mint.HTTP.recv(conn, 0, @budget)

    {status, body, done} =
      Enum.reduce(responses, {status, body, false}, fn
        {:status, ^ref, s}, {_, b, d} -> {s, b, d}
        {:data, ^ref, data}, {s, b, d} -> {s, b <> data, d}
        {:done, ^ref}, {s, b, _} -> {s, b, true}
        _, a -> a
      end)

    if done, do: {status, body}, else: all(conn, ref, status, body)
  end

  defp await_latest_settled(setup, deadline) do
    row = Repo.one(from r in Request, where: r.pool_id == ^setup.pool.id, order_by: [desc: r.admitted_at], limit: 1)

    cond do
      row && row.completed_at ->
        row

      System.monotonic_time(:millisecond) > deadline ->
        flunk("request never finalized")

      true ->
        Process.sleep(10)
        await_latest_settled(setup, deadline)
    end
  end
end
