defmodule CodexPooler.Gateway.Transports.Websocket.UpstreamWebsocketSessionRetirementTest do
  use CodexPooler.DataCase, async: false
  alias CodexPooler.Gateway.Transports.Websocket.UpstreamWebsocketSession
  alias CodexPooler.Gateway.Transports.Websocket.UpstreamWebsocketSession.{ConnectionUpgrade, Request}
  alias CodexPooler.Platform.OutboundHTTP
  alias CodexPooler.UpstreamWebsocketRetirementPeer, as: Peer
  alias CodexPoolerWeb.Runtime.BackendCodexTestSupport, as: SocketSupport
  @moduletag capture_log: true
  @budget 15_000

  setup do
    CodexPooler.TestAppEnv.restore_on_exit(OutboundHTTP)
    Application.put_env(:codex_pooler, OutboundHTTP, proxy_config: %{http: [], https: [], no_proxy: []})
    certificate = Peer.trust!()
    patterns = [{ConnectionUpgrade, :await_upgrade, 4}, {UpstreamWebsocketSession, :send_text, 2}, {Mint.HTTP, :close, 1}, {UpstreamWebsocketSession, :retire_socket, 1}, {:gen_tcp, :send, 2}, {:ssl, :send, 2}]
    on_exit(fn -> Enum.each(patterns, &:erlang.trace_pattern(&1, false, [:local])) end)
    Enum.each([ConnectionUpgrade, UpstreamWebsocketSession, Mint.HTTP, :ssl, :gen_tcp], &Code.ensure_loaded!/1)
    :erlang.trace_pattern({ConnectionUpgrade, :await_upgrade, 4}, [{[:"$1", :_, :_, :_], [], [{:message, {:map_get, :socket, :"$1"}}]}], [:local])
    :erlang.trace_pattern({UpstreamWebsocketSession, :send_text, 2}, true, [:local])
    :erlang.trace_pattern({Mint.HTTP, :close, 1}, true, [:local])
    :erlang.trace_pattern({UpstreamWebsocketSession, :retire_socket, 1}, true, [:local])
    for transport <- [:gen_tcp, :ssl], do: :erlang.trace_pattern({transport, :send, 2}, [{:_, [], [{:return_trace}]}], [:local])
    certificate
  end

  for transport <- [:ws, :wss], bytes <- [1024, 16 * 1024 * 1024] do
    @tag transport: transport, retirement_timeout: true
    test "#{transport} #{bytes} byte request retires at its receive timeout and serves next request", context do
      large? = unquote(bytes) > 1024
      peer = Peer.start!(context, [if(large?, do: :stalled, else: :small_silent), :success])
      session = start_session!()
      request = request(peer.url, unquote(bytes))
      task = Task.Supervisor.async_nolink(start_supervised!(Task.Supervisor), fn -> UpstreamWebsocketSession.request(session, request) end)
      assert_receive {:peer_upgraded, ref, 1, holder}, @budget
      assert ref == peer.ref
      holder_monitor = Process.monitor(holder)
      on_exit(fn -> send(holder, :release_peer) end)
      assert_receive {:trace_ts, ^session, :call, {ConnectionUpgrade, :await_upgrade, 4}, socket, _time}, @budget
      assert_receive {:trace_ts, ^session, :call, {UpstreamWebsocketSession, :send_text, 2}, send_at}, @budget
      sender = if context.transport == :ws, do: :gen_tcp, else: :ssl
      assert_receive {:trace_ts, ^session, :return_from, {^sender, :send, 2}, :ok, sent_at} when sent_at >= send_at, @budget
      stats = if context.transport == :ws, do: :inet.getstat(socket, [:send_pend]), else: :ssl.getstat(socket, [:send_pend])
      assert {:ok, [send_pend: queued]} = stats
      if large?, do: assert(queued > 0)
      options = socket_options(context.transport, socket)
      monitors = monitor_resources(socket)
      result = Task.yield(task, 1_500)
      observed_at = System.monotonic_time()

      retirement_started_at =
        receive do
          {:trace_ts, ^session, :call, {UpstreamWebsocketSession, :retire_socket, 1}, at} -> at
          {:trace_ts, ^session, :call, {Mint.HTTP, :close, 1}, at} -> at
        after
          0 -> nil
        end

      closed = resources_closed?(socket)

      if result == nil do
        send(holder, :release_peer)
        assert {:error, %{reason: :upstream_websocket_receive_timeout}} = Task.await(task, @budget)
      end

      CodexPooler.TestDiagnostics.puts(fn -> CodexPooler.JSON.encode!(%{scenario: "queued_retirement", transport: context.transport, payload_bytes: byte_size(request.payload), queued_bytes: queued, send_elapsed_ms: elapsed(send_at, sent_at), reply_observed: result != nil, retirement_after_send_ms: if(retirement_started_at, do: elapsed(sent_at, retirement_started_at)), observation_after_send_ms: elapsed(sent_at, observed_at), socket_closed_at_reply_observation: closed}) end)
      assert {:ok, {:error, %{reason: :upstream_websocket_receive_timeout}}} = result
      await_resources_down!(monitors)
      assert resources_closed?(socket)
      retired_after_send_ms = elapsed(sent_at, System.monotonic_time())
      assert retired_after_send_ms < 1_500
      assert Process.alive?(session)
      assert {:ok, %{terminal: "response.completed"}} = bounded_request!(session, request(peer.url, 1024))
      CodexPooler.TestDiagnostics.puts(fn -> CodexPooler.JSON.encode!(%{scenario: "retirement_completed", transport: context.transport, payload_bytes: byte_size(request.payload), old_resources_down_before_peer_release: true, retirement_complete_after_send_ms: retired_after_send_ms, successor_complete_after_send_ms: elapsed(sent_at, System.monotonic_time()), background_closers_created: 0}) end)
      successor = Mint.HTTP.get_socket(:sys.get_state(session).conn)
      assert socket_options(context.transport, successor) == options
      assert_receive {:peer_upgraded, ^ref, 2, _}, @budget
      send(session, {if(context.transport == :ws, do: :tcp_closed, else: :ssl_closed), socket})
      assert Mint.HTTP.get_socket(:sys.get_state(session).conn) == successor
      send(holder, :release_peer)
      assert :ok = Peer.await_closed!(peer, 1, holder, holder_monitor, @budget)
      assert {:ok, %{terminal: "response.completed", upstream_websocket_connection: %{reused: true}}} = bounded_request!(session, request(peer.url, 1024))
    end
  end

  for transport <- [:ws, :wss], ending <- [:caller_death, :idle_invalidation] do
    @tag transport: transport
    test "#{transport} queued output retires on #{ending} without blocking the successor", context do
      script = if unquote(ending) == :idle_invalidation, do: :early_terminal, else: :stalled
      peer = Peer.start!(context, [script, :success])
      session = start_session!()
      request = %{request(peer.url, 16 * 1024 * 1024) | timeouts: %{connect_timeout_ms: 5_000, receive_timeout_ms: 10_000}}
      task = Task.Supervisor.async_nolink(start_supervised!(Task.Supervisor), fn -> UpstreamWebsocketSession.request(session, request) end)
      assert_receive {:peer_upgraded, ref, 1, holder}, @budget
      assert ref == peer.ref
      holder_monitor = Process.monitor(holder)
      on_exit(fn -> send(holder, :release_peer) end)
      assert_receive {:trace_ts, ^session, :call, {ConnectionUpgrade, :await_upgrade, 4}, socket, _}, @budget
      assert_receive {:trace_ts, ^session, :call, {UpstreamWebsocketSession, :send_text, 2}, begin_at}, @budget
      sender = if context.transport == :ws, do: :gen_tcp, else: :ssl
      assert_receive {:trace_ts, ^session, :return_from, {^sender, :send, 2}, :ok, at} when at >= begin_at, @budget
      assert {:ok, [send_pend: queued]} = if(context.transport == :ws, do: :inet.getstat(socket, [:send_pend]), else: :ssl.getstat(socket, [:send_pend]))
      assert queued > 0
      monitors = monitor_resources(socket)
      started = System.monotonic_time()

      case unquote(ending) do
        :caller_death ->
          Task.shutdown(task, :brutal_kill)

        :idle_invalidation ->
          assert {:ok, %{terminal: "response.completed"}} = Task.await(task, @budget)
          assert :ok = UpstreamWebsocketSession.invalidate_connection(session)
      end

      :sys.get_state(session, 1_500)
      await_resources_down!(monitors)
      assert resources_closed?(socket)
      retired_ms = elapsed(started, System.monotonic_time())
      assert retired_ms < 1_500
      assert {:ok, %{terminal: "response.completed"}} = bounded_request!(session, request(peer.url, 1024))
      send(holder, :release_peer)
      assert :ok = Peer.await_closed!(peer, 1, holder, holder_monitor, @budget)
      CodexPooler.TestDiagnostics.puts(fn -> CodexPooler.JSON.encode!(%{scenario: "queued_retirement_control", transport: context.transport, ending: unquote(ending), queued_bytes: queued, retirement_ms: retired_ms, successor_completed: true, old_resources_down: true}) end)
    end
  end

  @tag transport: :wss
  test "peer-close diagnostics fail for a live held peer and for its death", context do
    peer = Peer.start!(context, [:stalled])
    session = start_session!()
    request = %{request(peer.url, 1024) | timeouts: %{connect_timeout_ms: 5_000, receive_timeout_ms: 10_000}}
    task = Task.Supervisor.async_nolink(start_supervised!(Task.Supervisor), fn -> UpstreamWebsocketSession.request(session, request) end)
    assert_receive {:peer_upgraded, ref, 1, holder}, @budget
    assert ref == peer.ref
    on_exit(fn -> send(holder, :release_peer) end)
    live_monitor = Process.monitor(holder)

    live_error = assert_raise ExUnit.AssertionError, fn -> Peer.await_closed!(peer, 1, holder, live_monitor, 0) end
    assert "physical peer close not observed: " <> live_json = live_error.message
    assert %{"cause" => "timeout", "holder_alive" => true, "read_calls" => 0} = CodexPooler.JSON.decode!(live_json)

    death_monitor = Process.monitor(holder)
    Process.exit(holder, :kill)
    death_error = assert_raise ExUnit.AssertionError, fn -> Peer.await_closed!(peer, 1, holder, death_monitor, @budget) end
    assert "physical peer close not observed: " <> death_json = death_error.message
    assert %{"cause" => "holder_down", "holder_exit" => "killed", "holder_alive" => false} = CodexPooler.JSON.decode!(death_json)
    assert {:ok, {:error, _reason}} = Task.yield(task, 1_500)
  end

  for transport <- [:ws, :wss] do
    @tag transport: transport
    test "#{transport} release probes liveness without closing a live peer", context do
      peer = Peer.start!(context, [:stalled])
      url = URI.parse(peer.url)
      scheme = if context.transport == :wss, do: :https, else: :http
      {:ok, conn} = Mint.HTTP.connect(scheme, url.host, url.port, protocols: [:http1])
      on_exit(fn -> Mint.HTTP.close(conn) end)
      {:ok, conn, request_ref} = Mint.WebSocket.upgrade(context.transport, conn, url.path, [])
      {:ok, conn, status, headers} = SocketSupport.await_public_websocket_upgrade(conn, request_ref)
      {conn, websocket} = SocketSupport.mint_websocket_new!(conn, request_ref, status, headers)
      assert_receive {:peer_upgraded, ref, 1, holder}, @budget
      assert ref == peer.ref
      assert {:ok, conn} = Mint.HTTP.set_mode(conn, :passive)
      send(holder, :release_peer)
      assert {:ok, conn, [{:data, ^request_ref, bytes}]} = Mint.WebSocket.recv(conn, 2, @budget)
      assert {:ok, websocket, [{:ping, ""}]} = Mint.WebSocket.decode(websocket, bytes)
      assert {:ok, _websocket, encoded} = Mint.WebSocket.encode(websocket, {:pong, ""})
      assert {:ok, conn} = Mint.WebSocket.stream_request_body(conn, request_ref, encoded)
      error = assert_raise ExUnit.AssertionError, fn -> Peer.await_closed!(peer, 1, holder, Process.monitor(holder), 50) end
      assert "physical peer close not observed: " <> json = error.message
      assert %{"cause" => "timeout", "holder_alive" => true, "raw_port_alive" => true} = CodexPooler.JSON.decode!(json)
      assert Mint.HTTP.open?(conn)
      assert {:ok, _conn} = Mint.HTTP.close(conn)
      assert :ok = Peer.await_closed!(peer, 1, holder, Process.monitor(holder), @budget)
    end
  end

  defp bounded_request!(session, request) do
    task = Task.Supervisor.async_nolink(start_supervised!({Task.Supervisor, []}, id: make_ref()), fn -> UpstreamWebsocketSession.request(session, request) end)
    assert {:ok, result} = Task.yield(task, 1_500)
    result
  end

  test "peer cleanup waits for exact live socket ownership without closing it" do
    parent = self()
    tasks = start_supervised!(Task.Supervisor)

    owner =
      Task.Supervisor.async_nolink(tasks, fn ->
        {:ok, listener} = :gen_tcp.listen(0, [:binary, active: false, ip: {127, 0, 0, 1}])
        send(parent, {:owned_listener, listener})

        receive do
          :release_listener -> :gen_tcp.close(listener)
        end
      end)

    assert_receive {:owned_listener, listener}, @budget
    waiter = Task.Supervisor.async_nolink(tasks, fn -> Peer.await_resources_closed!(%{tasks: [owner.pid], sockets: [ws: listener]}) end)
    await_port_monitor!(waiter.pid, listener, System.monotonic_time(:millisecond) + @budget)
    assert Port.info(listener) != nil
    assert Task.yield(waiter, 0) == nil
    send(owner.pid, :release_listener)
    assert Task.await(owner, @budget) == :ok
    assert Task.await(waiter, @budget) == :ok
    assert Port.info(listener) == nil
  end

  test "peer cleanup refuses a live port rather than closing it to satisfy the assertion" do
    {:ok, listener} = :gen_tcp.listen(0, [:binary, active: false, ip: {127, 0, 0, 1}])
    on_exit(fn -> :gen_tcp.close(listener) end)
    assert_raise ExUnit.AssertionError, ~r/pending_ports=1/, fn -> Peer.await_resources_closed!(%{tasks: [], sockets: [ws: listener]}, 0) end
    assert Port.info(listener) != nil
  end

  defp await_port_monitor!(pid, port, deadline) do
    case Process.info(pid, :monitors) do
      {:monitors, monitors} ->
        if {:port, port} in monitors do
          :ok
        else
          assert System.monotonic_time(:millisecond) < deadline

          receive do
          after
            1 -> await_port_monitor!(pid, port, deadline)
          end
        end

      nil ->
        flunk("cleanup waiter exited before monitoring its owned port")
    end
  end

  defp start_session! do
    session = start_supervised!(%{id: make_ref(), start: {UpstreamWebsocketSession, :start_link, [[]]}, restart: :temporary})

    :sys.replace_state(session, fn state ->
      Process.flag(:sensitive, false)
      state
    end)

    :erlang.trace(session, true, [:call, :arity, :monotonic_timestamp, {:tracer, self()}])
    session
  end

  defp socket_options(:ws, socket), do: :inet.getopts(socket, [:linger, :send_timeout, :send_timeout_close])
  defp socket_options(:wss, socket), do: :ssl.getopts(socket, [:linger, :send_timeout, :send_timeout_close])

  # Test-only inspection of the installed OTP socket handle: observe every exact
  # port/process without calling into a TLS controller that may be closing.
  defp monitor_resources(socket) do
    socket
    |> resources()
    |> Enum.uniq()
    |> Enum.map(fn resource ->
      type = if is_port(resource), do: :port, else: :process
      {:erlang.monitor(type, resource), type, resource}
    end)
  end

  defp await_resources_down!(monitors) do
    Enum.each(monitors, fn {ref, type, resource} ->
      assert_receive {:DOWN, ^ref, ^type, ^resource, _reason}, 1_500
    end)
  end

  defp resources(value) when is_port(value) or is_pid(value), do: [value]
  defp resources(value) when is_tuple(value), do: value |> Tuple.to_list() |> Enum.flat_map(&resources/1)
  defp resources(value) when is_list(value), do: Enum.flat_map(value, &resources/1)
  defp resources(_value), do: []

  defp resources_closed?(value) when is_port(value), do: Port.info(value) == nil
  defp resources_closed?(value) when is_pid(value), do: not Process.alive?(value)
  defp resources_closed?(value) when is_tuple(value), do: value |> Tuple.to_list() |> Enum.all?(&resources_closed?/1)
  defp resources_closed?(value) when is_list(value), do: Enum.all?(value, &resources_closed?/1)
  defp resources_closed?(_value), do: true

  defp elapsed(from, to), do: System.convert_time_unit(to - from, :native, :millisecond)

  defp request(url, bytes) do
    %Request{provider_credits_context: CodexPooler.ProviderCreditsDispatchSupport.context!(), url: url, headers: [{"authorization", "Bearer synthetic-token"}], payload: CodexPooler.JSON.encode!(%{"model" => "upstream-test-model", "input" => [%{"type" => "message", "role" => "user", "content" => String.duplicate("x", bytes)}], "stream" => true}), timeouts: %{connect_timeout_ms: 5_000, receive_timeout_ms: 500}, writer: fn _ -> :ok end, message_mapper: nil}
  end
end
