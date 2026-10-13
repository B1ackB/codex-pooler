defmodule CodexPooler.UpstreamWebsocketRetirementPeer do
  @moduledoc false
  import ExUnit.Assertions
  import ExUnit.Callbacks
  require Record
  Record.defrecordp(:ssl_socket, :sslsocket, Record.extract(:sslsocket, from_lib: "ssl/src/ssl_api.hrl"))
  @budget 15_000
  @guid "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

  @spec trust!() :: %{certificate: keyword()}
  def trust! do
    certificate = :public_key.pkix_test_data(%{root: [digest: :sha256, key: {:rsa, 2048, 65_537}], peer: [digest: :sha256, key: {:rsa, 2048, 65_537}, extensions: [{:Extension, {2, 5, 29, 17}, false, [{:iPAddress, <<127, 0, 0, 1>>}]}]]})
    previous = :persistent_term.get(:pubkey_os_cacerts, :absent)
    directory = Path.join(System.tmp_dir!(), "retirement-#{Ecto.UUID.generate()}")

    on_exit(fn ->
      if previous == :absent, do: :public_key.cacerts_clear(), else: :persistent_term.put(:pubkey_os_cacerts, previous)
      assert :persistent_term.get(:pubkey_os_cacerts, :absent) == previous
      File.rm_rf!(directory)
      refute File.exists?(directory)
      CodexPooler.TestDiagnostics.puts(fn -> CodexPooler.JSON.encode!(%{scenario: "retirement_trust_cleanup", restored: true, temporary_directory_removed: true}) end)
    end)

    File.mkdir!(directory)
    File.chmod!(directory, 0o700)
    ca = Path.join(directory, "ca.pem")
    File.write!(ca, :public_key.pem_encode(Enum.map(certificate[:cacerts], &{:Certificate, &1, :not_encrypted})))
    File.chmod!(ca, 0o600)
    :ok = :public_key.cacerts_load(String.to_charlist(ca))

    %{certificate: certificate}
  end

  @spec socket_closed?(:ws | :wss, term()) :: boolean()
  def socket_closed?(:ws, socket), do: Port.info(socket) == nil
  def socket_closed?(:wss, socket), do: match?({:error, :closed}, :ssl.connection_information(socket))

  @spec start!(map(), [atom()]) :: %{url: String.t(), ref: reference(), progress: :ets.tid()}
  def start!(context, scripts) do
    parent = self()
    ref = make_ref()
    {:ok, tracker} = Agent.start(fn -> %{tasks: [], sockets: [], progress: :ets.new(__MODULE__, [:set, :public])} end)

    on_exit(fn ->
      resources = Agent.get(tracker, & &1)

      try do
        await_resources_closed!(resources)
      after
        Agent.stop(tracker)
        assert :ets.info(resources.progress) == :undefined
      end

      CodexPooler.TestDiagnostics.puts(fn -> CodexPooler.JSON.encode!(%{scenario: "retirement_peer_cleanup", tasks_stopped: true, sockets_closed: true}) end)
    end)

    progress = Agent.get(tracker, & &1.progress)
    start_supervised!(Supervisor.child_spec({Task, fn -> peer(parent, ref, context, scripts, tracker, progress) end}, id: ref, restart: :temporary))
    assert_receive {:peer_listening, ^ref, port}, @budget
    scheme = if context.transport == :wss, do: "https", else: "http"
    %{url: "#{scheme}://127.0.0.1:#{port}/backend-api/codex/responses", ref: ref, progress: progress}
  end

  @doc false
  @spec await_resources_closed!(%{tasks: [pid()], sockets: [{:ws | :wss, term()}]}, non_neg_integer()) :: :ok
  def await_resources_closed!(resources, timeout \\ @budget) do
    owned = Enum.uniq(resources.tasks ++ Enum.flat_map(resources.sockets, fn {_transport, socket} -> socket_resources(socket) end))

    monitors =
      Enum.map(owned, fn resource ->
        type = if is_port(resource), do: :port, else: :process
        {:erlang.monitor(type, resource), type, resource}
      end)

    deadline = System.monotonic_time(:millisecond) + timeout

    try do
      for {monitor, type, resource} <- monitors do
        receive do
          {:DOWN, ^monitor, ^type, ^resource, _reason} -> :ok
        after
          max(deadline - System.monotonic_time(:millisecond), 0) ->
            pending_ports = Enum.count(owned, &(is_port(&1) and Port.info(&1) != nil))
            pending_processes = Enum.count(owned, &(is_pid(&1) and Process.alive?(&1)))
            flunk("peer resources still live: pending_ports=#{pending_ports} pending_processes=#{pending_processes}")
        end
      end

      assert Enum.all?(resources.tasks, &(not Process.alive?(&1)))
      assert Enum.all?(resources.sockets, fn {transport, socket} -> socket_closed?(transport, socket) end)
      :ok
    after
      for {monitor, _type, _resource} <- monitors, do: Process.demonitor(monitor, [:flush])
    end
  end

  # OTP's server handle also contains shared session-ticket trackers. Only
  # the transport and these two processes belong to this connection.
  defp socket_resources(ssl_socket(socket_handle: socket, connection_handler: receiver, payload_sender: sender)),
    do: Enum.flat_map([socket, receiver, sender], &socket_resources/1)

  defp socket_resources(value) when is_pid(value) or is_port(value), do: [value]
  defp socket_resources(value) when is_tuple(value), do: value |> Tuple.to_list() |> Enum.flat_map(&socket_resources/1)
  defp socket_resources(value) when is_list(value), do: Enum.flat_map(value, &socket_resources/1)
  defp socket_resources(_value), do: []

  defp peer(parent, ref, context, scripts, tracker, progress) do
    peer_pid = self()
    Agent.update(tracker, &%{&1 | tasks: [peer_pid | &1.tasks]})
    {:ok, listener} = :gen_tcp.listen(0, [:binary, active: false, packet: :raw, reuseaddr: true, recbuf: 4096, ip: {127, 0, 0, 1}])
    Agent.update(tracker, &%{&1 | sockets: [{:ws, listener} | &1.sockets]})
    {:ok, port} = :inet.port(listener)
    send(parent, {:peer_listening, ref, port})
    accept_loop(listener, parent, ref, context, scripts, tracker, progress, 1)
  end

  defp accept_loop(listener, parent, ref, context, scripts, tracker, progress, id) do
    {:ok, socket} = :gen_tcp.accept(listener)

    holder =
      spawn_link(fn ->
        receive do
          :owned -> serve(socket, parent, ref, context, Enum.at(scripts, id - 1, List.last(scripts)), id, tracker, progress)
        end
      end)

    initial_progress = %{phase: :accepted, holder: holder, raw: socket, tls_processes: [], received_bytes: 0, read_calls: 0, last_recv: :none, recv_started_at: nil}
    true = :ets.insert(progress, {id, initial_progress})
    Agent.update(tracker, &%{&1 | tasks: [holder | &1.tasks], sockets: [{:ws, socket} | &1.sockets]})
    :ok = :gen_tcp.controlling_process(socket, holder)
    send(holder, :owned)
    accept_loop(listener, parent, ref, context, scripts, tracker, progress, id + 1)
  end

  defp serve(raw, parent, ref, context, script, id, tracker, progress) do
    {transport, socket} =
      if context.transport == :wss do
        case :ssl.handshake(raw, [cert: context.certificate[:cert], key: context.certificate[:key], active: false, mode: :binary], @budget) do
          {:ok, socket} ->
            Agent.update(tracker, &%{&1 | sockets: [{:wss, socket} | &1.sockets]})
            update_progress(progress, id, &Map.put(&1, :tls_processes, socket_processes(socket)))
            {:ssl, socket}

          {:error, _reason} ->
            send(parent, {:peer_tls_rejected, ref})
            :gen_tcp.close(raw)
            exit(:normal)
        end
      else
        {:gen_tcp, raw}
      end

    {:ok, key} = read_head(transport, socket, "")
    send(parent, {:peer_head, ref, id, self()})

    case script do
      :hold ->
        :ok

      :refuse ->
        :ok = transport.send(socket, "HTTP/1.1 503 Service Unavailable\r\ncontent-length: 0\r\n\r\n")

      mode when mode in [:success, :stalled, :small_silent, :early_terminal] ->
        accept = Base.encode64(:crypto.hash(:sha, key <> @guid))
        :ok = transport.send(socket, ["HTTP/1.1 101 Switching Protocols\r\nupgrade: websocket\r\nconnection: Upgrade\r\nsec-websocket-accept: ", accept, "\r\n\r\n"])
    end

    update_progress(progress, id, &Map.put(&1, :phase, :upgraded))
    send(parent, {:peer_upgraded, ref, id, self()})

    if script == :early_terminal do
      data = CodexPooler.JSON.encode!(%{"type" => "response.completed", "response" => %{"id" => "resp_synthetic_early", "status" => "completed", "output" => []}})
      :ok = transport.send(socket, <<0x81, 126, byte_size(data)::16, data::binary>>)
    end

    if script in [:stalled, :early_terminal] do
      update_progress(progress, id, &Map.put(&1, :phase, :held))

      receive do
        :release_peer ->
          update_progress(progress, id, &Map.put(&1, :phase, :draining))

          probe_closure(transport, socket)

          drain(transport, socket, parent, ref, id, progress)
      end
    else
      serve_frames(transport, socket, parent, ref, id, script, 0)
    end
  end

  # An abortive reset can leave the receiving TCP peer open while queued
  # bytes arrive. A valid Ping probes the retired endpoint; only the
  # subsequent closed/reset receive in drain/6 proves physical closure.
  defp probe_closure(transport, socket) do
    case transport.send(socket, <<0x89, 0>>) do
      :ok -> :ok
      {:error, reason} when reason in [:closed, :econnreset] -> :ok
      {:error, reason} -> exit({:peer_probe_failed, reason})
    end
  end

  defp read_head(transport, socket, bytes) do
    if String.contains?(bytes, "\r\n\r\n") do
      [_, key] = Regex.run(~r/sec-websocket-key: ([^\r\n]+)/i, bytes)
      {:ok, key}
    else
      with {:ok, data} <- transport.recv(socket, 0, @budget), do: read_head(transport, socket, bytes <> data)
    end
  end

  defp serve_frames(transport, socket, parent, ref, id, script, count) do
    case transport.recv(socket, 2, @budget) do
      {:error, :closed} ->
        send(parent, {:peer_closed, ref, id})

      {:ok, <<first, second>>} when script in [:success, :small_silent] ->
        length = Bitwise.band(second, 0x7F)

        length =
          if length == 126 do
            {:ok, <<size::16>>} = transport.recv(socket, 2, @budget)
            size
          else
            length
          end

        {:ok, _mask_and_body} = transport.recv(socket, 4 + length, @budget)
        assert Bitwise.band(first, 0xF) == 1
        count = count + 1
        send(parent, {:peer_request, ref, id, count})
        data = CodexPooler.JSON.encode!(%{"type" => "response.completed", "response" => %{"id" => "resp_synthetic_upgrade", "status" => "completed", "output" => []}})
        if script == :success, do: :ok = transport.send(socket, <<0x81, 126, byte_size(data)::16, data::binary>>)
        serve_frames(transport, socket, parent, ref, id, script, count)

      other ->
        exit({:unexpected_peer_read, elem(other, 0)})
    end
  end

  defp drain(transport, socket, parent, ref, id, progress) do
    update_progress(progress, id, &%{&1 | read_calls: &1.read_calls + 1, last_recv: :pending, recv_started_at: System.monotonic_time(:millisecond)})

    case transport.recv(socket, 0, @budget) do
      {:ok, discarded} ->
        update_progress(progress, id, &%{&1 | received_bytes: &1.received_bytes + byte_size(discarded), last_recv: :ok})
        drain(transport, socket, parent, ref, id, progress)

      {:error, reason} when reason in [:closed, :econnreset] ->
        update_progress(progress, id, &%{&1 | phase: :closed, last_recv: reason})
        send(parent, {:peer_closed, ref, id})

      {:error, reason} ->
        update_progress(progress, id, &%{&1 | phase: :failed, last_recv: if(reason == :timeout, do: :timeout, else: :other_error)})
        exit({:peer_drain_failed, reason})
    end
  end

  @spec await_closed!(map(), pos_integer(), pid(), reference(), non_neg_integer()) :: :ok
  def await_closed!(peer, id, holder, monitor, timeout) do
    ref = peer.ref

    result =
      receive do
        {:peer_closed, ^ref, ^id} -> :ok
        {:DOWN, ^monitor, :process, ^holder, reason} -> {:holder_down, exit_class(reason)}
      after
        timeout -> :timeout
      end

    Process.demonitor(monitor, [:flush])

    case result do
      :ok -> :ok
      failure -> flunk("physical peer close not observed: " <> CodexPooler.JSON.encode!(failure_snapshot(peer, id, failure)))
    end
  end

  defp failure_snapshot(peer, id, failure) do
    cause = if failure == :timeout, do: %{cause: :timeout}, else: %{cause: :holder_down, holder_exit: elem(failure, 1)}
    Map.merge(snapshot(peer, id), cause)
  end

  defp snapshot(%{progress: table}, id) do
    case :ets.lookup(table, id) do
      [] ->
        %{phase: :not_registered}

      [{^id, progress}] ->
        progress
        |> Map.take([:phase, :received_bytes, :read_calls, :last_recv])
        |> Map.merge(%{holder_alive: Process.alive?(progress.holder), raw_port_alive: Port.info(progress.raw) != nil, tls_process_count: length(progress.tls_processes), tls_processes_alive: Enum.count(progress.tls_processes, &Process.alive?/1), recv_pending_ms: if(progress.last_recv == :pending, do: max(System.monotonic_time(:millisecond) - progress.recv_started_at, 0), else: 0)})
    end
  rescue
    ArgumentError -> %{phase: :diagnostics_unavailable}
  end

  defp exit_class(reason) when reason in [:normal, :shutdown, :killed], do: reason
  defp exit_class(_reason), do: :other_exit

  defp update_progress(table, id, fun) do
    [{^id, progress}] = :ets.lookup(table, id)
    true = :ets.insert(table, {id, fun.(progress)})
    :ok
  end

  defp socket_processes(value), do: value |> socket_resources() |> Enum.filter(&is_pid/1) |> Enum.uniq()
end
