defmodule CodexPooler.Gateway.Transports.FileFinalizeDeadlineTest do
  use CodexPooler.DataCase, async: false

  alias CodexPooler.Gateway.Routing.RoutingSelection
  alias CodexPooler.Gateway.Transports.FileBridge

  @budget 5_000

  for retry_first <- [false, true] do
    @tag retry_first: retry_first
    test "finalize deadline bounds a blocked poll with retry_first=#{retry_first}", %{retry_first: retry_first} do
      {:ok, listener} = :gen_tcp.listen(0, [:binary, active: false, packet: :raw, ip: {127, 0, 0, 1}])
      on_exit(fn -> :gen_tcp.close(listener) end)
      {:ok, {_, port}} = :inet.sockname(listener)
      supervisor = start_supervised!(Task.Supervisor)
      parent = self()

      %{pool: pool} = CodexPooler.PoolerFixtures.active_api_key_fixture()
      %{assignment: assignment, identity: identity} = CodexPooler.PoolerFixtures.active_upstream_assignment_fixture(pool, %{metadata: %{"base_url" => "http://127.0.0.1:#{port}"}, access_token: "synthetic-token"})
      selection = %RoutingSelection{assignment: assignment, identity: identity, route_metadata: %{}}

      server =
        Task.Supervisor.async_nolink(supervisor, fn ->
          {:ok, socket} = :gen_tcp.accept(listener, @budget)
          on_socket(socket, listener, parent, retry_first)
        end)

      task = Task.Supervisor.async_nolink(supervisor, fn -> FileBridge.finalize_file("file_synthetic", %{finalize_retry_timeout_ms: 300, finalize_retry_interval_ms: 0, receive_timeout_ms: 30_000}, selection) end)

      on_exit(fn ->
        if(Process.alive?(task.pid), do: Process.exit(task.pid, :kill))
        if(Process.alive?(server.pid), do: Process.exit(server.pid, :kill))
      end)

      assert_receive :finalize_poll_held, @budget
      assert {:ok, result} = Task.yield(task, 1_500)
      if retry_first, do: assert({:retry_timeout, %{body: %{"status" => "retry"}}} = result), else: assert({:error, %{status: 502}} = result)
      assert :closed = Task.await(server, @budget)
    end
  end

  test "zero finalize budget refuses before opening an upstream connection" do
    {:ok, listener} = :gen_tcp.listen(0, [:binary, active: false, packet: :raw, ip: {127, 0, 0, 1}])
    on_exit(fn -> :gen_tcp.close(listener) end)
    {:ok, {_, port}} = :inet.sockname(listener)
    %{pool: pool} = CodexPooler.PoolerFixtures.active_api_key_fixture()
    %{assignment: assignment, identity: identity} = CodexPooler.PoolerFixtures.active_upstream_assignment_fixture(pool, %{metadata: %{"base_url" => "http://127.0.0.1:#{port}"}, access_token: "synthetic-token"})
    selection = %RoutingSelection{assignment: assignment, identity: identity, route_metadata: %{}}
    assert {:error, %{status: 502, code: :upstream_request_failed}} = FileBridge.finalize_file("file_synthetic", %{finalize_retry_timeout_ms: 0}, selection)
    assert {:error, :timeout} = :gen_tcp.accept(listener, 0)
  end

  defp on_socket(socket, listener, parent, retry_first) do
    receive_request(socket, "")

    if retry_first do
      body = ~s({"status":"retry"})
      :ok = :gen_tcp.send(socket, "HTTP/1.1 200 OK\r\nconnection: close\r\ncontent-type: application/json\r\ncontent-length: #{byte_size(body)}\r\n\r\n" <> body)
      :gen_tcp.close(socket)
      {:ok, next} = :gen_tcp.accept(listener, @budget)
      on_socket(next, listener, parent, false)
    end

    unless retry_first do
      send(parent, :finalize_poll_held)
      assert {:error, :closed} = :gen_tcp.recv(socket, 0, @budget)
    end

    :closed
  after
    :gen_tcp.close(socket)
  end

  defp receive_request(socket, data) do
    case :binary.split(data, "\r\n\r\n") do
      [head, body] ->
        [_, length] = Regex.run(~r/content-length: (\d+)/i, head)
        remaining = String.to_integer(length) - byte_size(body)
        if remaining > 0, do: assert({:ok, _} = :gen_tcp.recv(socket, remaining, @budget))
        :ok

      _incomplete ->
        assert {:ok, chunk} = :gen_tcp.recv(socket, 0, @budget)
        receive_request(socket, data <> chunk)
    end
  end
end
