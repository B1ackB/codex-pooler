defmodule CodexPooler.Alerts.WebhookDeliveryDeadlineTest do
  use CodexPooler.DataCase, async: false
  import Ecto.Query
  import CodexPooler.AccountsFixtures
  import CodexPooler.PoolerFixtures
  alias CodexPooler.Accounts.Scope
  alias CodexPooler.Alerts
  alias CodexPooler.Alerts.Delivery.{Execution, WebhookDelivery, WebhookDestination}
  alias CodexPooler.Alerts.Schemas.AlertDeliveryAttempt
  alias CodexPooler.Jobs.AlertDeliveryWorker
  alias CodexPooler.Platform.OutboundHTTP
  alias CodexPooler.Repo
  alias CodexPooler.TestAppEnv
  alias Oban.Engines.Basic

  defmodule Receiver do
    def init(opts), do: opts

    def call(conn, {parent, mode}) do
      {:ok, _body, conn} = Plug.Conn.read_body(conn)
      send(parent, {:https_request, self()})

      case mode do
        :success ->
          Plug.Conn.send_resp(conn, 204, "")

        :drip ->
          drip(Plug.Conn.send_chunked(conn, 200), parent)

        :hold ->
          receive do
            :finish -> Plug.Conn.send_resp(conn, 200, "done")
          after
            5_000 -> raise "HTTP pool barrier not released"
          end
      end
    end

    defp drip(conn, parent) do
      ref = make_ref()
      Process.send_after(self(), ref, 50)

      receive do
        ^ref ->
          case Plug.Conn.chunk(conn, "x") do
            {:ok, conn} ->
              drip(conn, parent)

            {:error, _} ->
              send(parent, :https_connection_released)
              conn
          end
      end
    end
  end

  setup do
    TestAppEnv.restore_on_exit(OutboundHTTP)
    Application.put_env(:codex_pooler, OutboundHTTP, proxy_config: %{http: [], https: [], no_proxy: []})
    %{user: user} = bootstrap_owner_fixture()
    fixture = CodexPooler.WebhookDestinationFixture.setup!()
    Map.put(fixture, :scope, Scope.for_user(user, ["instance_owner"]))
  end

  test "actual worker creates one absolute default budget before preparation and carries it through HTTPS and receipt", context do
    fixture = fixture!(context, :success)
    patterns = [{Execution, :new, 1}, {Execution, :run, 2}, {Execution, :storage, 2}, {Alerts, :next_delivery_attempt_number, 3}]
    on_exit(fn -> Enum.each(patterns, &:erlang.trace_pattern(&1, false, [:local])) end)
    Enum.each([Execution, Alerts], &Code.ensure_loaded!/1)
    :erlang.trace_pattern({Execution, :new, 1}, [{:_, [], [{:return_trace}]}], [:local])
    for function <- [:run, :storage], do: :erlang.trace_pattern({Execution, function, 2}, [{[:"$1", :_], [], [{:message, {:map_get, :deadline, :"$1"}}]}], [:local])
    :erlang.trace_pattern({Alerts, :next_delivery_attempt_number, 3}, [{:_, [], [{:message, :number_selection}]}], [:local])
    # A dedicated tracer forwards only typed clock data/arity; no headers or body.
    parent = self()
    tracer = spawn_link(fn -> forward_trace(parent) end)
    on_exit(fn -> if Process.alive?(tracer), do: Process.exit(tracer, :kill) end)
    :erlang.trace(self(), true, [:call, :arity, {:tracer, tracer}])
    started = System.monotonic_time(:millisecond)
    assert :ok = Oban.Testing.perform_job(fixture.job, [])
    :erlang.trace(self(), false, [:call])
    assert_receive {:observed, {:trace, _, :return_from, {Execution, :new, 1}, %Execution{} = execution}}, 5_000
    assert execution.deadline >= started + 10_000
    assert execution.deadline < System.monotonic_time(:millisecond) + 10_000
    assert execution.completion_deadline - execution.deadline == 2_000
    assert_receive {:observed, {:trace, _, :call, {Alerts, :next_delivery_attempt_number, 3}, :number_selection}}, 5_000
    deadline = execution.deadline
    assert_receive {:observed, {:trace, _, :call, {Execution, :storage, 2}, ^deadline}}, 5_000
    assert_receive {:observed, {:trace, _, :call, {Execution, :run, 2}, ^deadline}}, 5_000
    assert_receive {:observed, {:trace, _, :call, {Execution, :storage, 2}, ^deadline}}, 5_000
    assert_receive {:https_request, _}, 5_000
    [attempt] = Repo.all(from a in AlertDeliveryAttempt, where: a.incident_id == ^fixture.incident.id)
    assert attempt.status == "sent"
    assert attempt.response_metadata["delivery_execution"]["job_id"] == fixture.job.id
  end

  test "full delivery preserves the timeout receipt whether its short budget reaches HTTPS or expires earlier", context do
    fixture = fixture!(context, :drip)
    execution = %{Execution.new(fixture.job) | deadline: System.monotonic_time(:millisecond) + 300}
    assert {:error, %{code: "alert_webhook_delivery_timeout", retryable: true}} = WebhookDelivery.deliver_incident_to_channel(fixture.incident.id, fixture.channel.id, 1, delivery_execution: execution)
    assert Execution.remaining(execution) == 0

    # A valid absolute timeout may consume its budget before HTTP arrival.
    # Fence any accepted connection before classifying the observed phase.
    assert {:ok, connections} = ThousandIsland.connection_pids(fixture.server)
    monitors = Enum.map(connections, &{&1, Process.monitor(&1)})
    for {pid, monitor} <- monitors, do: assert_receive({:DOWN, ^monitor, :process, ^pid, _reason}, 5_000)
    assert {:ok, []} = ThousandIsland.connection_pids(fixture.server)

    reached_http? =
      receive do
        {:https_request, _receiver} ->
          assert_receive :https_connection_released, 5_000
          true
      after
        0 -> false
      end

    [attempt] = Repo.all(from a in AlertDeliveryAttempt, where: a.incident_id == ^fixture.incident.id)
    assert attempt.status == "retryable"
    assert DateTime.compare(attempt.next_retry_at, attempt.completed_at) == :gt
    assert attempt.response_metadata["delivery_outcome"] == "unknown"
    assert attempt.response_metadata["delivery_execution"]["job_id"] == fixture.job.id
    CodexPooler.TestDiagnostics.puts("alert-full-delivery deadline_ms=300 reached_http=#{reached_http?} active_connections=0 receipt=retryable outcome=unknown")
  end

  test "a 300ms absolute deadline cancels an established private HTTPS drip and releases its connection", context do
    fixture = fixture!(context, :drip)
    {:ok, connection_tracker} = Agent.start(fn -> nil end)

    on_exit(fn ->
      if connection = Agent.get(connection_tracker, & &1), do: Mint.HTTP.close(connection)
      Agent.stop(connection_tracker)
    end)

    uri = URI.parse(fixture.url)
    assert {:ok, connection} = Mint.HTTP.connect(:https, context.ip, uri.port, hostname: context.host, protocols: [:http1], mode: :passive, transport_opts: [cacerts: context.cert[:cacerts]])
    Agent.update(connection_tracker, fn _ -> connection end)
    assert {:ok, connection, ref} = Mint.HTTP.request(connection, "POST", uri.path, [], "")
    assert_receive {:https_request, _}, 5_000
    connection = await_first_drip(connection, ref)
    parent = self()
    tasks = start_supervised!(Task.Supervisor)

    caller =
      Task.Supervisor.async_nolink(tasks, fn ->
        execution = %{Execution.new() | deadline: System.monotonic_time(:millisecond) + 300}

        Execution.run(execution, fn ->
          send(parent, {:drip_executor, self()})

          receive do
            {:owned_drip, connection} -> drain_drip(connection)
          end
        end)
      end)

    caller_monitor = Process.monitor(caller.pid)
    assert_receive {:drip_executor, executor}, 5_000
    executor_monitor = Process.monitor(executor)
    assert {:ok, connection} = Mint.HTTP.controlling_process(connection, executor)
    send(executor, {:owned_drip, connection})
    assert {:error, :delivery_timeout} = Task.await(caller, 5_000)
    assert_receive {:DOWN, ^executor_monitor, :process, ^executor, :killed}, 5_000
    assert_receive {:DOWN, ^caller_monitor, :process, _, :normal}, 5_000
    assert_receive :https_connection_released, 5_000
    CodexPooler.TestDiagnostics.puts("alert-drip established_before_budget=true deadline_ms=300 executor_down=true peer_released=true")
  end

  @tag slow: "holds the caller while the real HTTPS transport exhausts its absolute budget"
  test "an exhausted HTTPS receive budget keeps the delivery timeout receipt when the caller resumes after the transport", context do
    TestAppEnv.restore_on_exit(WebhookDestination)
    parent = self()

    Application.put_env(:codex_pooler, WebhookDestination,
      resolver: fn
        _, :inet ->
          send(parent, {:delivery_executor, self()})
          {:ok, [context.ip]}

        _, :inet6 ->
          {:error, :nxdomain}
      end
    )

    fixture = fixture!(%{context | host: "webhook.example.test"}, :drip)
    execution = %{Execution.new(fixture.job) | deadline: System.monotonic_time(:millisecond) + 2_000}
    tasks = start_supervised!(Task.Supervisor)

    caller =
      Task.Supervisor.async_nolink(tasks, fn ->
        WebhookDelivery.deliver_incident_to_channel(fixture.incident.id, fixture.channel.id, 1, delivery_execution: execution)
      end)

    assert_receive {:delivery_executor, executor}, 5_000
    monitor = Process.monitor(executor)
    assert_receive {:https_request, _receiver}, 5_000

    try do
      assert :erlang.suspend_process(caller.pid)
      assert_receive {:DOWN, ^monitor, :process, ^executor, :normal}, 5_000
      assert Execution.remaining(execution) == 0
    after
      :erlang.resume_process(caller.pid)
    end

    assert {:error, %{code: "alert_webhook_delivery_timeout", retryable: true}} = Task.await(caller, 5_000)
    assert_receive :https_connection_released, 5_000
    [attempt] = Repo.all(from a in AlertDeliveryAttempt, where: a.incident_id == ^fixture.incident.id)
    assert attempt.status == "retryable"
    assert attempt.response_metadata["delivery_outcome"] == "unknown"
    assert attempt.response_metadata["delivery_execution"]["job_id"] == fixture.job.id
    assert DateTime.compare(attempt.next_retry_at, attempt.completed_at) == :gt
  end

  @tag slow: "exercises the real five-second TLS connection timeout before the delivery deadline"
  test "an earlier TLS connection timeout remains a transport timeout while the delivery budget is available", context do
    {:ok, listener} = :gen_tcp.listen(0, [:binary, active: false, ip: context.ip])
    on_exit(fn -> :gen_tcp.close(listener) end)
    {:ok, port} = :inet.port(listener)
    tasks = start_supervised!(Task.Supervisor)
    parent = self()

    peer =
      Task.Supervisor.async_nolink(tasks, fn ->
        {:ok, socket} = :gen_tcp.accept(listener, 10_000)

        try do
          {:ok, _hello} = :gen_tcp.recv(socket, 0, 5_000)
          send(parent, :early_tls_started)
          await_tls_close(socket, 10_000)
        after
          :gen_tcp.close(socket)
        end
      end)

    pool = pool_fixture()
    {:ok, channel} = Alerts.create_channel(context.scope, %{channel_type: "webhook", display_name: "connect deadline", endpoint_url: "https://#{context.host}:#{port}/hooks", webhook_signing_secret: "synthetic-connect-secret"})
    incident = alert_incident_fixture(pool: pool)
    execution = Execution.new()
    assert {:error, %{code: "alert_webhook_transport_timeout", retryable: true}} = WebhookDelivery.deliver_incident_to_channel(incident.id, channel.id, 1, delivery_execution: execution)
    assert Execution.remaining(execution) > 0
    assert_receive :early_tls_started
    assert Task.await(peer, 5_000) == {:error, :closed}
    [attempt] = Repo.all(from a in AlertDeliveryAttempt, where: a.incident_id == ^incident.id)
    assert attempt.status == "retryable"
    assert attempt.response_metadata["transport_reason"] == "timeout"
    refute Map.has_key?(attempt.response_metadata, "delivery_outcome")
  end

  test "TLS negotiation that never answers is cancelled by the same absolute deadline" do
    {:ok, listener} = :gen_tcp.listen(0, [:binary, active: false, ip: {127, 0, 0, 1}])
    on_exit(fn -> :gen_tcp.close(listener) end)
    {:ok, port} = :inet.port(listener)
    parent = self()
    tasks = start_supervised!(Task.Supervisor)

    peer =
      Task.Supervisor.async_nolink(tasks, fn ->
        {:ok, socket} = :gen_tcp.accept(listener, 5_000)

        try do
          {:ok, _client_hello} = :gen_tcp.recv(socket, 0, 5_000)
          send(parent, :tls_started)
          await_tls_close(socket)
        after
          :gen_tcp.close(socket)
        end
      end)

    monitor = Process.monitor(peer.pid)
    execution = %{Execution.new() | deadline: System.monotonic_time(:millisecond) + 300}
    # Preparation consuming the deadline is covered separately. This call must
    # actually enter TLS so its closed socket proves handshake cancellation.
    assert {:error, :delivery_timeout} =
             Execution.run(execution, fn ->
               OutboundHTTP.post("https://127.0.0.1:#{port}/hooks", body: "", retry: false, redirect: false, receive_timeout: 5_000)
             end)

    assert_receive :tls_started, 5_000
    assert Task.await(peer, 5_000) == {:error, :closed}
    assert_receive {:DOWN, ^monitor, :process, _, :normal}, 5_000
    CodexPooler.TestDiagnostics.puts("alert-handshake client_hello=true deadline_ms=300 socket_closed=true peer_down=true")
  end

  test "preparation exhausting the same budget returns a deadline receipt without connecting", context do
    {:ok, listener} = :gen_tcp.listen(0, [:binary, active: false, ip: context.ip])
    on_exit(fn -> :gen_tcp.close(listener) end)
    {:ok, port} = :inet.port(listener)
    pool = pool_fixture()
    {:ok, channel} = Alerts.create_channel(context.scope, %{channel_type: "webhook", display_name: "preparation deadline", endpoint_url: "https://#{context.host}:#{port}/hooks", webhook_signing_secret: "synthetic-deadline-secret"})
    incident = alert_incident_fixture(pool: pool)
    execution = %{Execution.new() | deadline: System.monotonic_time(:millisecond) + 300}
    handler = {__MODULE__, self()}
    on_exit(fn -> :telemetry.detach(handler) end)
    :ok = :telemetry.attach(handler, [:codex_pooler, :repo, :query], &__MODULE__.hold_preparation/4, {self(), execution.deadline})
    assert {:error, %{code: "alert_webhook_delivery_timeout", retryable: true}} = WebhookDelivery.deliver_incident_to_channel(incident.id, channel.id, 1, delivery_execution: execution)
    assert_receive {:preparation_budget_elapsed, held_until}, 5_000
    assert held_until >= execution.deadline
    assert Execution.remaining(execution) == 0
    assert {:error, :timeout} = :gen_tcp.accept(listener, 0)
    [attempt] = Repo.all(from a in AlertDeliveryAttempt, where: a.incident_id == ^incident.id)
    assert attempt.status == "retryable"
    assert attempt.completed_at
    assert DateTime.compare(attempt.next_retry_at, attempt.completed_at) == :gt
    assert attempt.response_metadata["delivery_outcome"] == "unknown"
    CodexPooler.TestDiagnostics.puts("alert-preparation deadline_expired=true tcp_connections=0 receipt=retryable outcome=unknown")
  end

  test "absolute executor budget cancels a real Finch checkout without consuming the held connection", context do
    fixture = fixture!(context, :hold)
    finch_name = Module.concat(__MODULE__, "Finch#{System.unique_integer([:positive])}")
    start_supervised!({Finch, name: finch_name, pools: %{default: [size: 1, count: 1, conn_opts: [transport_opts: [cacerts: context.cert[:cacerts]]]]}})
    tasks = start_supervised!(Task.Supervisor)
    first = Task.Supervisor.async_nolink(tasks, fn -> Req.get(fixture.url, finch: [name: finch_name], retry: false) end)
    first_monitor = Process.monitor(first.pid)
    assert_receive {:https_request, held}, 5_000
    execution = %{Execution.new() | deadline: System.monotonic_time(:millisecond) + 200}
    assert {:error, :delivery_timeout} = Execution.run(execution, fn -> Req.get(fixture.url, finch: [name: finch_name], retry: false) end)
    send(held, :finish)
    assert {:ok, %{status: 200}} = Task.await(first, 5_000)
    assert_receive {:DOWN, ^first_monitor, :process, _, :normal}, 5_000
    # Completed first writer plus a fresh successful checkout fences the
    # cancellation; a queued cancelled caller must not consume the next slot.
    third = Task.Supervisor.async_nolink(tasks, fn -> Req.get(fixture.url, finch: [name: finch_name], retry: false) end)
    third_monitor = Process.monitor(third.pid)
    assert_receive {:https_request, next}, 5_000
    send(next, :finish)
    assert {:ok, %{status: 200}} = Task.await(third, 5_000)
    assert_receive {:DOWN, ^third_monitor, :process, _, :normal}, 5_000
  end

  test "absolute executor budget closes an owned TCP socket blocked sending synthetic bytes" do
    {:ok, listener} = :gen_tcp.listen(0, [:binary, active: false, ip: {127, 0, 0, 1}, recbuf: 1024])
    on_exit(fn -> :gen_tcp.close(listener) end)
    {:ok, port} = :inet.port(listener)
    tasks = start_supervised!(Task.Supervisor)
    parent = self()

    sender =
      Task.Supervisor.async_nolink(tasks, fn ->
        execution = %{Execution.new() | deadline: System.monotonic_time(:millisecond) + 200}

        Execution.run(execution, fn ->
          {:ok, socket} = :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false, sndbuf: 1024], 5_000)
          send(parent, :send_ready)

          try do
            :gen_tcp.send(socket, :binary.copy(<<0>>, 16_777_216))
          after
            :gen_tcp.close(socket)
          end
        end)
      end)

    monitor = Process.monitor(sender.pid)
    {:ok, peer} = :gen_tcp.accept(listener, 5_000)
    on_exit(fn -> :gen_tcp.close(peer) end)
    assert_receive :send_ready, 5_000
    assert Task.await(sender, 5_000) == {:error, :delivery_timeout}
    assert_receive {:DOWN, ^monitor, :process, _, :normal}, 5_000
    bytes = drain_closed_socket(peer, 0)
    # Timeout is not proof that zero or partial bytes reached the peer. The
    # actual EOF and sender DOWN are the cancellation/ownership oracles.
    CodexPooler.TestDiagnostics.puts("alert-send-cancellation received_bytes=#{bytes} outcome=unknown socket_closed=true")
    assert bytes <= 16_777_216
  end

  @doc false
  @spec hold_preparation([atom()], map(), map(), {pid(), integer()}) :: :ok
  def hold_preparation(_event, _measurements, _metadata, {owner, deadline}) do
    if self() == owner and not Process.get({__MODULE__, :preparation_held}, false) do
      Process.put({__MODULE__, :preparation_held}, true)

      receive do
      after
        max(deadline - System.monotonic_time(:millisecond) + 1, 1) -> :ok
      end

      send(owner, {:preparation_budget_elapsed, System.monotonic_time(:millisecond)})
    end

    :ok
  end

  defp await_first_drip(connection, ref) do
    assert {:ok, connection, events} = Mint.HTTP.recv(connection, 0, 5_000)
    if Enum.any?(events, &match?({:data, ^ref, _}, &1)), do: connection, else: await_first_drip(connection, ref)
  end

  defp drain_drip(connection) do
    case Mint.HTTP.recv(connection, 0, :infinity) do
      {:ok, connection, _events} -> drain_drip(connection)
      {:error, _connection, error, _events} -> {:unexpected_close, error}
    end
  end

  defp drain_closed_socket(socket, count) do
    case :gen_tcp.recv(socket, 0, 5_000) do
      {:ok, data} -> drain_closed_socket(socket, count + byte_size(data))
      {:error, :closed} -> count
    end
  end

  defp await_tls_close(socket, timeout \\ 5_000) do
    case :gen_tcp.recv(socket, 0, timeout) do
      {:ok, _tls_alert} -> await_tls_close(socket, timeout)
      {:error, reason} -> {:error, reason}
    end
  end

  defp forward_trace(parent) do
    receive do
      message ->
        send(parent, {:observed, message})
        forward_trace(parent)
    end
  end

  defp fixture!(context, mode) do
    server = start_supervised!({Bandit, plug: {Receiver, {self(), mode}}, scheme: :https, port: 0, ip: context.ip, startup_log: false, thousand_island_options: [transport_options: [cert: context.cert[:cert], key: context.cert[:key]]]})
    {:ok, {_, port}} = ThousandIsland.listener_info(server)
    pool = pool_fixture()
    {:ok, channel} = Alerts.create_channel(context.scope, %{channel_type: "webhook", display_name: "deadline", endpoint_url: "https://#{context.host}:#{port}/hooks", webhook_signing_secret: "synthetic-deadline-secret"})
    incident = alert_incident_fixture(pool: pool)
    queue = "deadline_#{System.unique_integer([:positive])}"
    job = %{"alert_incident_id" => incident.id, "alert_channel_id" => channel.id} |> AlertDeliveryWorker.new(queue: queue) |> Repo.insert!()
    conf = Oban.config()
    {:ok, meta} = Basic.init(conf, queue: queue, limit: 1)
    {:ok, {_meta, [claimed]}} = Basic.fetch_jobs(conf, meta, %{})
    assert claimed.id == job.id
    %{server: server, incident: incident, channel: channel, job: claimed, url: "https://#{context.host}:#{port}/hooks"}
  end
end
