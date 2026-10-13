defmodule CodexPooler.Platform.ExecutionProofPublisherTest do
  # findings#283: an execution that ends `process_down`, without delivering its
  # result, is the executor a client's resend of its turn waits on, so its
  # registry asks the publisher to write its proof within 100 ms instead of
  # at the next tick. Each test pairs its own registry with its own publisher,
  # whose tick is far beyond the test: only an early publication can write a
  # proof, and the global registry other tests use is left alone.
  use CodexPooler.DataCase, async: false

  import ExUnit.CaptureLog

  alias CodexPooler.ExecutionProofSupport
  alias CodexPooler.Platform.{ExecutionProofPublisher, ExecutionRegistry, ExecutionTerminalProof, ExecutionTerminalProofs}
  alias CodexPooler.Platform.InstancePresence.Identity

  @tick_ms 60_000
  # One fixed name: the file is synchronous, so no two tests hold it at once.
  @registry :execution_proof_publisher_test_registry
  @detection_timeout_ms 2_000

  test "an execution that ends without delivering is published without waiting for the tick" do
    {registry, _publisher} = start_pair!()
    execution = start_execution!(registry)

    Process.exit(execution.pid, :kill)

    :ok = await_published!(execution.identity)
    assert %ExecutionTerminalProof{end_kind: "process_down"} = Repo.get!(ExecutionTerminalProof, execution.identity.owner_execution_id)
  end

  # A backlog of older pending proofs (retained through a database outage, or
  # left by earlier tests in this VM, where the application's publisher is
  # off) does not hold the early publication back: it writes the execution
  # that asked for it and leaves the backlog to the tick, which writes the
  # oldest hundred at a time. It used to write the oldest hundred, so the
  # asking execution waited one tick per hundred older proofs (Drone 1707).
  test "an execution that ends without delivering is published ahead of an older backlog" do
    registry = start_supervised!({ExecutionRegistry, name: nil})

    for _ <- 1..250 do
      id = Ecto.UUID.generate()
      :ok = ExecutionRegistry.register(id, registry)
      :ok = ExecutionRegistry.complete(id, registry)
    end

    publisher = ExecutionProofSupport.start_publisher!(enabled: true, name: nil, registry: registry, interval_ms: @tick_ms)
    :sys.get_state(publisher)
    execution = start_execution!(registry)
    Process.exit(execution.pid, :kill)

    :ok = await_published!(execution.identity)
  end

  test "a delivered execution is published without waiting for the tick" do
    {registry, _publisher} = start_pair!()
    execution = start_execution!(registry)

    send(execution.pid, :complete)
    assert_receive {:completed, _id}
    :ok = await_published!(execution.identity)
    assert %ExecutionTerminalProof{end_kind: "completed"} = Repo.get!(ExecutionTerminalProof, execution.identity.owner_execution_id)
  end

  # A burst of ends shares publications: each execution here ends only after
  # the publisher has read the previous end's request, which without the
  # shared window would cost one transaction per execution. The fifty ends
  # take a few milliseconds, well inside one window.
  test "executions ending one after another share a publication" do
    {registry, publisher} = start_pair!()
    executions = for _ <- 1..50, do: start_execution!(registry)
    publications = count_publications(publisher)

    for execution <- executions do
      Process.exit(execution.pid, :kill)
      assert :dead = ExecutionRegistry.status(execution.identity.owner_execution_id, execution.pid, registry)
      :sys.get_state(publisher)
    end

    Enum.each(executions, &(:ok = await_published!(&1.identity)))
    assert publications.() in 1..2
  end

  test "a failed older proof does not block a later exact executor proof" do
    {registry, publisher} = start_pair!()

    # A stored proof that disagrees with a pending one fails every publication.
    poisoned = start_execution!(registry)
    {id, identity} = Map.pop(poisoned.identity, :owner_execution_id)
    Repo.insert_all(ExecutionTerminalProof, [Map.merge(identity, %{execution_id: id, end_kind: "process_down", ended_at: DateTime.add(DateTime.utc_now(), -60, :second)})])

    log =
      capture_log(fn ->
        send(poisoned.pid, :complete)
        assert_receive {:completed, _id}
        send(publisher, :publish)
        assert %{failed: true} = :sys.get_state(publisher)
      end)

    assert log =~ "execution terminal proof publication unavailable; pending proofs retained: 1\n"

    execution = start_execution!(registry)
    monitor = Process.monitor(execution.pid)
    Process.exit(execution.pid, :kill)
    assert_receive {:DOWN, ^monitor, :process, _pid, :killed}
    :ok = await_published!(execution.identity)
    assert ExecutionRegistry.pending_proofs([id], registry) != []
    assert ExecutionRegistry.pending_proofs([execution.identity.owner_execution_id], registry) == []
  end

  test "a failed newer proof does not starve an older proof after the publisher restarts" do
    # Runs after publisher cleanup but before the sandbox owner is stopped.
    on_exit(fn -> assert %{rows: [[1]]} = Repo.query!("SELECT 1") end)
    registry = start_supervised!({ExecutionRegistry, name: nil})
    older = start_execution!(registry)
    send(older.pid, :complete)
    assert_receive {:completed, _id}
    newer = start_execution!(registry)
    {id, identity} = Map.pop(newer.identity, :owner_execution_id)
    Repo.insert_all(ExecutionTerminalProof, [Map.merge(identity, %{execution_id: id, end_kind: "process_down", ended_at: DateTime.add(DateTime.utc_now(), -60, :second)})])
    send(newer.pid, :complete)
    assert_receive {:completed, ^id}

    {publisher, log} =
      with_log(fn ->
        publisher = ExecutionProofSupport.start_publisher!(enabled: true, name: nil, registry: registry, interval_ms: @tick_ms)
        :ok = await_published!(older.identity)
        publisher
      end)

    assert log =~ "execution terminal proof publication unavailable; pending proofs retained: 2\n"
    assert ExecutionRegistry.pending_proofs([id], registry) != []
    hold_publication_until_cleanup!(publisher)
  end

  # The warning claims only what the registry holds: a publisher that cannot reach its registry cannot say, so it names no
  # retained proofs. The registry named here is never started; the Repo is up, so the failure is the registry's alone.
  test "a publisher that cannot reach its registry warns without claiming retained proofs" do
    log =
      capture_log(fn ->
        publisher = ExecutionProofSupport.start_publisher!(enabled: true, name: nil, registry: :execution_proof_publisher_test_absent_registry, interval_ms: @tick_ms)
        assert %{failed: true} = :sys.get_state(publisher)
      end)

    assert log =~ "execution terminal proof publication unavailable\n"
    refute log =~ "retained"
  end

  # The publisher renews its subscription at every publication, so a registry
  # its supervisor restarted, which knows no subscriber, asks for early
  # publications again after the publisher's next tick.
  test "a restarted registry asks for early publications again after the next tick" do
    registry = start_supervised!({ExecutionRegistry, name: @registry})
    publisher = ExecutionProofSupport.start_publisher!(enabled: true, name: nil, registry: @registry, interval_ms: @tick_ms)
    :sys.get_state(publisher)

    ref = Process.monitor(registry)
    Process.exit(registry, :kill)
    assert_receive {:DOWN, ^ref, :process, ^registry, :killed}
    :ok = await_restarted!(registry, System.monotonic_time(:millisecond) + @detection_timeout_ms)

    send(publisher, :publish)
    :sys.get_state(publisher)
    execution = start_execution!(@registry)
    Process.exit(execution.pid, :kill)

    :ok = await_published!(execution.identity)
  end

  # findings#270 row 270-371: the application stops the publisher right after
  # its shutdown drain, before an early publication could run, so
  # `prep_stop/1` flushes: the proof is written when the flush returns.
  test "a flush writes the proof of an ended execution before it returns" do
    {registry, publisher} = start_pair!()
    execution = start_execution!(registry)
    monitor = Process.monitor(execution.pid)
    Process.exit(execution.pid, :kill)
    assert_receive {:DOWN, ^monitor, :process, _pid, :killed}

    assert :ok = ExecutionProofPublisher.flush(@detection_timeout_ms, publisher)
    assert %ExecutionTerminalProof{end_kind: "process_down"} = Repo.get!(ExecutionTerminalProof, execution.identity.owner_execution_id)
  end

  # The drain that ended an execution saw its process go down, but nothing
  # orders that signal before the one the registry receives. Here the
  # registry's monitor is dropped, so its `:DOWN` never comes: the flush
  # retires the execution from its process being gone.
  test "a flush publishes an execution whose end the registry has not heard of" do
    {registry, publisher} = start_pair!()
    execution = start_execution!(registry)
    id = execution.identity.owner_execution_id

    :sys.replace_state(registry, fn state ->
      {_pid, ref} = Map.fetch!(state.entries, id)
      true = Process.demonitor(ref, [:flush])
      state
    end)

    monitor = Process.monitor(execution.pid)
    Process.exit(execution.pid, :kill)
    assert_receive {:DOWN, ^monitor, :process, _pid, :killed}
    assert %{entries: %{^id => {_pid, ref}}} = :sys.get_state(registry)
    assert is_reference(ref)

    assert :ok = ExecutionProofPublisher.flush(@detection_timeout_ms, publisher)
    assert %ExecutionTerminalProof{end_kind: "process_down"} = Repo.get!(ExecutionTerminalProof, id)
  end

  test "a flush with nothing pending touches no database" do
    {_registry, publisher} = start_pair!()
    queries = count_queries(publisher)

    assert :ok = ExecutionProofPublisher.flush(@detection_timeout_ms, publisher)
    assert queries.() == 0
  end

  # A publisher that cannot answer, as behind a stalled database, holds the
  # VM's exit only for the budget the flush was given; none is none.
  test "a flush returns within its budget when the publisher cannot answer" do
    {_registry, publisher} = start_pair!()
    :ok = :sys.suspend(publisher)

    assert :no_budget = ExecutionProofPublisher.flush(0, publisher)

    log =
      capture_log(fn ->
        flush = Task.async(fn -> ExecutionProofPublisher.flush(100, publisher) end)
        assert {:ok, :timeout} = Task.yield(flush, @detection_timeout_ms)
      end)

    assert log =~ "execution terminal proof flush timed out before the VM exit; pending proofs retained"
    :ok = :sys.resume(publisher)
  end

  defp await_restarted!(previous, deadline) do
    case Process.whereis(@registry) do
      pid when is_pid(pid) and pid != previous ->
        :ok

      _absent_or_previous ->
        if System.monotonic_time(:millisecond) >= deadline, do: flunk("the registry was not restarted")

        receive do
        after
          5 -> await_restarted!(previous, deadline)
        end
    end
  end

  defp start_pair! do
    registry = start_supervised!({ExecutionRegistry, name: nil})
    publisher = ExecutionProofSupport.start_publisher!(enabled: true, name: nil, registry: registry, interval_ms: @tick_ms)
    # Its first publication, which subscribes it to the registry, runs before
    # it reads any message.
    :sys.get_state(publisher)
    {registry, publisher}
  end

  # An already-visible proof does not mean the next retry released the shared
  # connection. End this test with a real publication transaction in flight.
  defp hold_publication_until_cleanup!(publisher) do
    handler = {__MODULE__, make_ref()}
    config = %{publisher: publisher, test_pid: self(), handler: handler}

    on_exit(fn ->
      send(publisher, {handler, :release})
      :telemetry.detach(handler)
    end)

    :ok = :telemetry.attach(handler, [:codex_pooler, :repo, :query], &__MODULE__.hold_publication/4, config)
    send(publisher, :publish)
    assert_receive {:publication_transaction_held, ^handler, ^publisher}, @detection_timeout_ms
  end

  @doc false
  def hold_publication(_event, _measurements, %{source: "execution_terminal_proofs", query: "INSERT" <> _}, %{publisher: publisher} = config) when self() == publisher do
    :ok = :telemetry.detach(config.handler)
    true = Repo.in_transaction?()
    send(config.test_pid, {:publication_transaction_held, config.handler, self()})
    handler = config.handler

    receive do
      {^handler, :release} -> :ok
    after
      @detection_timeout_ms -> raise "publication transaction was not released by test cleanup"
    end
  end

  def hold_publication(_event, _measurements, _metadata, _config), do: :ok

  # One execution registered from its own process, as a response task
  # registers the one it runs; `:complete` retires it as delivered.
  defp start_execution!(registry) do
    test_pid = self()
    id = Ecto.UUID.generate()

    pid =
      spawn(fn ->
        :ok = ExecutionRegistry.register(id, registry)
        send(test_pid, {:registered, id})

        receive do
          :complete ->
            :ok = ExecutionRegistry.complete(id, registry)
            send(test_pid, {:completed, id})
        end
      end)

    assert_receive {:registered, ^id}
    local = Identity.local()

    %{
      pid: pid,
      identity: %{
        owner_execution_id: id,
        owner_instance_id: local.node_name,
        owner_instance_boot_id: local.boot_id,
        owner_process_id: List.to_string(:erlang.pid_to_list(pid))
      }
    }
  end

  defp await_published!(identity),
    do: await_published(identity, System.monotonic_time(:millisecond) + @detection_timeout_ms)

  defp await_published(identity, deadline) do
    cond do
      ExecutionTerminalProofs.terminal?(identity) ->
        :ok

      System.monotonic_time(:millisecond) >= deadline ->
        flunk("execution terminal proof was not published before the publisher's tick")

      true ->
        receive do
        after
          5 -> await_published(identity, deadline)
        end
    end
  end

  # Publications that wrote proofs: the publisher's inserts into the table.
  defp count_publications(publisher) do
    handler = {__MODULE__, make_ref()}
    config = %{publisher: publisher, test_pid: self(), handler: handler}
    :ok = :telemetry.attach(handler, [:codex_pooler, :repo, :query], &__MODULE__.observe_query/4, config)
    on_exit(fn -> :telemetry.detach(handler) end)
    fn -> drain_count(handler, 0) end
  end

  @doc false
  def observe_query(_event, _measurements, %{source: "execution_terminal_proofs", query: "INSERT" <> _}, %{publisher: publisher} = config)
      when self() == publisher,
      do: send(config.test_pid, {:proof_insert, config.handler})

  def observe_query(_event, _measurements, _metadata, _config), do: :ok

  # Every query the publisher runs, whatever it reads or writes.
  defp count_queries(publisher) do
    handler = {__MODULE__, make_ref()}
    config = %{publisher: publisher, test_pid: self(), handler: handler}
    :ok = :telemetry.attach(handler, [:codex_pooler, :repo, :query], &__MODULE__.observe_any_query/4, config)
    on_exit(fn -> :telemetry.detach(handler) end)
    fn -> drain_query_count(handler, 0) end
  end

  @doc false
  def observe_any_query(_event, _measurements, _metadata, %{publisher: publisher} = config) when self() == publisher,
    do: send(config.test_pid, {:publisher_query, config.handler})

  def observe_any_query(_event, _measurements, _metadata, _config), do: :ok

  defp drain_query_count(handler, count) do
    receive do
      {:publisher_query, ^handler} -> drain_query_count(handler, count + 1)
    after
      0 -> count
    end
  end

  defp drain_count(handler, count) do
    receive do
      {:proof_insert, ^handler} -> drain_count(handler, count + 1)
    after
      0 -> count
    end
  end
end
