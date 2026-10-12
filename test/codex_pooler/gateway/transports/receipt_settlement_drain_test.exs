defmodule CodexPooler.Gateway.Transports.Websocket.ReceiptSettlementDrainTest do
  use ExUnit.Case, async: false

  import CodexPooler.Gateway.Transports.WebsocketRolloutDrainSupport

  alias CodexPooler.Gateway.Transports.Websocket.{ActivityDrain, ActivityRegistry, RolloutDrain}
  alias CodexPooler.Gateway.Transports.WebsocketRolloutDrainSupport.VirtualDeadline

  @detection_ms 5_000

  setup do
    registry = :"receipt-registry-#{System.unique_integer([:positive])}"
    start_supervised!({ActivityRegistry, name: registry})
    supervisor = start_supervised!({Task.Supervisor, []})
    %{registry: registry, supervisor: supervisor, identity: %{request_id: Ecto.UUID.generate(), attempt_id: Ecto.UUID.generate(), replay_generation: 0}}
  end

  test "an initially empty receipt cohort waits for the accepted producer handoff", ctx do
    %{registry: registry, identity: identity} = ctx
    assert {:ok, producer} = ActivityRegistry.begin_receipt_producer(identity, name: registry)
    assert {_epoch, []} = ActivityRegistry.begin_drain(name: registry)
    assert %{closed?: false, entries: [], pending_producers: 1} = ActivityRegistry.receipt_cohort(name: registry)
    worker = worker(ctx)
    assert {:ok, token} = ActivityRegistry.register_receipt_settlement(producer, identity, worker, name: registry)
    assert :ok = ActivityRegistry.finish_receipt_producer(producer, name: registry)
    assert %{closed?: true, entries: [%{token: ^token, status: :active}]} = ActivityRegistry.receipt_cohort(name: registry)
    assert {:error, :producer_closed} = ActivityRegistry.register_receipt_settlement(producer, identity, worker, name: registry)
    initialize(worker, token)
    complete(worker, :inserted)
    assert %{entries: [%{status: {:finished, :completed}, receipt_disposition: :inserted}]} = ActivityRegistry.receipt_cohort(name: registry)
  end

  test "the worker cannot settle before registration and explicit initialization", ctx do
    worker = worker(ctx)
    assert_receive {:worker_waiting, ^worker}, @detection_ms
    assert ActivityRegistry.activities(name: ctx.registry) == []
    {:ok, producer} = ActivityRegistry.begin_receipt_producer(ctx.identity, name: ctx.registry)
    {:ok, token} = ActivityRegistry.register_receipt_settlement(producer, ctx.identity, worker, name: ctx.registry)
    assert [%{token: ^token, pid: ^worker, kind: :receipt_settlement}] = ActivityRegistry.activities(name: ctx.registry)
    initialize(worker, token)
    assert_receive {:worker_initialized, ^worker, {:active, :admitted}}, @detection_ms
    complete(worker, :reused)
    :ok = ActivityRegistry.finish_receipt_producer(producer, name: ctx.registry)
  end

  for field <- [:request_id, :attempt_id, :replay_generation] do
    test "receipt registration rejects a changed #{field}", ctx do
      {:ok, producer} = ActivityRegistry.begin_receipt_producer(ctx.identity, name: ctx.registry)
      value = if unquote(field) == :replay_generation, do: 1, else: Ecto.UUID.generate()
      identity = Map.put(ctx.identity, unquote(field), value)
      worker = worker(ctx)
      assert {:error, :identity_mismatch} = ActivityRegistry.register_receipt_settlement(producer, identity, worker, name: ctx.registry)
      assert ActivityRegistry.activities(name: ctx.registry) == []
      :ok = ActivityRegistry.finish_receipt_producer(producer, name: ctx.registry)
    end
  end

  test "malformed or empty admission identities do not create producers", ctx do
    for identity <- [%{}, %{ctx.identity | request_id: ""}, %{ctx.identity | attempt_id: nil}, %{ctx.identity | replay_generation: -1}, Map.put(ctx.identity, :untrusted, true)] do
      assert {:error, :invalid_identity} = ActivityRegistry.begin_receipt_producer(identity, name: ctx.registry)
    end

    ActivityRegistry.begin_drain(name: ctx.registry)
    assert %{closed?: true, pending_producers: 0, entries: []} = ActivityRegistry.receipt_cohort(name: ctx.registry)
  end

  test "final drain closure cannot erase an unfinished producer or receipt", ctx do
    {:ok, producer} = ActivityRegistry.begin_receipt_producer(ctx.identity, name: ctx.registry)
    {epoch, []} = ActivityRegistry.begin_drain(name: ctx.registry)
    assert {:error, :receipt_work_pending} = ActivityRegistry.complete_drain(epoch, name: ctx.registry)
    worker = worker(ctx)
    {:ok, token} = ActivityRegistry.register_receipt_settlement(producer, ctx.identity, worker, name: ctx.registry)
    :ok = ActivityRegistry.finish_receipt_producer(producer, name: ctx.registry)
    assert {:error, :receipt_work_pending} = ActivityRegistry.complete_drain(epoch, name: ctx.registry)
    initialize(worker, token)
    complete(worker, :inserted)
    assert :ok = ActivityRegistry.complete_drain(epoch, name: ctx.registry)
  end

  test "receipt work registered before drain is included even after its producer closes", ctx do
    {:ok, producer} = ActivityRegistry.begin_receipt_producer(ctx.identity, name: ctx.registry)
    worker = worker(ctx)
    {:ok, token} = ActivityRegistry.register_receipt_settlement(producer, ctx.identity, worker, name: ctx.registry)
    :ok = ActivityRegistry.finish_receipt_producer(producer, name: ctx.registry)
    assert {_epoch, []} = ActivityRegistry.begin_drain(name: ctx.registry)
    assert %{closed?: true, entries: [%{token: ^token, status: :active}]} = ActivityRegistry.receipt_cohort(name: ctx.registry)
    initialize(worker, token)
    complete(worker, :reused)
  end

  test "another process cannot use an owner's producer capability", ctx do
    {:ok, producer} = ActivityRegistry.begin_receipt_producer(ctx.identity, name: ctx.registry)
    worker = worker(ctx)
    result = Task.async(fn -> ActivityRegistry.register_receipt_settlement(producer, ctx.identity, worker, name: ctx.registry) end) |> Task.await()
    assert result == {:error, :producer_closed}
    assert :ok = ActivityRegistry.finish_receipt_producer(producer, name: ctx.registry)
  end

  test "a duplicate handoff reuses its token and a second worker is rejected", ctx do
    {:ok, producer} = ActivityRegistry.begin_receipt_producer(ctx.identity, name: ctx.registry)
    first = worker(ctx)
    second = worker(ctx)
    {:ok, token} = ActivityRegistry.register_receipt_settlement(producer, ctx.identity, first, name: ctx.registry)
    assert {:ok, ^token} = ActivityRegistry.register_receipt_settlement(producer, ctx.identity, first, name: ctx.registry)
    assert {:error, :already_registered} = ActivityRegistry.register_receipt_settlement(producer, ctx.identity, second, name: ctx.registry)
    :ok = ActivityRegistry.finish_receipt_producer(producer, name: ctx.registry)
  end

  test "closed registration rejects new producer admission and old handoffs", ctx do
    {:ok, producer} = ActivityRegistry.begin_receipt_producer(ctx.identity, name: ctx.registry)
    ActivityRegistry.begin_drain(name: ctx.registry)
    :ok = ActivityRegistry.finish_receipt_producer(producer, name: ctx.registry)
    assert %{closed?: true, entries: []} = ActivityRegistry.receipt_cohort(name: ctx.registry)
    assert {:error, :owner_drained} = ActivityRegistry.begin_receipt_producer(ctx.identity, name: ctx.registry)
    assert {:error, :producer_closed} = ActivityRegistry.register_receipt_settlement(producer, ctx.identity, worker(ctx), name: ctx.registry)
  end

  for after_handoff? <- [false, true] do
    test "owner death #{if after_handoff?, do: "after", else: "before"} handoff retains an explicit cohort outcome", ctx do
      parent = self()
      worker = worker(ctx)

      {:ok, owner} =
        Task.Supervisor.start_child(ctx.supervisor, fn ->
          {:ok, producer} = ActivityRegistry.begin_receipt_producer(ctx.identity, name: ctx.registry)
          send(parent, {:producer_ready, self()})

          receive do
            :handoff -> :ok
          end

          token =
            if unquote(after_handoff?) do
              {:ok, token} = ActivityRegistry.register_receipt_settlement(producer, ctx.identity, worker, name: ctx.registry)
              token
            end

          send(parent, {:owner_finished, token})
        end)

      assert_receive {:producer_ready, ^owner}, @detection_ms
      ActivityRegistry.begin_drain(name: ctx.registry)
      monitor = Process.monitor(owner)
      send(owner, :handoff)
      assert_receive {:owner_finished, token}, @detection_ms
      assert_receive {:DOWN, ^monitor, :process, ^owner, :normal}, @detection_ms
      cohort = await_closed(ctx.registry)

      if unquote(after_handoff?) do
        assert cohort.producers_failed? == false
        assert [%{token: ^token, status: :active}] = cohort.entries
        initialize(worker, token)
        complete(worker, :replaced)
        assert %{entries: [%{status: {:finished, :completed}}]} = ActivityRegistry.receipt_cohort(name: ctx.registry)
      else
        assert cohort.producers_failed?
        assert cohort.entries == []
      end
    end
  end

  test "the cohort cancels at cutoff and force cuts at the same absolute settlement margin", ctx do
    {:ok, producer} = ActivityRegistry.begin_receipt_producer(ctx.identity, name: ctx.registry)
    worker = worker(ctx)
    {:ok, token} = ActivityRegistry.register_receipt_settlement(producer, ctx.identity, worker, name: ctx.registry)
    initialize(worker, token)
    :ok = ActivityRegistry.finish_receipt_producer(producer, name: ctx.registry)
    ActivityRegistry.begin_drain(name: ctx.registry)
    deadline = start_virtual_deadline(self())
    options = deadline_options(deadline)
    policy = Keyword.fetch!(options, :deadline) |> Map.put(:owner_post_deadline_call_budget_ms, 1_000)
    drain = Task.async(fn -> ActivityDrain.drain_receipts(100, policy, ctx.registry, fn _ -> :ok end) end)
    assert_receive {:rollout_drain_deadline_wait, ^deadline, 100}, @detection_ms
    VirtualDeadline.advance(deadline, 100)
    assert_receive {:worker_cancelled, ^worker, ^token}, @detection_ms
    assert_receive {:rollout_drain_deadline_wait, ^deadline, 200}, @detection_ms
    VirtualDeadline.advance(deadline, 999)
    assert_receive {:rollout_drain_deadline_wait, ^deadline, 1}, @detection_ms
    assert Process.alive?(worker)
    monitor = Process.monitor(worker)
    VirtualDeadline.advance(deadline, 1)
    assert %{closed?: true, entries: [%{status: {:finished, :aborted}}]} = Task.await(drain, @detection_ms)
    assert_receive {:DOWN, ^monitor, :process, ^worker, :killed}, @detection_ms
  end

  test "settlement completed after cutoff stays completed within the shared margin", ctx do
    {:ok, producer} = ActivityRegistry.begin_receipt_producer(ctx.identity, name: ctx.registry)
    worker = worker(ctx)
    {:ok, token} = ActivityRegistry.register_receipt_settlement(producer, ctx.identity, worker, name: ctx.registry)
    initialize(worker, token)
    :ok = ActivityRegistry.finish_receipt_producer(producer, name: ctx.registry)
    ActivityRegistry.begin_drain(name: ctx.registry)
    deadline = start_virtual_deadline(self())
    policy = deadline_options(deadline) |> Keyword.fetch!(:deadline) |> Map.put(:owner_post_deadline_call_budget_ms, 1_000)
    drain = Task.async(fn -> ActivityDrain.drain_receipts(100, policy, ctx.registry, fn _ -> :ok end) end)
    assert_receive {:rollout_drain_deadline_wait, ^deadline, 100}, @detection_ms
    VirtualDeadline.advance(deadline, 100)
    assert_receive {:worker_cancelled, ^worker, ^token}, @detection_ms
    assert_receive {:rollout_drain_deadline_wait, ^deadline, 200}, @detection_ms
    complete(worker, :inserted)
    VirtualDeadline.advance(deadline, 200)
    assert %{closed?: true, entries: [%{status: {:finished, :completed}, receipt_disposition: :inserted}]} = Task.await(drain, @detection_ms)
  end

  test "an unresolved producer is forced closed without a misleading successful empty cohort", ctx do
    {:ok, producer} = ActivityRegistry.begin_receipt_producer(ctx.identity, name: ctx.registry)
    ActivityRegistry.begin_drain(name: ctx.registry)
    :ok = ActivityRegistry.force_receipt_cohort(name: ctx.registry)
    assert %{closed?: true, producers_failed?: true, pending_producers: 0, entries: []} = ActivityRegistry.receipt_cohort(name: ctx.registry)
    assert {:error, :producer_closed} = ActivityRegistry.register_receipt_settlement(producer, ctx.identity, worker(ctx), name: ctx.registry)
  end

  for producer_end <- [:no_receipt, :death] do
    test "the coordinator reports an empty cohort after producer #{producer_end}", ctx do
      owner_registry = :"receipt-owner-registry-#{System.unique_integer([:positive])}"
      start_supervised!({Registry, keys: :unique, name: owner_registry})
      harness = start_rollout_drain_harness(self(), owner_registry: owner_registry)
      parent = self()

      {:ok, owner} =
        Task.Supervisor.start_child(ctx.supervisor, fn ->
          {:ok, producer} = ActivityRegistry.begin_receipt_producer(ctx.identity, name: harness.activity_registry)
          send(parent, {:producer_ready, self()})

          receive do
            :finish -> :ok
          end

          if unquote(producer_end) == :no_receipt, do: ActivityRegistry.finish_receipt_producer(producer, name: harness.activity_registry)
        end)

      assert_receive {:producer_ready, ^owner}, @detection_ms
      drain = Task.async(fn -> RolloutDrain.start_drain([name: harness.name, timeout_ms: 5_000] ++ deadline_options(harness.deadline)) end)
      deadline = harness.deadline
      assert_receive {:rollout_drain_deadline_wait, ^deadline, _}, @detection_ms
      monitor = Process.monitor(owner)
      send(owner, :finish)
      assert_receive {:DOWN, ^monitor, :process, ^owner, :normal}, @detection_ms
      await_closed(harness.activity_registry)
      VirtualDeadline.advance(deadline, 200)
      summary = Task.await(drain, @detection_ms)
      assert summary.receipt_settlements_seen == 0
      assert summary.receipt_producers_failed? == (unquote(producer_end) == :death)
      assert summary.result == if(unquote(producer_end) == :death, do: :error, else: :ok)
      assert :ok = await_rollout_drain_harness(harness)
    end
  end

  for {disposition, result, outcome} <- [{:inserted, :ok, :completed}, {:reused, :ok, :completed}, {:replaced, :ok, :completed}, {:superseded, :ok, :superseded}, {:failed, :error, :failed}, {:aborted, :error, :aborted}, {:crash, :error, :failed}] do
    test "the real coordinator includes late #{disposition} receipt work without inflating turn totals", ctx do
      owner_registry = :"receipt-owner-registry-#{System.unique_integer([:positive])}"
      start_supervised!({Registry, keys: :unique, name: owner_registry})
      harness = start_rollout_drain_harness(self(), owner_registry: owner_registry)
      ctx = %{ctx | registry: harness.activity_registry}
      {:ok, producer} = ActivityRegistry.begin_receipt_producer(ctx.identity, name: ctx.registry)
      drain = Task.async(fn -> RolloutDrain.start_drain([name: harness.name, timeout_ms: 5_000] ++ deadline_options(harness.deadline)) end)
      deadline = harness.deadline
      assert_receive {:rollout_drain_deadline_wait, ^deadline, _}, @detection_ms
      assert %{entries: [], closed?: false, pending_producers: 1} = ActivityRegistry.receipt_cohort(name: ctx.registry)
      worker = worker(ctx)
      {:ok, token} = ActivityRegistry.register_receipt_settlement(producer, ctx.identity, worker, name: ctx.registry)
      :ok = ActivityRegistry.finish_receipt_producer(producer, name: ctx.registry)
      initialize(worker, token)

      if unquote(disposition) == :crash do
        monitor = Process.monitor(worker)
        Process.exit(worker, :kill)
        assert_receive {:DOWN, ^monitor, :process, ^worker, :killed}, @detection_ms
        await_finished(ctx.registry, token)
      else
        complete(worker, unquote(disposition))
      end

      VirtualDeadline.advance(deadline, 200)
      summary = Task.await(drain, @detection_ms)
      assert summary.result == unquote(result)
      assert summary.receipt_settlements_seen == 1
      assert summary[unquote(String.to_atom("receipt_settlements_#{outcome}"))] == 1
      if unquote(disposition) == :superseded, do: assert(summary.receipt_settlements_completed == 0)
      assert summary.direct_turns_seen == 0
      assert summary.proxy_turns_seen == 0
      assert summary.turns_completed == 0
      assert summary.turns_aborted == 0
      assert :ok = await_rollout_drain_harness(harness)
    end
  end

  defp worker(ctx) do
    parent = self()

    {:ok, worker} =
      Task.Supervisor.start_child(ctx.supervisor, fn ->
        send(parent, {:worker_waiting, self()})

        receive do
          {:initialize, token} ->
            send(parent, {:worker_initialized, self(), ActivityRegistry.status(token, name: ctx.registry)})
            worker_loop(parent, ctx.registry, token)
        end
      end)

    worker
  end

  defp worker_loop(parent, registry, token) do
    receive do
      {:complete, disposition} ->
        :ok = ActivityRegistry.complete_receipt_settlement(token, disposition, name: registry)
        :ok = ActivityRegistry.complete_receipt_settlement(token, disposition, name: registry)
        send(parent, {:worker_completed, self()})

      {:websocket_activity_cancel, ^token, :owner_drained} ->
        send(parent, {:worker_cancelled, self(), token})
        worker_loop(parent, registry, token)
    end
  end

  defp initialize(worker, token), do: send(worker, {:initialize, token})

  defp complete(worker, disposition) do
    monitor = Process.monitor(worker)
    send(worker, {:complete, disposition})
    assert_receive {:worker_completed, ^worker}, @detection_ms
    assert_receive {:DOWN, ^monitor, :process, ^worker, :normal}, @detection_ms
  end

  defp await_closed(registry), do: await_state(fn -> ActivityRegistry.receipt_cohort(name: registry) end, & &1.closed?)
  defp await_finished(registry, token), do: await_state(fn -> ActivityRegistry.status(token, name: registry) end, &match?({:finished, _}, &1))

  defp await_state(read, ready?), do: await_state(read, ready?, System.monotonic_time(:millisecond) + @detection_ms)

  defp await_state(read, ready?, until_ms) do
    state = read.()

    cond do
      ready?.(state) -> state
      System.monotonic_time(:millisecond) >= until_ms -> flunk("registry transition missing: #{inspect(state)}")
      true -> await_state(read, ready?, until_ms)
    end
  end
end
