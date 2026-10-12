defmodule CodexPooler.Gateway.Transports.Websocket.ReceiptSettlementDrainTest do
  use ExUnit.Case, async: false

  import CodexPooler.Gateway.Transports.WebsocketRolloutDrainSupport

  alias CodexPooler.Gateway.Transports.Websocket.{ActivityDrain, ActivityRegistry, RolloutDrain}
  alias CodexPooler.Gateway.Transports.WebsocketRolloutDrainSupport.VirtualDeadline
  alias CodexPooler.Gateway.Websocket.DeliveryReceipt
  alias CodexPooler.Gateway.Websocket.DirectCleanup

  @detection_ms 5_000

  setup do
    registry = :"receipt-registry-#{System.unique_integer([:positive])}"
    start_supervised!({ActivityRegistry, name: registry})
    supervisor = start_supervised!({Task.Supervisor, []})
    %{registry: registry, supervisor: supervisor, identity: %{request_id: Ecto.UUID.generate(), attempt_id: Ecto.UUID.generate(), replay_generation: 0}}
  end

  test "delivery metadata holds the producer barrier without counting another usage worker or turn", ctx do
    {:ok, token} = ActivityRegistry.enroll_delivery_receipt(self(), ctx.identity, name: ctx.registry)
    assert_receive {:downstream_receipt_enrolled, task, identity, ^token}
    assert task == self() and identity == ctx.identity
    assert {_epoch, []} = ActivityRegistry.begin_drain(name: ctx.registry)
    assert %{closed?: false, entries: [], pending_producers: 1} = ActivityRegistry.receipt_cohort(name: ctx.registry)
    assert ActivityRegistry.activities(name: ctx.registry) == []
    receipt = DeliveryReceipt.build(%{outcome: "aborted"})
    assert :ok = ActivityRegistry.publish_delivery_receipt(token, ctx.identity, receipt, {:native, self(), ctx.identity, :aborted}, name: ctx.registry)
    assert_receive {:"$gen_cast", {:delivery_complete, ^identity, :aborted, envelope}}
    assert Map.keys(envelope) |> Enum.sort() == [:context, :receipt, :registry, :token]
    assert :ok = ActivityRegistry.claim_delivery_receipt(token, name: ctx.registry)
    assert :ok = ActivityRegistry.finish_delivery_receipt(token, :ok, name: ctx.registry)
    assert %{closed?: true, entries: [], pending_producers: 0, producers_failed?: false} = ActivityRegistry.receipt_cohort(name: ctx.registry)
  end

  test "delivery handoff rejects a foreign caller and a changed admitted generation", ctx do
    {:ok, token} = ActivityRegistry.enroll_delivery_receipt(self(), ctx.identity, name: ctx.registry)
    receipt = DeliveryReceipt.build(%{outcome: "aborted"})
    assert {:error, :identity_mismatch} = ActivityRegistry.publish_delivery_receipt(token, %{ctx.identity | replay_generation: 1}, receipt, :fallback, name: ctx.registry)
    caller = Task.async(fn -> ActivityRegistry.publish_delivery_receipt(token, ctx.identity, receipt, :fallback, name: ctx.registry) end)
    assert Task.await(caller) == {:error, :unknown_delivery}
    assert :ok = ActivityRegistry.abandon_delivery_receipt(token, name: ctx.registry)
    assert {_epoch, []} = ActivityRegistry.begin_drain(name: ctx.registry)
    assert %{closed?: true, producers_failed?: false} = ActivityRegistry.receipt_cohort(name: ctx.registry)
  end

  test "a receipt failure belongs to its current drain rather than a historical socket", ctx do
    {:ok, token} = ActivityRegistry.enroll_delivery_receipt(self(), ctx.identity, name: ctx.registry)
    receipt = DeliveryReceipt.build(%{outcome: "aborted"})
    :ok = ActivityRegistry.publish_delivery_receipt(token, ctx.identity, receipt, {:native, self(), ctx.identity, :aborted}, name: ctx.registry)
    assert :ok = ActivityRegistry.finish_delivery_receipt(token, {:error, :attempt_not_found}, name: ctx.registry)
    {_epoch, []} = ActivityRegistry.begin_drain(name: ctx.registry)
    assert %{closed?: true, producers_failed?: false} = ActivityRegistry.receipt_cohort(name: ctx.registry)
  end

  test "force closure aborts its assigned metadata actor without changing counters", ctx do
    parent = self()

    actor =
      spawn(fn ->
        {:ok, token} = ActivityRegistry.enroll_delivery_receipt(parent, ctx.identity, name: ctx.registry)
        send(parent, {:enrolled_for_cut, token})

        receive do
          :stop -> :ok
        end
      end)

    monitor = Process.monitor(actor)
    assert_receive {:enrolled_for_cut, token}
    receipt = DeliveryReceipt.build(%{outcome: "aborted"})
    {_epoch, []} = ActivityRegistry.begin_drain(name: ctx.registry)
    :ok = ActivityRegistry.publish_delivery_receipt(token, ctx.identity, receipt, {:native, actor, ctx.identity, :aborted}, name: ctx.registry)
    assert :ok = ActivityRegistry.force_receipt_cohort(name: ctx.registry)
    assert_receive {:DOWN, ^monitor, :process, ^actor, :killed}
    assert %{closed?: true, entries: [], pending_producers: 0, producers_failed?: true} = ActivityRegistry.receipt_cohort(name: ctx.registry)
    assert {:error, :stale_delivery} = ActivityRegistry.claim_delivery_receipt(token, name: ctx.registry)
    assert {:error, :owner_drained} = ActivityRegistry.enroll_delivery_receipt(self(), ctx.identity, name: ctx.registry)
    assert {:error, :unknown_delivery} = ActivityRegistry.publish_delivery_receipt(token, ctx.identity, receipt, :fallback, name: ctx.registry)
  end

  test "an assigned cleanup obligation survives socket death until its actor finishes", ctx do
    parent = self()

    socket =
      spawn(fn ->
        receive do
          :stop -> :ok
        end
      end)

    {:ok, token} = ActivityRegistry.enroll_delivery_receipt(socket, ctx.identity, name: ctx.registry)
    receipt = DeliveryReceipt.build(%{outcome: "aborted"})

    actor =
      Task.async(fn ->
        {:ok, envelope} = ActivityRegistry.take_delivery_cleanup(token, ctx.identity, receipt, name: ctx.registry)
        send(parent, {:cleanup_assigned, self(), envelope.token})

        receive do
          :complete ->
            assert [^envelope] = ActivityRegistry.assigned_delivery_receipts(name: ctx.registry)
            forged = put_in(envelope.receipt["outcome"], "delivered")
            assert {:error, :stale_delivery} = ActivityRegistry.claim_delivery_receipt(token, name: ctx.registry, envelope: forged)
            assert {:ok, remaining_ms} = ActivityRegistry.claim_delivery_receipt(token, name: ctx.registry, envelope: envelope)
            assert remaining_ms > 0
        end

        ActivityRegistry.finish_delivery_receipt(token, :ok, name: ctx.registry)
      end)

    assert_receive {:cleanup_assigned, actor_pid, ^token}
    assert actor_pid == actor.pid
    {_epoch, []} = ActivityRegistry.begin_drain(name: ctx.registry)
    monitor = Process.monitor(socket)
    send(socket, :stop)
    assert_receive {:DOWN, ^monitor, :process, ^socket, :normal}
    assert %{closed?: false, pending_producers: 1} = ActivityRegistry.receipt_cohort(name: ctx.registry)
    assert {:error, :stale_delivery} = ActivityRegistry.claim_delivery_receipt(token, name: ctx.registry)
    send(actor.pid, :complete)
    assert Task.await(actor) == :ok
    assert %{closed?: true, pending_producers: 0, producers_failed?: false} = ActivityRegistry.receipt_cohort(name: ctx.registry)
  end

  test "only an explicitly stopped executor shares its cleanup actor's settlement protection", ctx do
    parent = self()

    task =
      spawn(fn ->
        direct = %DirectCleanup{registry: ctx.registry, task: self(), parent: parent, ref: make_ref(), session_id: Ecto.UUID.generate()}
        {:ok, activity} = ActivityRegistry.register(:direct, self(), name: ctx.registry, direct_cleanup_ref: direct.ref, direct_cleanup_parent: parent)
        :ok = ActivityRegistry.admit(activity, name: ctx.registry)
        :ok = ActivityRegistry.begin_direct_cleanup(direct)
        :ok = ActivityRegistry.enter_direct_upstream_wait(direct)
        {:ok, token} = ActivityRegistry.enroll_delivery_receipt(parent, ctx.identity, name: ctx.registry)
        send(parent, {:stop_context, direct, token})

        receive do
          :stop -> :ok
        end
      end)

    on_exit(fn -> if Process.alive?(task), do: Process.exit(task, :kill) end)
    assert_receive {:stop_context, direct, token}
    receipt = DeliveryReceipt.build(%{outcome: "aborted"})

    cleanup =
      Task.async(fn ->
        {:ok, _envelope} = ActivityRegistry.take_delivery_cleanup(token, ctx.identity, receipt, name: ctx.registry)
        send(parent, :cleanup_has_receipt)

        receive do
          :complete -> :ok
        end

        ActivityRegistry.finish_delivery_receipt(token, :ok, name: ctx.registry)
      end)

    assert_receive :cleanup_has_receipt
    refute ActivityRegistry.delivery_settling?(task, name: ctx.registry)
    assert :stop = ActivityRegistry.stop_direct_upstream_wait(direct)
    assert ActivityRegistry.delivery_settling?(task, name: ctx.registry)
    send(cleanup.pid, :complete)
    assert Task.await(cleanup) == :ok
    refute ActivityRegistry.delivery_settling?(task, name: ctx.registry)
    send(task, :stop)
  end

  test "only a privately registered admitted task can bind delivery after draining starts", ctx do
    parent = self()
    ref = make_ref()

    actor =
      spawn(fn ->
        {:ok, activity} = ActivityRegistry.register(:direct, self(), name: ctx.registry, direct_cleanup_ref: ref, direct_cleanup_parent: parent)
        :ok = ActivityRegistry.admit(activity, name: ctx.registry)
        send(parent, :admitted_before_drain)

        receive do
          :bind -> :ok
        end

        assert {:error, :owner_drained} = ActivityRegistry.enroll_delivery_receipt(self(), ctx.identity, name: ctx.registry)
        result = ActivityRegistry.enroll_delivery_receipt(parent, ctx.identity, name: ctx.registry)
        send(parent, {:bound_during_drain, result})

        receive do
          :closed -> send(parent, {:binding_after_close, ActivityRegistry.enroll_delivery_receipt(parent, ctx.identity, name: ctx.registry)})
        end
      end)

    on_exit(fn -> if Process.alive?(actor), do: Process.exit(actor, :kill) end)
    assert_receive :admitted_before_drain
    ActivityRegistry.begin_drain(name: ctx.registry)
    assert {:error, :owner_drained} = ActivityRegistry.enroll_delivery_receipt(parent, ctx.identity, name: ctx.registry)
    send(actor, :bind)
    assert_receive {:bound_during_drain, {:ok, _token}}
    assert %{closed?: false, pending_producers: 1} = ActivityRegistry.receipt_cohort(name: ctx.registry)
    :ok = ActivityRegistry.force_receipt_cohort(name: ctx.registry)
    assert %{closed?: true, producers_failed?: true} = ActivityRegistry.receipt_cohort(name: ctx.registry)
    send(actor, :closed)
    assert_receive {:binding_after_close, {:error, :owner_drained}}
  end

  test "a reused actor retires its completed delivery capability before exposing a successor", ctx do
    {:ok, token} = ActivityRegistry.enroll_delivery_receipt(self(), ctx.identity, name: ctx.registry)
    receipt = DeliveryReceipt.build(%{outcome: "aborted"})
    :ok = ActivityRegistry.publish_delivery_receipt(token, ctx.identity, receipt, {:native, self(), ctx.identity, :aborted}, name: ctx.registry)
    :ok = ActivityRegistry.finish_delivery_receipt(token, :ok, name: ctx.registry)
    next_identity = %{ctx.identity | request_id: Ecto.UUID.generate(), attempt_id: Ecto.UUID.generate()}
    assert {:ok, next_token} = ActivityRegistry.enroll_delivery_receipt(self(), next_identity, name: ctx.registry)
    assert next_token != token
    assert map_size(:sys.get_state(ctx.registry).delivery_receipts) == 1
    assert {:error, :unknown_delivery} = ActivityRegistry.publish_delivery_receipt(token, ctx.identity, receipt, :fallback, name: ctx.registry)
    assert {:ok, ^next_token} = ActivityRegistry.delivery_receipt_token(next_identity, name: ctx.registry)
    assert :ok = ActivityRegistry.abandon_delivery_receipt(next_token, name: ctx.registry)
  end

  test "failed receipt completion retains only a fixed bounded disposition", ctx do
    {:ok, token} = ActivityRegistry.enroll_delivery_receipt(self(), ctx.identity, name: ctx.registry)
    receipt = DeliveryReceipt.build(%{outcome: "aborted"})
    :ok = ActivityRegistry.publish_delivery_receipt(token, ctx.identity, receipt, {:native, self(), ctx.identity, :aborted}, name: ctx.registry)
    :ok = ActivityRegistry.finish_delivery_receipt(token, {:error, RuntimeError.exception("synthetic database failure")}, name: ctx.registry)
    assert :sys.get_state(ctx.registry).delivery_receipts[token].result == {:error, :receipt_not_persisted}
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
