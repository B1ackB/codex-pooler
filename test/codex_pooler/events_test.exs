defmodule CodexPooler.EventsTest do
  use CodexPooler.DataCase, async: false

  alias CodexPooler.Accounting
  alias CodexPooler.Accounting.RequestLifecycle.Reservation
  alias CodexPooler.Events
  alias CodexPooler.Events.Event
  alias CodexPooler.Events.PostgresBridge
  alias CodexPooler.Gateway.Persistence.SessionContinuity
  alias CodexPooler.Gateway.Websocket
  alias CodexPooler.Pools.Pool
  alias Ecto.Adapters.SQL.Sandbox

  import CodexPooler.UnboxedFixture
  import CodexPooler.PoolerFixtures

  @commit_detection_timeout_ms 15_000

  test "nested request log publication waits for the outer commit and is visible on another connection" do
    pool_id = Ecto.UUID.generate()
    assert :ok = Events.subscribe_pool(pool_id, "request_logs")
    parent = self()
    release = make_ref()

    writer =
      Task.async(fn ->
        Sandbox.unboxed_run(Repo, fn ->
          Repo.transaction(fn ->
            {:ok, event} =
              Repo.transaction(fn ->
                {:ok, event} = Events.broadcast_request_logs(pool_id, "request_reserved", %{status: "accepted"})
                event
              end)

            send(parent, {:nested_finished, event})
            await_release!(release, event)
          end)
        end)
      end)

    assert_receive {:nested_finished, event}, @commit_detection_timeout_ms
    # The outer transaction outlives the request-log view's 250 ms debounce.
    refute_receive {Events, ^event}, 300
    send(writer.pid, release)
    assert {:ok, ^event} = Task.await(writer, @commit_detection_timeout_ms)
    assert_receive {Events, ^event}, @commit_detection_timeout_ms
    assert_transaction_marker!(pool_id)
    refute_received {Events, ^event}
  end

  test "a held nested reservation publishes the committed in-progress row on a separate connection" do
    pool_slug = "committed-event-#{System.unique_integer([:positive])}"

    register_unboxed_cleanup!(fn ->
      pool_ids = Repo.all(from pool in Pool, where: pool.slug == ^pool_slug, select: pool.id)
      delete_committed_pools!(pool_ids)
    end)

    setup =
      run_unboxed(fn ->
        pool = pool_fixture(%{slug: pool_slug})
        key = active_api_key_fixture(pool)
        model = model_fixture(pool)
        {:ok, session} = Websocket.start_codex_session(key, %{accepted_turn_state: Ecto.UUID.generate()})
        opts = %{codex_session: session, endpoint: "/backend-api/codex/responses", requested_model: model.exposed_model_id, correlation_id: Ecto.UUID.generate(), transport: "websocket"}
        {:ok, %{request: claim}} = Accounting.claim_websocket_turn(key, model, opts)
        key |> Map.put(:model, model) |> Map.put(:claim, claim) |> Map.put(:opts, Map.put(opts, :turn_claim, claim))
      end)

    assert :ok = Events.subscribe_pool(setup.pool, "request_logs")
    reader = start_supervised!({Postgrex, Keyword.take(Repo.config(), [:hostname, :port, :database, :username, :password, :ssl])})
    [[reader_backend]] = Postgrex.query!(reader, "SELECT pg_backend_pid()", []).rows
    parent = self()
    release = make_ref()
    writer = Task.async(fn -> held_reservation(setup, parent, release) end)
    assert_receive {:reservation_held, request, writer_backend}, @commit_detection_timeout_ms
    assert writer_backend != reader_backend
    request_id = request.id
    assert Postgrex.query!(reader, "SELECT status FROM requests WHERE id = $1", [Ecto.UUID.dump!(request_id)]).rows == [["accepted"]]
    refute_receive {Events, %{payload: %{"request_id" => ^request_id}}}, 300
    send(writer.pid, release)
    assert {:ok, %{status: "in_progress"}} = Task.await(writer, @commit_detection_timeout_ms)
    assert_receive {Events, %{reason: "request_reserved", payload: %{"request_id" => ^request_id}} = event}, @commit_detection_timeout_ms
    assert Postgrex.query!(reader, "SELECT status FROM requests WHERE id = $1", [Ecto.UUID.dump!(request_id)]).rows == [["in_progress"]]
    assert_transaction_marker!(setup.pool.id)
    refute_received {Events, ^event}
  end

  for rollback <- [:outer, :nested] do
    test "#{rollback} rollback publishes no request log invalidation" do
      pool_id = Ecto.UUID.generate()
      assert :ok = Events.subscribe_pool(pool_id, "request_logs")
      rollback = unquote(rollback)
      parent = self()

      result =
        run_unboxed(fn ->
          Repo.transaction(fn ->
            {:ok, event} = Events.broadcast_request_logs(pool_id, "request_reserved")
            send(parent, {:rolled_back_event, event})
            rollback_event(rollback)
          end)
        end)

      assert match?({:error, _}, result)
      assert_receive {:rolled_back_event, event}
      assert_transaction_marker!(pool_id)
      refute_received {Events, ^event}
    end
  end

  defp held_reservation(setup, parent, release) do
    Sandbox.unboxed_run(Repo, fn ->
      SessionContinuity.mailbox_admission_transaction(
        fn -> Reservation.mailbox_admission_session_ids(setup, setup.model, setup.opts) end,
        fn ->
          [[writer_backend]] = Repo.query!("SELECT pg_backend_pid()").rows
          {:ok, %{request: request}} = Accounting.reserve(setup, setup.model, %{}, setup.opts)
          assert request.status == "in_progress"
          send(parent, {:reservation_held, request, writer_backend})
          await_release!(release, request)
        end,
        Reservation.mailbox_admission_exhausted_error()
      )
    end)
  end

  defp await_release!(release, result) do
    receive do
      ^release -> result
    after
      15_000 -> Repo.rollback(:release_timeout)
    end
  end

  defp rollback_event(:outer), do: Repo.rollback(:cancelled)
  defp rollback_event(:nested), do: Repo.transaction(fn -> Repo.rollback(:cancelled) end)

  defp assert_transaction_marker!(pool_id) do
    {:ok, {:ok, marker}} =
      run_unboxed(fn ->
        Repo.transaction(fn -> Events.broadcast_pool_event_after_commit(pool_id, ["request_logs"], "commit_barrier") end)
      end)

    assert_receive {Events, ^marker}, @commit_detection_timeout_ms
  end

  test "a PostgreSQL notification failure is returned rather than acknowledged" do
    pool = pool_fixture()

    {result, log} =
      ExUnit.CaptureLog.with_log(fn ->
        Repo.transaction(fn ->
          result = Events.broadcast_request_logs(pool.id, "request_reserved", %{sample: String.duplicate("x", 8_000)})
          Repo.rollback(result)
        end)
      end)

    assert match?({:error, {:error, :postgres_event_relay_failed}}, result)
    assert log =~ "pool event postgres relay failed"
    refute log =~ String.duplicate("x", 100)
  end

  test "broadcasts exact pool event shapes for LiveView topics" do
    pool = pool_fixture()
    assert :ok = Events.subscribe_pool(pool.id)

    assert {:ok, request_logs} =
             publish_from_task(fn ->
               Events.broadcast_request_logs(pool.id, "request_log_created", %{
                 request_id: "request-test"
               })
             end)

    assert_receive {Events, ^request_logs}
    assert_event_shape(request_logs, pool.id, ["request_logs"], "request_log_created")
    assert request_logs.payload == %{"request_id" => "request-test"}

    assert {:ok, usage} =
             publish_from_task(fn ->
               Events.broadcast_usage(pool.id, "usage_updated", %{request_count: 1})
             end)

    assert_receive {Events, ^usage}
    assert_event_shape(usage, pool.id, ["usage"], "usage_updated")
    assert usage.payload == %{"request_count" => 1}

    assert {:ok, job_status} =
             publish_from_task(fn ->
               Events.broadcast_job_status(pool.id, "job_status_updated", %{
                 id: "job-test",
                 status: "complete"
               })
             end)

    assert_receive {Events, ^job_status}
    assert_event_shape(job_status, pool.id, ["job_status"], "job_status_updated")
    assert job_status.payload == %{"id" => "job-test", "status" => "complete"}

    assert {:ok, model_sync} =
             publish_from_task(fn ->
               Events.broadcast_model_sync(pool.id, "model_sync_completed", %{status: "succeeded"})
             end)

    assert_receive {Events, ^model_sync}
    assert_event_shape(model_sync, pool.id, ["model_sync"], "model_sync_completed")
    assert model_sync.payload == %{"status" => "succeeded"}

    assert {:ok, pools} =
             publish_from_task(fn ->
               Events.broadcast_pools(pool.id, "pool_routing_settings_updated", %{
                 routing_strategy: "bridge_ring"
               })
             end)

    assert_receive {Events, ^pools}
    assert_event_shape(pools, pool.id, ["pools"], "pool_routing_settings_updated")
    assert pools.payload == %{"routing_strategy" => "bridge_ring"}

    assert {:ok, upstreams} =
             publish_from_task(fn ->
               Events.broadcast_upstreams(pool.id, "upstream_account_imported", %{
                 upstream_identity_id: "upstream-test"
               })
             end)

    assert_receive {Events, ^upstreams}
    assert_event_shape(upstreams, pool.id, ["upstreams"], "upstream_account_imported")
    assert upstreams.payload == %{"upstream_identity_id" => "upstream-test"}
  end

  test "does not deliver local pool events back to the broadcasting subscriber" do
    pool = pool_fixture()
    assert :ok = Events.subscribe_pool(pool.id)

    assert {:ok, event} = Events.broadcast_pools(pool.id, "pool_routing_settings_updated", %{})

    refute_receive {Events, ^event}
  end

  test "delivers only selected topics to a scoped pool subscriber" do
    pool = pool_fixture()
    assert :ok = Events.subscribe_pool(pool.id, "pools")

    assert {:ok, usage} =
             publish_from_task(fn -> Events.broadcast_usage(pool.id, "usage_updated", %{}) end)

    assert {:ok, request_logs} =
             publish_from_task(fn ->
               Events.broadcast_request_logs(pool.id, "request_log_created", %{})
             end)

    refute_receive {Events, ^usage}
    refute_receive {Events, ^request_logs}

    assert {:ok, pools} =
             publish_from_task(fn ->
               Events.broadcast_pools(pool.id, "pool_routing_settings_updated", %{})
             end)

    assert_receive {Events, ^pools}
  end

  test "delivers each matching event once to a multi-topic pool subscriber" do
    pool = pool_fixture()
    assert :ok = Events.subscribe_pool(pool.id, ["pools", "upstreams"])

    assert {:ok, pools} =
             publish_from_task(fn ->
               Events.broadcast_pools(pool.id, "pool_routing_settings_updated", %{})
             end)

    assert_receive {Events, ^pools}
    refute_receive {Events, ^pools}

    assert {:ok, upstreams} =
             publish_from_task(fn ->
               Events.broadcast_upstreams(pool.id, "upstream_account_imported", %{})
             end)

    assert_receive {Events, ^upstreams}
    refute_receive {Events, ^upstreams}
  end

  test "rejects invalid subscription topics without subscribing" do
    pool = pool_fixture()

    assert Events.pubsub_topic(pool.id, "pools") ==
             Events.pubsub_topic(pool.id) <> ":pools"

    assert {:error, :invalid_topics} = Events.pubsub_topic(pool.id, "unknown")
    assert {:error, :invalid_topics} = Events.subscribe_pool(pool.id, ["unknown"])

    assert {:ok, event} =
             publish_from_task(fn ->
               Events.broadcast_pools(pool.id, "pool_routing_settings_updated", %{})
             end)

    refute_receive {Events, ^event}
  end

  test "rejects malformed scoped subscription topics without raising" do
    pool = pool_fixture()

    assert {:error, :invalid_topics} = Events.subscribe_pool(pool.id, [%{}])
    assert {:error, :invalid_topics} = Events.unsubscribe_pool(pool.id, [123])
    assert {:error, :invalid_topics} = Events.pubsub_topic(pool.id, %{})
  end

  test "does not deliver scoped local events back to the broadcasting subscriber" do
    pool = pool_fixture()
    assert :ok = Events.subscribe_pool(pool.id, "pools")

    assert {:ok, event} = Events.broadcast_pools(pool.id, "pool_routing_settings_updated", %{})

    refute_receive {Events, ^event}
  end

  test "rejects invalid event topics" do
    pool = pool_fixture()

    assert {:error, :invalid_topics} =
             Events.broadcast_pool_event(pool.id, ["unknown"], "invalid", %{})
  end

  test "relays postgres notification payloads back into local pool PubSub" do
    pool = pool_fixture()
    assert :ok = Events.subscribe_pool(pool.id)

    event = %Event{
      version: 1,
      id: Ecto.UUID.generate(),
      pool_id: pool.id,
      topics: ["upstreams"],
      reason: "upstream_quota_windows_updated",
      emitted_at: DateTime.utc_now() |> DateTime.truncate(:microsecond),
      payload: %{"upstream_identity_id" => Ecto.UUID.generate()}
    }

    assert {:ok, payload} = Events.event_to_postgres_payload(event)
    assert :ok = publish_from_task(fn -> PostgresBridge.relay_payload(payload) end)

    assert_receive {Events, ^event}
  end

  test "relays postgres notification payloads to scoped pool PubSub" do
    pool = pool_fixture()
    assert :ok = Events.subscribe_pool(pool.id, "upstreams")

    event = event_fixture(pool.id, ["upstreams"])

    assert {:ok, payload} = Events.event_to_postgres_payload(event)
    assert :ok = publish_from_task(fn -> PostgresBridge.relay_payload(payload) end)

    assert_receive {Events, ^event}
  end

  test "rejects relayed invalid topics without delivering an event" do
    pool = pool_fixture()
    assert :ok = Events.subscribe_pool(pool.id)

    event = event_fixture(pool.id, ["unknown"])

    assert {:ok, payload} = Events.event_to_postgres_payload(event)
    assert {:error, :invalid_topics} = PostgresBridge.relay_payload(payload)
    refute_receive {Events, ^event}
  end

  test "rejects relayed malformed topic values without raising" do
    pool = pool_fixture()
    assert :ok = Events.subscribe_pool(pool.id)

    event = event_fixture(pool.id, [123])

    assert {:ok, payload} = Events.event_to_postgres_payload(event)
    assert {:error, :invalid_topics} = PostgresBridge.relay_payload(payload)
    refute_receive {Events, ^event}
  end

  defp publish_from_task(fun) when is_function(fun, 0) do
    fun
    |> Task.async()
    |> Task.await(5_000)
  end

  defp assert_event_shape(event, pool_id, topics, reason) do
    assert %Events.Event{
             version: 1,
             pool_id: ^pool_id,
             topics: ^topics,
             reason: ^reason,
             payload: payload
           } = event

    assert is_binary(event.id)
    assert {:ok, _uuid} = Ecto.UUID.cast(event.id)
    assert %DateTime{} = event.emitted_at
    assert is_map(payload)
  end

  defp event_fixture(pool_id, topics) do
    %Event{
      version: 1,
      id: Ecto.UUID.generate(),
      pool_id: pool_id,
      topics: topics,
      reason: "upstream_quota_windows_updated",
      emitted_at: DateTime.utc_now() |> DateTime.truncate(:microsecond),
      payload: %{"status" => "updated"}
    }
  end
end
