defmodule CodexPooler.Accounting.AttemptFinalizationDeliveryReceiptTest do
  # A websocket socket merges its delivery receipt into the attempt row with its
  # own statement, normally after the gateway finalized the attempt. A socket
  # that closes while its turn is still settling records it first, and the
  # finalization, holding the attempt it loaded before, replaced the whole
  # metadata map: the receipt was gone and the resend admission that reads it
  # refused the released client's identical resend (findings#232, one
  # forwarding-on released-client run of five). Both orders must end with the
  # receipt on the row.
  use CodexPooler.DataCase, async: false

  import CodexPooler.AccountingTestSupport

  alias CodexPooler.Accounting
  alias CodexPooler.Accounting.{Attempt, LedgerEntry}
  alias CodexPooler.Gateway.Payloads.RequestOptions
  alias CodexPooler.Gateway.Persistence.{BridgeOwnerLease, CodexSession, CodexTurn}
  alias CodexPooler.Gateway.Persistence.SessionContinuity
  alias CodexPooler.Gateway.Runtime.Finalization.AttemptSettlement
  alias CodexPooler.Gateway.Runtime.Finalization.ExpiredOwnerGenerationCleanup, as: Cleanup
  alias CodexPooler.Gateway.Websocket
  alias CodexPooler.Gateway.Websocket.DeliveryReceipt
  alias CodexPooler.Platform.ExecutionIdentity
  alias CodexPooler.Platform.InstancePresence.Identity
  alias CodexPooler.UnboxedFixture
  alias Ecto.Adapters.SQL.Sandbox

  import CodexPoolerWeb.Runtime.BackendCodexTestSupport, only: [cleanup_unboxed_pool!: 1]

  setup context do
    if context[:unboxed_writers] do
      CodexPooler.DataCase.stop_sandbox(context.sandbox_owner, context.sandbox_settings_cache)
      on_exit(fn -> Sandbox.mode(Repo, :manual) end)
      :ok = Sandbox.mode(Repo, :auto)
    end

    :ok
  end

  test "a receipt recorded before the attempt's finalization survives it" do
    %{attempt: attempt, request: request} = websocket_attempt!()
    receipt = aborted_partial_receipt()

    assert :ok = DeliveryReceipt.persist(attempt.id, receipt)

    # `attempt` is the struct loaded before the receipt was merged, as the
    # settling task holds it.
    assert {:ok, _finalized} = Accounting.finalize_request(request, attempt, interrupted_attrs())

    assert %Attempt{status: "failed", response_metadata: metadata} = Repo.get!(Attempt, attempt.id)
    assert metadata["downstream_delivery"] == receipt
    assert metadata["attempt_marker"] == "finalization"
  end

  test "a receipt recorded after the attempt's finalization is merged beside its metadata" do
    %{attempt: attempt, request: request} = websocket_attempt!()
    receipt = aborted_partial_receipt()

    assert {:ok, _finalized} = Accounting.finalize_request(request, attempt, interrupted_attrs())
    assert :ok = DeliveryReceipt.persist(attempt.id, receipt)

    assert %Attempt{response_metadata: metadata} = Repo.get!(Attempt, attempt.id)
    assert metadata["downstream_delivery"] == receipt
    assert metadata["attempt_marker"] == "finalization"
  end

  test "a socket receipt recorded before retry finalization survives the stale caller struct" do
    %{attempt: attempt} = websocket_attempt!()
    receipt = aborted_partial_receipt()
    assert :ok = DeliveryReceipt.record(%{attempt_id: attempt.id, request_id: attempt.request_id}, receipt)
    assert {:ok, saved} = Accounting.record_retryable_attempt_failure(attempt, retry_attrs())
    assert saved.response_metadata["downstream_delivery"] == receipt
    assert saved.response_metadata["attempt_marker"] == "retry"
  end

  test "retry finalization's explicit delivery receipt retains existing precedence" do
    %{attempt: attempt} = websocket_attempt!()
    assert :ok = DeliveryReceipt.persist(attempt.id, aborted_partial_receipt())
    reported = DeliveryReceipt.build(%{outcome: "aborted", frames_after_visible: 8, write_failure: "closed"})
    attrs = Map.put(retry_attrs(), :attempt_metadata, %{"downstream_delivery" => reported, "attempt_marker" => "retry"})
    assert {:ok, saved} = Accounting.record_retryable_attempt_failure(attempt, attrs)
    assert saved.response_metadata["downstream_delivery"] == reported
  end

  for {first, second} <- [{:receipt, :finalizer}, {:finalizer, :receipt}, {:receipt, :retry}, {:retry, :receipt}, {:witness, :retry}, {:retry, :witness}] do
    @tag :unboxed_writers
    @tag first: first, second: second
    test "independent PostgreSQL #{first} then #{second} preserves both writers' metadata", %{first: first, second: second} do
      holder = String.to_atom("receipt-order-fixture-#{System.unique_integer([:positive])}")
      # The unlinked holder survives an assertion failure long enough for the
      # pre-registered exact committed-graph cleanup; it is stopped afterward.
      on_exit(fn ->
        if pid = Process.whereis(holder) do
          try do
            if fixture = Agent.get(pid, & &1) do
              UnboxedFixture.cleanup_unboxed!(fn -> cleanup_unboxed_pool!(fixture.setup) end)
            end
          after
            Agent.stop(pid)
          end
        end
      end)

      {:ok, _holder} = Agent.start(fn -> nil end, name: holder)

      fixture =
        UnboxedFixture.run_unboxed(fn ->
          {:ok, fixture} =
            Repo.transaction(fn ->
              fixture = websocket_attempt!()
              Agent.update(holder, fn _ -> fixture end)
              fixture
            end)

          if :witness in [first, second], do: authorize_witness!(fixture), else: fixture
        end)

      supervisor = start_supervised!(Task.Supervisor)
      parent = self()
      release = make_ref()
      handler = {__MODULE__, release}
      on_exit(fn -> :telemetry.detach(handler) end)
      :ok = :telemetry.attach(handler, [:codex_pooler, :repo, :query], &__MODULE__.hold_attempt_write/4, nil)
      receipt = aborted_partial_receipt()
      first_task = start_ordered_writer(supervisor, fixture, receipt, first, parent, release, true)
      first_monitor = Process.monitor(first_task.pid)
      assert_receive {:ordered_writer_ready, ^first, first_backend, first_pid}, 15_000
      second_task = start_ordered_writer(supervisor, fixture, receipt, second, parent, release, false)
      second_monitor = Process.monitor(second_task.pid)
      assert_receive {:ordered_writer_blocked, ^second, second_backend, second_pid}, 15_000
      assert first_backend != second_backend
      send(first_pid, {:release_ordered_writer, release})
      assert {:ok, _result} = Task.await(first_task, 15_000)
      send(second_pid, {:release_ordered_writer, release})
      assert {:ok, _result} = Task.await(second_task, 15_000)
      assert_receive {:DOWN, ^first_monitor, :process, _pid, :normal}, 15_000
      assert_receive {:DOWN, ^second_monitor, :process, _pid, :normal}, 15_000
      stored = UnboxedFixture.run_unboxed(fn -> Repo.get!(Attempt, fixture.attempt.id) end)

      if :witness in [first, second] do
        assert {:ok, %{"phase" => "ended", "end_kind" => "serialized_connection_closed"} = ended} = Cleanup.read(stored)
        assert Cleanup.scope(ended) == Cleanup.scope(fixture.witness)
        assert stored.response_metadata["downstream_delivery"] == receipt
      else
        assert stored.response_metadata["downstream_delivery"] == receipt
      end

      assert stored.status == if(:retry in [first, second], do: "retryable_failed", else: "failed")
      assert stored.response_metadata["attempt_marker"] == if(:retry in [first, second], do: "retry", else: "finalization")
      CodexPooler.TestDiagnostics.puts("attempt metadata writer_order=#{first}->#{second} backend_ids=#{first_backend},#{second_backend} attempt_nowait=55P03 actual_writer_lock_timeout=55P03 metadata_preserved=true")
    end
  end

  for {code, expected_turn} <- [{"owner_crashed", "interrupted"}, {"owner_unavailable", "interrupted"}, {"owner_drained", "interrupted"}, {"client_disconnected", "interrupted"}, {"upstream_stream_error", "failed"}], order <- [:receipt_first, :accounting_first] do
    @tag :unboxed_writers
    @tag code: code, expected_turn: expected_turn, receipt_order: order
    test "authoritative #{code} delivery #{order} preserves measured charge and canonical turn outcome", context do
      assert_authoritative_delivery(context)
    end
  end

  test "legacy aborted receipt alone does not infer cancellation" do
    %{attempt: attempt, request: request} = websocket_attempt!()
    assert :ok = DeliveryReceipt.persist(attempt.id, aborted_partial_receipt())
    assert {:ok, settled} = Accounting.finalize_success(request, attempt, %{status: "usage_known", input_tokens: 0, output_tokens: 0, total_tokens: 0})
    assert settled.request.status == "succeeded"
    assert settled.settlement.usage_status == "usage_known"
  end

  test "reconciliation rejects mismatched request and generation without metadata writes" do
    %{attempt: attempt, request: request} = websocket_attempt!()
    other = websocket_attempt!()
    receipt = aborted_partial_receipt()
    context = %{request_id: request.id, replay_generation: 9, delivery_failure: %{status: "failed", response_status_code: 499, error_code: "client_disconnected"}}
    before_attempt = Repo.reload!(attempt)
    assert :ok = DeliveryReceipt.persist(attempt.id, receipt, context)
    assert Repo.reload!(attempt) == before_attempt
    assert :ok = DeliveryReceipt.persist(attempt.id, receipt, %{context | request_id: other.request.id, replay_generation: 0})
    assert Repo.reload!(attempt) == before_attempt
    assert {:ok, %{stale_generation?: true}} = Accounting.reconcile_request_with_disposition(other.request, attempt, %{usage: %{status: "usage_known", input_tokens: 1, output_tokens: 0, total_tokens: 1}})
    assert Repo.reload!(attempt) == before_attempt
  end

  test "failed before-commit correction rolls back charge outcome witness and projections" do
    %{attempt: attempt, request: request} = websocket_attempt!()
    assert {:ok, settled} = Accounting.finalize_success(request, attempt, %{status: "usage_known", input_tokens: 10, output_tokens: 2, total_tokens: 12})
    before_request = Repo.reload!(request)
    before_attempt = Repo.reload!(attempt)
    assert {:error, :rollback_correction} = Accounting.reconcile_request_with_disposition(request, attempt, %{delivery_failure: %{status: "failed", response_status_code: 499, error_code: "client_disconnected"}, before_commit: fn _ -> {:error, :rollback_correction} end})
    assert Repo.reload!(request) == before_request
    assert Repo.reload!(attempt) == before_attempt
    assert Repo.reload!(settled.settlement) == settled.settlement
  end

  test "unbounded or unknown failure witness cannot change a delivered disposition" do
    %{attempt: attempt, request: request} = websocket_attempt!()
    assert {:ok, settled} = Accounting.finalize_success(request, attempt, %{status: "usage_known", input_tokens: 0, output_tokens: 0, total_tokens: 0})
    assert {:ok, corrected} = Accounting.reconcile_request_with_disposition(request, attempt, %{delivery_failure: %{status: "failed", response_status_code: 499, error_code: String.duplicate("invalid", 100)}})
    assert corrected.request.status == "succeeded"
    assert Repo.reload!(settled.settlement) == settled.settlement
    refute Map.has_key?(Repo.reload!(attempt).response_metadata, "delivery_failure")
  end

  defp assert_authoritative_delivery(%{code: code, expected_turn: expected_turn, receipt_order: order}) do
    fixture = committed_turn_fixture!()
    %{attempt: attempt, request: request, turn: turn} = fixture
    observer = start_supervised!({Postgrex, Keyword.take(Repo.config(), [:hostname, :port, :username, :password, :database])})
    context = %{request_id: request.id, attempt_id: attempt.id, replay_generation: attempt.replay_generation, delivery_failure: %{status: "failed", response_status_code: 499, error_code: code}}
    receipt = aborted_partial_receipt()
    usage = %{status: "usage_known", input_tokens: 10, output_tokens: 2, total_tokens: 12}

    {settled, before_state} =
      UnboxedFixture.run_unboxed(fn ->
        if order == :receipt_first, do: assert(:ok = DeliveryReceipt.persist(attempt.id, receipt, context))
        assert {:ok, settled} = AttemptSettlement.finalize_success(request, attempt, usage, %{response_status_code: 200})

        if order == :accounting_first do
          assert Repo.reload!(turn).status == "succeeded"
          assert {:ok, ordinary} = AttemptSettlement.finalize_partial_stream_failure(request, attempt, %{status: "usage_unknown"}, %{last_error_code: code, response_status_code: 499})
          assert ordinary.request.status == "succeeded"
          assert ordinary.settlement == settled.settlement
          assert :ok = DeliveryReceipt.persist(attempt.id, receipt)
          assert Repo.reload!(request).status == "succeeded"
          assert Repo.reload!(turn).status == "succeeded"
        end

        {settled, {Repo.reload!(request).status, Repo.reload!(turn).status}}
      end)

    UnboxedFixture.run_unboxed(fn ->
      assert :ok = DeliveryReceipt.persist(attempt.id, receipt, context)
      first_turn = Repo.reload!(turn)
      ledger = Repo.all(from entry in LedgerEntry, where: entry.request_id == ^request.id, order_by: entry.id)
      assert :ok = DeliveryReceipt.persist(attempt.id, receipt, context)
      assert Repo.reload!(turn) == first_turn
      assert Repo.all(from entry in LedgerEntry, where: entry.request_id == ^request.id, order_by: entry.id) == ledger
      assert Repo.reload!(settled.settlement) == settled.settlement
      assert settled.settlement.total_tokens == 12
      assert Repo.reload!(request).status == "failed"
      assert Repo.reload!(request).last_error_code == code
      assert Repo.reload!(attempt).status == "failed"
      assert Repo.reload!(attempt).network_error_code == code
      assert Repo.reload!(attempt).usage_status == "usage_known"
      assert Repo.reload!(attempt).response_metadata["downstream_delivery"] == receipt
      assert first_turn.final_attempt_id == attempt.id
      assert first_turn.error_code == code
    end)

    [[database, backend, request_status, attempt_status, turn_status, tokens, settlements, attempts]] =
      Postgrex.query!(observer, "SELECT current_database(), pg_backend_pid(), r.status, a.status, t.status, e.total_tokens, (SELECT count(*) FROM ledger_entries l WHERE l.request_id = r.id AND l.entry_kind = 'settlement' AND l.amount_status = 'recorded'), (SELECT count(*) FROM attempts x WHERE x.request_id = r.id) FROM requests r JOIN attempts a ON a.id = $2 AND a.request_id = r.id JOIN codex_turns t ON t.id = $3 AND t.request_id = r.id JOIN ledger_entries e ON e.id = $4 WHERE r.id = $1", Enum.map([request.id, attempt.id, turn.id, settled.settlement.id], &Ecto.UUID.dump!/1)).rows

    assert database == Repo.config()[:database]
    CodexPooler.TestDiagnostics.puts("authoritative delivery code=#{code} order=#{order} database=#{database} observer_backend=#{backend} before=#{inspect(before_state)} request=#{request_status} attempt=#{attempt_status} turn=#{turn_status} expected_turn=#{expected_turn} tokens=#{tokens} recorded_settlements=#{settlements} attempts=#{attempts} settlement_id=#{settled.settlement.id} ledger_unchanged=true repeat_unchanged=true")
    assert {request_status, attempt_status, turn_status, tokens, settlements, attempts} == {"failed", "failed", expected_turn, 12, 1, 1}
  end

  defp committed_turn_fixture! do
    holder = String.to_atom("receipt-turn-fixture-#{System.unique_integer([:positive])}")

    on_exit(fn ->
      if pid = Process.whereis(holder) do
        try do
          if fixture = Agent.get(pid, & &1), do: UnboxedFixture.cleanup_unboxed!(fn -> cleanup_unboxed_pool!(fixture.setup) end)
        after
          Agent.stop(pid)
        end
      end
    end)

    {:ok, _holder} = Agent.start(fn -> nil end, name: holder)

    UnboxedFixture.run_unboxed(fn ->
      {:ok, fixture} = Repo.transaction(fn -> insert_turn_fixture!(holder) end)
      fixture
    end)
  end

  defp insert_turn_fixture!(holder) do
    fixture = websocket_attempt!()
    Agent.update(holder, fn _ -> fixture end)
    assert {:ok, session} = Websocket.start_codex_session(fixture.setup.auth, %{accepted_turn_state: Ecto.UUID.generate()})
    opts = RequestOptions.for_websocket(%{}) |> RequestOptions.put_continuity(semantic_turn_key: :crypto.strong_rand_bytes(32))
    assert {:ok, turn} = SessionContinuity.start_codex_turn(session, fixture.request, opts)
    Map.merge(fixture, %{session: session, turn: turn})
  end

  defp start_ordered_writer(supervisor, fixture, receipt, kind, parent, release, hold?) do
    context = %{fixture: fixture, receipt: receipt, kind: kind, parent: parent, release: release, hold?: hold?}

    Task.Supervisor.async_nolink(supervisor, fn ->
      Sandbox.unboxed_run(Repo, fn -> run_ordered_statement(context) end)
    end)
  end

  defp run_ordered_statement(context) do
    [[backend]] = Repo.query!("SELECT pg_backend_pid()").rows

    if context.hold? do
      Process.put({__MODULE__, :hold_write}, {context, backend})
    else
      assert_attempt_locked!(context.fixture.attempt.id)
      Repo.query!("SET lock_timeout = '250ms'")

      try do
        assert {:error, %Postgrex.Error{postgres: %{code: :lock_not_available}}} = write_result(context)
      after
        Repo.query!("SET lock_timeout = 0")
      end

      send(context.parent, {:ordered_writer_blocked, context.kind, backend, self()})
      await_ordered_release(context.release)
    end

    write_result(context)
  end

  defp write_result(context) do
    perform_write(context)
  rescue
    error in Postgrex.Error -> {:error, error}
  end

  defp perform_write(%{kind: :witness, fixture: fixture}), do: Cleanup.record_end(fixture.witness, "serialized_connection_closed")

  defp perform_write(context) do
    Repo.transaction(fn ->
      case context.kind do
        :receipt ->
          persist_receipt!(context)

        :finalizer ->
          assert {:ok, _finalized} = Accounting.finalize_request(context.fixture.request, context.fixture.attempt, interrupted_attrs())

        :retry ->
          assert {:ok, _attempt} = Accounting.record_retryable_attempt_failure(context.fixture.attempt, retry_attrs())
      end
    end)
  end

  defp persist_receipt!(context) do
    case DeliveryReceipt.persist(context.fixture.attempt.id, context.receipt) do
      :ok -> :ok
      {:error, error} -> Repo.rollback(error)
    end
  end

  def hold_attempt_write(_event, _measurements, %{source: "attempts", query: "UPDATE" <> _}, _config) do
    case Process.delete({__MODULE__, :hold_write}) do
      {context, backend} ->
        send(context.parent, {:ordered_writer_ready, context.kind, backend, self()})
        await_ordered_release(context.release)

      nil ->
        :ok
    end
  end

  def hold_attempt_write(_event, _measurements, _metadata, _config), do: :ok

  defp assert_attempt_locked!(attempt_id) do
    assert_raise Postgrex.Error, ~r/55P03/, fn ->
      Repo.transaction(fn -> Repo.one!(from a in Attempt, where: a.id == ^attempt_id, lock: "FOR UPDATE NOWAIT") end)
    end
  end

  defp await_ordered_release(release) do
    receive do
      {:release_ordered_writer, ^release} -> :ok
    after
      15_000 -> Repo.rollback(:ordered_writer_release_missing)
    end
  end

  defp websocket_attempt! do
    setup = accounting_setup()

    assert {:ok, reserved} =
             Accounting.reserve(setup.auth, setup.model, %{"model" => setup.model.exposed_model_id, "input" => []}, %{
               endpoint: "/backend-api/codex/responses",
               transport: "websocket",
               correlation_id: Ecto.UUID.generate()
             })

    assert {:ok, attempt} = Accounting.create_attempt(reserved.request, setup.assignment)
    %{attempt: attempt, request: reserved.request, setup: setup}
  end

  defp authorize_witness!(fixture) do
    %{setup: setup, attempt: attempt, request: request} = fixture
    clock = DateTime.utc_now() |> DateTime.truncate(:microsecond)
    expired = DateTime.add(clock, -1, :second)
    token = Ecto.UUID.generate()
    owner = Identity.local()
    session = Repo.insert!(%CodexSession{pool_id: setup.pool.id, api_key_id: setup.api_key.id, session_key: "sample-retry-witness-#{Ecto.UUID.generate()}", status: "active", owner_instance_id: owner.node_name, owner_instance_boot_id: owner.boot_id, owner_lease_token: token, owner_lease_expires_at: expired, last_heartbeat_at: expired, created_at: clock, updated_at: clock})
    Repo.insert!(%BridgeOwnerLease{codex_session_id: session.id, pool_id: setup.pool.id, api_key_id: setup.api_key.id, lease_token: token, owner_instance_id: owner.node_name, owner_instance_boot_id: owner.boot_id, status: "active", acquired_at: expired, renewed_at: expired, expires_at: expired, metadata: %{}, created_at: clock, updated_at: clock})
    Repo.insert!(%CodexTurn{codex_session_id: session.id, request_id: request.id, turn_sequence: 1, transport_kind: "websocket", status: "in_progress", started_at: clock, created_at: clock, updated_at: clock})
    cleanup = %{session_id: session.id, owner_instance_id: owner.node_name, owner_lease_token: token, request_id: request.id, attempt_id: attempt.id, replay_generation: 0, downstream_epoch: 1, native_replay_binding: nil}
    state = %{codex_session_id: session.id, owner_lease_token: token, process_generation: 1, producer_identity: ExecutionIdentity.producer(), upstream_pid: self(), pending_handoff: nil, suspended_replay: nil, pending_admissions: %{}, active_turn: %{cleanup_witness: cleanup, task_pid: self(), task_ref: make_ref(), pending_result: nil, admission_request_id: request.id, admission_attempt_id: attempt.id, downstream: %{epoch: 1}, terminal_forwarded?: false, task_settled?: false}}
    candidate = %{session_id: session.id, owner_instance_id: owner.node_name, owner_lease_token: token, owner_lease_expires_at: expired}
    assert {:ok, witness} = Cleanup.within_deadline(System.monotonic_time(:millisecond) + 15_000, fn -> Cleanup.authorize(state, candidate) end)
    assert :ok = DeliveryReceipt.persist(attempt.id, aborted_partial_receipt())
    Map.put(fixture, :witness, witness)
  end

  defp aborted_partial_receipt do
    DeliveryReceipt.build(%{outcome: "aborted", terminal_class: nil, pushed_at: nil, frames_after_visible: 4, transport: "websocket", highest_frame_class: "part_added"})
  end

  defp interrupted_attrs do
    %{
      status: "failed",
      response_status_code: 499,
      last_error_code: "client_disconnected",
      attempt_metadata: %{"attempt_marker" => "finalization"},
      usage: %{status: "usage_unknown", source: "unavailable"}
    }
  end

  defp retry_attrs do
    %{response_status_code: 503, last_error_code: "server_error", attempt_metadata: %{"attempt_marker" => "retry"}}
  end
end
