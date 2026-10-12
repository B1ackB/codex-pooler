defmodule CodexPooler.Accounting.ArmedReplayDeliveryReceiptTest do
  use ExUnit.Case, async: false
  use CodexPooler.CommittedWriteGuard

  import Ecto.Query
  import CodexPooler.AccountsFixtures, only: [committed_bootstrap_owner_fixture!: 0]
  import CodexPooler.PoolerFixtures, only: [ledger_entry_fixture: 2, attempt_fixture: 3]
  import CodexPooler.RequestReplayFixtures

  alias CodexPooler.Accounting
  alias CodexPooler.Accounting.{Attempt, LedgerEntry, Request, RequestReplay, RequestReplayEntitlement}
  alias CodexPooler.Gateway.Persistence.{CodexTurn, SessionContinuity}
  alias CodexPooler.Gateway.Transports.Websocket.WebsocketOwnerSession
  alias CodexPooler.Gateway.Websocket.DeliveryReceipt
  alias CodexPooler.Repo
  alias Ecto.Adapters.SQL.Sandbox

  setup do
    fixture = committed_fixture!()
    observer = start_supervised!({Postgrex, Keyword.take(Repo.config(), [:hostname, :port, :username, :password, :database])})
    %{fixture: fixture, observer: observer}
  end

  test "armed original records only metadata and remains fenced from finalization and visibility", %{fixture: fixture, observer: observer} do
    unboxed(fn ->
      assert {:ok, _armed} = RequestReplay.arm(arm_input(fixture))
      before = snapshot(fixture)
      observed_before = observe(observer, fixture)
      receipt = receipt()
      assert :ok = DeliveryReceipt.persist(fixture.attempt.id, receipt, context(fixture))
      after_receipt = snapshot(fixture)
      assert_metadata_only(before, after_receipt, receipt)
      assert :ok = DeliveryReceipt.persist(fixture.attempt.id, receipt, context(fixture))
      assert snapshot(fixture) == after_receipt
      assert {:ok, %{stale_generation?: true}} = Accounting.finalize_failure_with_disposition(fixture.request, fixture.attempt, %{last_error_code: "client_disconnected"})
      assert {:ok, %{stale_generation?: true}} = Accounting.reconcile_request_with_disposition(fixture.request, fixture.attempt, %{delivery_failure: failure()})
      assert {:error, :stale_generation} = SessionContinuity.mark_codex_turn_visible(fixture.request, fixture.attempt)
      assert snapshot(fixture) == after_receipt
      observed_after = observe(observer, fixture)
      assert List.replace_at(observed_after, 5, nil) == observed_before
      assert Enum.at(observed_after, 5) == receipt
      CodexPooler.TestDiagnostics.puts("armed receipt committed before=#{inspect(observed_before)} after=#{inspect(observed_after)} lifecycle_ledger_entitlement_unchanged=true duplicate_unchanged=true generic_finalization_stale=true")
    end)
  end

  test "armed metadata path ignores valid and malformed outcome witnesses and never invokes callbacks", %{fixture: fixture} do
    unboxed(fn ->
      assert {:ok, _armed} = RequestReplay.arm(arm_input(fixture))
      before = snapshot(fixture)

      for witness <- [failure(), %{status: "failed", response_status_code: 999, error_code: String.duplicate("invalid", 100)}, nil] do
        attrs = %{before_commit: fn _ -> flunk("metadata-only receipt invoked finalization callback") end}
        assert {:ok, result} = Accounting.record_delivery_receipt(Map.put(context(fixture), :delivery_failure, witness), receipt(), attrs)
        refute Map.get(result, :stale_generation?, false)
        assert_metadata_only(before, snapshot(fixture), receipt())
      end
    end)
  end

  test "positively delivered receipt cannot be replaced by armed original close", %{fixture: fixture} do
    unboxed(fn ->
      assert {:ok, _armed} = RequestReplay.arm(arm_input(fixture))
      delivered = DeliveryReceipt.build(%{outcome: "delivered", terminal_class: "response.completed"})
      assert :ok = DeliveryReceipt.persist(fixture.attempt.id, delivered)
      before = snapshot(fixture)
      assert :ok = DeliveryReceipt.persist(fixture.attempt.id, receipt(), context(fixture))
      assert snapshot(fixture) == before
    end)
  end

  for mutation <- [:generation, :session, :missing_session, :malformed_session, :foreign_request, :newer_attempt, :known_settlement, :completed_turn, :interrupted_turn, :completed_request] do
    @tag mutation: mutation
    test "#{mutation} cannot authorize an armed original receipt", state do
      assert_rejected(state)
    end
  end

  for scope <- [:api_key_id, :pool_id] do
    @tag scope: scope
    test "foreign #{scope} cannot arm or replace the trusted replay snapshot", %{fixture: fixture, scope: scope} do
      other = committed_fixture!()
      foreign_id = if scope == :api_key_id, do: other.api_key.id, else: other.pool.id

      unboxed(fn ->
        assert {:error, :ineligible} = RequestReplay.arm(Map.put(arm_input(fixture), scope, foreign_id))
        assert Repo.get_by(RequestReplayEntitlement, request_id: fixture.request.id) == nil
        assert {:ok, _armed} = RequestReplay.arm(arm_input(fixture))
        before = snapshot(fixture)

        assert_raise Ecto.ConstraintError, ~r/request_replay_entitlements_snapshot_immutable_check/, fn ->
          before.entitlement |> Ecto.Changeset.change([{scope, foreign_id}]) |> Repo.update!()
        end

        assert snapshot(fixture) == before
        CodexPooler.TestDiagnostics.puts("armed receipt scope=#{scope} foreign_arm_rejected=true immutable_snapshot_enforced=true")
      end)
    end
  end

  test "consumed generation one rejects late original receipt without touching successor", %{fixture: fixture, observer: observer} do
    assert {:ok, armed} = unboxed(fn -> RequestReplay.arm(arm_input(fixture)) end)
    assert :ok = unboxed(fn -> DeliveryReceipt.persist(fixture.attempt.id, receipt(), context(fixture)) end)
    input = unboxed(fn -> consume_input(fixture, armed, :crypto.strong_rand_bytes(32)) end)
    database_owner = Sandbox.start_owner!(Repo, sandbox: false)
    on_exit(fn -> Sandbox.stop_owner(database_owner) end)
    {:ok, owner} = WebsocketOwnerSession.lookup(fixture.session.id)
    :ok = Sandbox.allow(Repo, database_owner, owner)

    unboxed(fn ->
      assert {:ok, consumed} = RequestReplay.consume(input)
      assert consumed.attempt.replay_generation == 1
      before = snapshot(fixture)
      late_receipt = DeliveryReceipt.build(%{outcome: "delivered", terminal_class: "response.completed"})
      assert :ok = DeliveryReceipt.persist(fixture.attempt.id, late_receipt, context(fixture))
      assert snapshot(fixture) == before
      assert Repo.reload!(consumed.attempt) == consumed.attempt
      assert Enum.at(observe(observer, fixture), 8) == "consumed"
      CodexPooler.TestDiagnostics.puts("consumed receipt original_no_write=true successor_unchanged=true attempts=2")
      stop_replay_owner(fixture.session.id)
    end)
  end

  defp assert_rejected(%{fixture: fixture, mutation: mutation}) do
    other = if mutation == :foreign_request, do: committed_fixture!()

    unboxed(fn ->
      assert {:ok, _armed} = RequestReplay.arm(arm_input(fixture))
      context = mutate(mutation, fixture, other)
      before = snapshot(fixture)
      assert {:ok, %{stale_generation?: true}} = Accounting.record_delivery_receipt(context, receipt(), %{})
      assert snapshot(fixture) == before
      CodexPooler.TestDiagnostics.puts("armed receipt negative=#{mutation} all_rows_unchanged=true")
    end)
  end

  defp mutate(:generation, fixture, _), do: %{context(fixture) | replay_generation: 1}
  defp mutate(:session, fixture, _), do: %{context(fixture) | codex_session_id: Ecto.UUID.generate()}
  defp mutate(:missing_session, fixture, _), do: Map.delete(context(fixture), :codex_session_id)
  defp mutate(:malformed_session, fixture, _), do: %{context(fixture) | codex_session_id: 17}
  defp mutate(:foreign_request, fixture, other), do: %{context(fixture) | request_id: other.request.id}

  defp mutate(kind, fixture, _) when kind in [:completed_turn, :interrupted_turn] do
    status = if kind == :completed_turn, do: "succeeded", else: "interrupted"
    fixture.turn |> Ecto.Changeset.change(status: status, completed_at: DateTime.utc_now(), final_attempt_id: fixture.attempt.id) |> Repo.update!()
    context(fixture)
  end

  defp mutate(:completed_request, fixture, _) do
    fixture.request |> Ecto.Changeset.change(status: "failed", completed_at: DateTime.utc_now()) |> Repo.update!()
    context(fixture)
  end

  defp mutate(:newer_attempt, fixture, _) do
    attempt_fixture(fixture.request, fixture.assignment, %{attempt_number: 2, replay_generation: 0})
    context(fixture)
  end

  defp mutate(:known_settlement, fixture, _) do
    ledger_entry_fixture(fixture.request, %{entry_kind: "settlement", amount_status: "recorded", usage_status: "usage_known", attempt_id: fixture.attempt.id, total_tokens: 0, source_event_id: "receipt-known-#{fixture.request.id}"})
    context(fixture)
  end

  defp context(fixture), do: %{request_id: fixture.request.id, attempt_id: fixture.attempt.id, codex_session_id: fixture.session.id, replay_generation: 0, delivery_failure: failure()}
  defp failure, do: %{status: "failed", response_status_code: 499, error_code: "client_disconnected"}
  defp receipt, do: DeliveryReceipt.build(%{outcome: "aborted", terminal_class: nil, highest_frame_class: "none", frames_after_visible: 0})

  defp assert_metadata_only(before, after_receipt, receipt) do
    expected_attempt = %{before.attempt | response_metadata: Map.put(before.attempt.response_metadata, "downstream_delivery", receipt)}
    assert after_receipt == %{before | attempt: expected_attempt, attempts: [expected_attempt]}
    refute Map.has_key?(after_receipt.attempt.response_metadata, "delivery_failure")
  end

  defp snapshot(fixture) do
    %{request: Repo.get!(Request, fixture.request.id), attempt: Repo.get!(Attempt, fixture.attempt.id), turn: Repo.get!(CodexTurn, fixture.turn.id), entitlement: Repo.get_by!(RequestReplayEntitlement, request_id: fixture.request.id), ledger: Repo.all(from row in LedgerEntry, where: row.request_id == ^fixture.request.id, order_by: row.id), attempts: Repo.all(from row in Attempt, where: row.request_id == ^fixture.request.id, order_by: row.attempt_number)}
  end

  defp observe(observer, fixture) do
    [row] = Postgrex.query!(observer, "SELECT current_database(), r.status, a.status, a.replay_generation, t.status, a.response_metadata->'downstream_delivery', (SELECT count(*) FROM ledger_entries l WHERE l.request_id=r.id), (SELECT count(*) FROM attempts x WHERE x.request_id=r.id), e.status, e.replay_generation, e.replay_attempt_id IS NULL, e.closed_at IS NULL FROM requests r JOIN attempts a ON a.id=$2 AND a.request_id=r.id JOIN codex_turns t ON t.request_id=r.id JOIN request_replay_entitlements e ON e.request_id=r.id WHERE r.id=$1", Enum.map([fixture.request.id, fixture.attempt.id], &Ecto.UUID.dump!/1)).rows
    assert hd(row) == Repo.config()[:database]
    row
  end

  defp committed_fixture! do
    %{user: owner} = committed_bootstrap_owner_fixture!()
    holder = String.to_atom("armed-receipt-fixture-#{System.unique_integer([:positive])}")

    on_exit(fn -> cleanup_holder!(holder) end)
    {:ok, _holder} = Agent.start(fn -> nil end, name: holder)

    unboxed(fn ->
      {:ok, fixture} = Repo.transaction(fn -> insert_fixture!(owner, holder) end)

      fixture
    end)
  end

  defp insert_fixture!(owner, holder) do
    fixture = replay_fixture(reservation?: true, owner: owner)
    Agent.update(holder, fn _ -> fixture end)
    fixture
  end

  defp cleanup_holder!(holder) do
    if pid = Process.whereis(holder) do
      try do
        if fixture = Agent.get(pid, & &1), do: CodexPooler.UnboxedFixture.cleanup_unboxed!(fn -> delete_fixture!(fixture) end)
      after
        Agent.stop(pid)
      end
    end
  end

  defp delete_fixture!(fixture) do
    case WebsocketOwnerSession.lookup(fixture.session.id) do
      {:ok, owner} ->
        Sandbox.allow(Repo, self(), owner)
        stop_replay_owner(fixture.session.id)

      {:error, :owner_unavailable} ->
        :ok
    end

    Repo.delete_all(from row in RequestReplayEntitlement, where: row.request_id == ^fixture.request.id)
    CodexPooler.PoolerFixtures.delete_committed_pools!([fixture.pool.id])
    Repo.delete_all(from row in CodexPooler.Upstreams.Schemas.UpstreamIdentity, where: row.id == ^fixture.identity.id)
  end

  defp unboxed(fun), do: Sandbox.unboxed_run(Repo, fun)
end
