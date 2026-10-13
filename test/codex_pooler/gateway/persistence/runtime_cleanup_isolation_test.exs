defmodule CodexPooler.Gateway.Persistence.RuntimeCleanupIsolationTest do
  use CodexPooler.DataCase, async: false

  import CodexPooler.PoolerFixtures
  import ExUnit.CaptureLog

  alias CodexPooler.Gateway.OperationalSettings
  alias CodexPooler.Gateway.Persistence.{BridgeOwnerLease, BridgeSessionAlias, CodexSession, CodexTurn, IdempotencyKey, RuntimeCleanup}
  alias CodexPooler.Jobs.RuntimeStateCleanup
  alias CodexPooler.Platform.InstancePresence

  setup do
    CodexPooler.TestAppEnv.restore_on_exit(:runtime_cleanup_owner_candidate_test_barrier)
    %{now: InstancePresence.database_now()}
  end

  for blocked_index <- [0, 1] do
    @tag blocked_index: blocked_index
    test "an unknown owner in candidate position #{blocked_index} preserves independent recovery and TTL progress", %{now: now, blocked_index: blocked_index} do
      fixtures = [owner_fixture(now), owner_fixture(now)]
      refute hd(fixtures).pool.id == List.last(fixtures).pool.id
      ttl = ttl_fixture(hd(fixtures), now)

      {result, blocked, recovered} = run_with_blocked_candidate(fixtures, blocked_index, fn -> RuntimeCleanup.cleanup_expired_runtime_state(now) end)

      assert {:error, {:runtime_cleanup_failed, failures}, summary} = result
      refute Enum.empty?(failures)
      assert summary.expired_owner_sessions_recovered == 1
      assert summary.expired_owner_sessions_failed == 1
      assert_ttl_progress(summary, ttl)
      assert_unchanged(blocked)
      assert_recovered(recovered)
    end
  end

  test "the full job retains gateway partial progress and the six-hour backstop still clears eligible abandoned requests", %{now: now} do
    fixtures = [owner_fixture(now), owner_fixture(now)]
    ttl = ttl_fixture(hd(fixtures), now)

    {{result, blocked, recovered}, log} = with_log(fn -> run_with_blocked_candidate(fixtures, 0, fn -> RuntimeStateCleanup.run(now) end) end)

    assert {:error, {:runtime_state_cleanup_steps_failed, [:gateway_runtime], summary}} = result
    assert summary.expired_owner_sessions_recovered == 1
    assert summary.expired_owner_sessions_failed == 1
    assert_ttl_progress(summary, ttl)
    assert_unchanged(blocked)
    assert_recovered(recovered)
    assert log =~ "runtime state cleanup step gateway_runtime failed"
    assert log =~ "runtime state cleanup completed with failures [:gateway_runtime]"

    blocked.request
    |> Ecto.Changeset.change(admitted_at: DateTime.add(now, -7, :hour))
    |> Repo.update!()

    assert {:ok, final_summary} = RuntimeStateCleanup.run(now)
    assert final_summary.expired_owner_sessions_failed == 0
    assert Repo.reload!(blocked.request).status == "failed"
    assert Repo.reload!(blocked.turn).status == "interrupted"
    assert Repo.reload!(blocked.lease).status == "expired"
  end

  test "a caller transaction with a physical owner candidate is refused before any cleanup writes", %{now: now} do
    blocked = now |> owner_fixture() |> require_physical_stop()
    recoverable = owner_fixture(now)
    ttl = ttl_fixture(recoverable, now)

    assert {:ok, {:error, :caller_transaction}} = Repo.transaction(fn -> RuntimeCleanup.cleanup_expired_runtime_state(now) end)

    assert_unchanged(blocked)
    assert_unchanged(recoverable)
    assert Repo.reload!(ttl.alias) == ttl.alias
    assert Repo.reload!(ttl.key) == ttl.key
    assert Repo.reload!(ttl.lease) == ttl.lease
    assert Repo.reload!(ttl.session) == ttl.session
  end

  test "failure samples stay bounded while the summary counts every unresolved owner", %{now: now} do
    blocked = for _ <- 1..12, do: now |> owner_fixture() |> require_physical_stop()
    recoverable = owner_fixture(now)

    assert {:error, {:runtime_cleanup_failed, failures}, summary} = RuntimeCleanup.cleanup_expired_runtime_state(now)
    assert summary.expired_owner_sessions_failed == 12
    assert summary.expired_owner_sessions_recovered == 1
    assert length(failures[:owner_recovery]) == 10
    ids = Enum.map(blocked, & &1.session.id)
    assert Enum.all?(failures[:owner_recovery], fn {id, reason} -> id in ids and reason == :owner_unavailable end)
    Enum.each(blocked, &assert_unchanged/1)
    assert_recovered(recoverable)
  end

  test "a real accounting exception rolls back its candidate and does not skip the next candidate", %{now: now} do
    fixtures = [owner_fixture(now, no_attempt: true), owner_fixture(now, no_attempt: true)]

    detach_reservation = fn fixture ->
      entry = Repo.get_by!(CodexPooler.Accounting.LedgerEntry, source_event_id: "request:#{fixture.request.id}:reservation")
      entry |> Ecto.Changeset.change(source_event_id: "request:#{fixture.request.id}:detached-reservation") |> Repo.update!()
      fixture
    end

    {result, blocked, recovered} = run_with_blocked_candidate(fixtures, 0, fn -> RuntimeCleanup.cleanup_expired_runtime_state(now) end, detach_reservation)

    assert {:error, {:runtime_cleanup_failed, failures}, summary} = result
    assert summary.expired_owner_sessions_failed == 1
    assert summary.expired_owner_sessions_recovered == 1
    assert failures[:owner_recovery] == [{blocked.session.id, {:raised, Ecto.NoResultsError}}]
    assert_unchanged(blocked)
    assert_recovered(recovered)
  end

  defp run_with_blocked_candidate(fixtures, blocked_index, run, block \\ &require_physical_stop/1) do
    ref = make_ref()
    Application.put_env(:codex_pooler, :runtime_cleanup_owner_candidate_test_barrier, {self(), ref})
    task = Task.async(run)

    try do
      assert_receive {:runtime_cleanup_owner_candidates_selected, pid, ^ref, candidates}, 5_000
      assert length(candidates) == 2
      blocked_id = Enum.at(candidates, blocked_index).session_id
      blocked = fixtures |> Enum.find(&(&1.session.id == blocked_id)) |> block.()
      recovered = Enum.find(fixtures, &(&1.session.id != blocked_id))
      send(pid, {:release_runtime_cleanup_owner_candidates, ref})
      {Task.await(task, 15_000), blocked, recovered}
    after
      Application.delete_env(:codex_pooler, :runtime_cleanup_owner_candidate_test_barrier)
      send(task.pid, {:release_runtime_cleanup_owner_candidates, ref})
    end
  end

  defp require_physical_stop(fixture) do
    request = fixture.request |> Ecto.Changeset.change(request_metadata: Map.put(fixture.request.request_metadata, "websocket_owner_forwarding", %{})) |> Repo.update!()
    %{fixture | request: request}
  end

  defp assert_unchanged(fixture) do
    for record <- [fixture.request, fixture.attempt, fixture.turn, fixture.session, fixture.lease], not is_nil(record), do: assert(Repo.reload!(record) == record)
  end

  defp assert_recovered(fixture) do
    assert Repo.reload!(fixture.request).status == "failed"
    assert Repo.reload!(fixture.request).last_error_code == "owner_unavailable"
    assert Repo.reload!(fixture.turn).status == "interrupted"
    assert Repo.reload!(fixture.lease).status == "expired"
    assert Repo.reload!(fixture.session).status == "interrupted"
  end

  defp assert_ttl_progress(summary, ttl) do
    assert summary.expired_aliases == 1
    assert summary.expired_idempotency_keys == 1
    assert summary.expired_owner_leases >= 1
    assert summary.closed_retired_sessions == 1
    assert Repo.reload!(ttl.alias).status == "expired"
    assert Repo.reload!(ttl.key).status == "expired"
    assert Repo.reload!(ttl.lease).status == "expired"
    assert Repo.reload!(ttl.session).status == "closed"
  end

  defp owner_fixture(now, opts \\ []) do
    %{pool: pool, api_key: api_key} = active_api_key_fixture()
    %{assignment: assignment} = upstream_assignment_fixture(pool)
    model = model_fixture(pool, %{exposed_model_id: "sample-cleanup-#{System.unique_integer([:positive])}"})
    expired_at = DateTime.add(now, -60, :second)
    session = session_fixture(pool, api_key, assignment, expired_at)
    lease = lease_fixture(session, assignment, expired_at)

    request = request_fixture(%{pool: pool, api_key: api_key}, %{model_id: model.id, requested_model: model.exposed_model_id, transport: "websocket", status: "in_progress", usage_status: "usage_pending", completed_at: nil, response_status_code: nil, request_metadata: %{"codex_session_id" => session.id}})
    attempt = if !opts[:no_attempt], do: attempt_fixture(request, assignment, %{status: "in_progress", completed_at: nil, usage_status: "usage_pending", response_metadata: %{}})
    attempt_id = if attempt, do: attempt.id
    turn = Repo.insert!(%CodexTurn{codex_session_id: session.id, request_id: request.id, turn_sequence: 1, transport_kind: "websocket", final_attempt_id: attempt_id, status: "in_progress", started_at: expired_at, created_at: expired_at, updated_at: expired_at})

    request
    |> ledger_entry_fixture(%{attempt_id: attempt_id, pool_upstream_assignment_id: assignment.id, upstream_identity_id: assignment.upstream_identity_id, entry_kind: "reservation", amount_status: "recorded", usage_status: "usage_pending", transport: "websocket", details: %{"source" => "test_reservation"}})
    |> Ecto.Changeset.change(source_event_id: "request:#{request.id}:reservation")
    |> Repo.update!()

    %{pool: pool, api_key: api_key, assignment: assignment, request: request, attempt: attempt, session: session, lease: lease, turn: turn}
  end

  defp ttl_fixture(fixture, now) do
    retired_at = DateTime.add(now, -(OperationalSettings.current().expired_alias_ttl_seconds + 60), :second)
    session = session_fixture(fixture.pool, fixture.api_key, fixture.assignment, retired_at)
    lease = lease_fixture(session, fixture.assignment, retired_at)
    alias_record = Repo.insert!(%BridgeSessionAlias{codex_session_id: session.id, pool_id: fixture.pool.id, api_key_id: fixture.api_key.id, alias_kind: "session_header", alias_hash: :crypto.hash(:sha256, session.id), alias_preview: "sample", status: "active", expires_at: retired_at, created_at: retired_at, updated_at: retired_at})
    key = Repo.insert!(%IdempotencyKey{pool_id: fixture.pool.id, api_key_id: fixture.api_key.id, scope: "sample-cleanup", key_hash: :crypto.hash(:sha256, session.id), status: "succeeded", expires_at: retired_at, created_at: retired_at, updated_at: retired_at})
    %{session: session, lease: lease, alias: Repo.reload!(alias_record), key: Repo.reload!(key)}
  end

  defp session_fixture(pool, api_key, assignment, expired_at) do
    Repo.insert!(%CodexSession{pool_id: pool.id, api_key_id: api_key.id, session_key: "sample-session-#{System.unique_integer([:positive])}", pool_upstream_assignment_id: assignment.id, status: "active", owner_instance_id: "sample-owner@unreachable.invalid", owner_instance_boot_id: Ecto.UUID.generate(), owner_lease_token: Ecto.UUID.generate(), owner_lease_expires_at: expired_at, last_heartbeat_at: expired_at, created_at: expired_at, updated_at: expired_at})
  end

  defp lease_fixture(session, assignment, expired_at) do
    Repo.insert!(%BridgeOwnerLease{codex_session_id: session.id, pool_id: session.pool_id, api_key_id: session.api_key_id, pool_upstream_assignment_id: assignment.id, owner_instance_id: session.owner_instance_id, owner_instance_boot_id: session.owner_instance_boot_id, lease_token: session.owner_lease_token, status: "active", acquired_at: expired_at, renewed_at: expired_at, expires_at: expired_at, metadata: %{}, created_at: expired_at, updated_at: expired_at})
  end
end
