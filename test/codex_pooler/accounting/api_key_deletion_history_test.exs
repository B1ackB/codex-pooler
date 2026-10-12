defmodule CodexPooler.Accounting.APIKeyDeletionHistoryTest do
  use CodexPooler.DataCase, async: false

  import CodexPooler.AccountingTestSupport
  import CodexPooler.AccountsFixtures

  alias CodexPooler.{Access, Accounting}

  alias CodexPooler.Accounting.{
    APIKeyUsageBucket,
    Attempt,
    LedgerEntry,
    Reporting,
    Request,
    Rollups
  }

  alias CodexPooler.Accounts.Scope

  test "an admitted HTTP attempt settles after its API key is deleted" do
    scope = Scope.for_user(bootstrap_owner_fixture().user, ["instance_owner"])
    setup = accounting_setup()

    assert {:ok, reserved} =
             Accounting.reserve(
               setup.auth,
               setup.model,
               %{"model" => setup.model.exposed_model_id, "input" => []},
               %{
                 endpoint: "/v1/responses",
                 transport: "http_json",
                 correlation_id: Ecto.UUID.generate()
               }
             )

    assert {:ok, attempt} = Accounting.create_attempt(reserved.request, setup.assignment)
    assert {:ok, _} = Access.delete_api_key(scope, setup.api_key)

    assert {:ok, finalized} =
             Accounting.finalize_request(reserved.request, attempt, %{
               response_status_code: 200,
               usage: %{
                 status: "usage_known",
                 source: "upstream",
                 input_tokens: 10,
                 output_tokens: 5,
                 total_tokens: 15
               }
             })

    assert finalized.request.status == "succeeded"
    assert finalized.settlement.api_key_id == nil
    assert finalized.settlement.total_tokens == 15
    refute Accounting.reservation_outstanding?(finalized.request)
  end

  for reload? <- [true, false] do
    test "metadata enrichment preserves admitted identity through key deletion with reload=#{reload?}" do
      {scope, setup, request, attempt} = admitted_http_attempt()
      foreign = accounting_setup()
      principal = request.request_metadata["api_key"]
      forged = %{request | request_metadata: %{"api_key" => %{"id" => foreign.api_key.id, "prefix" => "foreign"}, "api_key_secret" => "synthetic-secret", "routing" => %{"strategy" => "pool_order"}}}

      assert {:ok, enriched} = Accounting.persist_request_metadata(forged, reload?: unquote(reload?))
      assert enriched.request_metadata["api_key"] == principal
      assert enriched.request_metadata["api_key_secret"] == "[REDACTED]"
      assert enriched.request_metadata["routing"]["strategy"] == "pool_order"
      assert {:ok, merged} = Accounting.merge_request_metadata(enriched, %{"api_key" => %{"id" => foreign.api_key.id}, "stage" => "enriched"})
      assert merged.request_metadata["api_key"] == principal
      assert {:ok, _} = Access.delete_api_key(scope, setup.api_key)
      assert Repo.reload!(request).api_key_id == nil
      assert {:ok, result} = Accounting.finalize_request_with_disposition(request, attempt, known_usage_attrs())
      assert result.finalization_disposition == :inserted
      assert result.request.status == "succeeded"
      assert result.request.request_metadata["api_key"] == principal
      assert result.settlement.total_tokens == 15
      assert Decimal.equal?(result.settlement.settled_cost_micros, Decimal.new(200))
      assert result.settlement.api_key_id == nil
      assert {:ok, duplicate} = Accounting.finalize_request_with_disposition(request, attempt, known_usage_attrs())
      assert duplicate.settlement.id == result.settlement.id
      assert duplicate.finalization_disposition == :reused
      refute Accounting.reservation_outstanding?(result.request)
      assert Repo.aggregate(from(l in LedgerEntry, where: l.request_id == ^request.id and l.entry_kind == "settlement" and l.amount_status == "recorded"), :count) == 1
      assert Repo.aggregate(APIKeyUsageBucket, :count) == 0
      assert %{items: [log]} = Accounting.list_request_logs(setup.pool, filters: %{request_id: request.id})
      assert log.status == "succeeded"
      assert Repo.get_by!(CodexPooler.Accounting.RequestLogFact, request_id: request.id).latest_total_tokens == 15
      rollup = Repo.get_by!(CodexPooler.Accounting.DailyRollup, pool_id: setup.pool.id, dimension_kind: "pool")
      assert {rollup.request_count, rollup.success_count, rollup.failure_count, rollup.total_tokens} == {1, 1, 0, 15}
    end
  end

  test "deleted key finalization rejects forged caller provenance without writing" do
    {scope, setup, request, attempt} = admitted_http_attempt()
    foreign = accounting_setup()
    assert {:ok, _} = Access.delete_api_key(scope, setup.api_key)
    before = {Repo.reload!(request), Repo.reload!(attempt), Accounting.list_ledger_entries_for_request(request.id)}

    for {request_arg, attempt_arg} <- [
          {%{request | api_key_id: foreign.api_key.id}, attempt},
          {%{request | api_key_id: "malformed"}, attempt},
          {%{request | pool_id: foreign.pool.id}, attempt},
          {request, %{attempt | request_id: Ecto.UUID.generate()}},
          {request, %{attempt | replay_generation: 1}},
          {%{request | api_key_id: foreign.api_key.id, request_metadata: %{"api_key" => %{"id" => foreign.api_key.id}}}, attempt}
        ] do
      assert {:ok, %{stale_generation?: true}} = Accounting.finalize_request_with_disposition(request_arg, attempt_arg, known_usage_attrs())
      assert before == {Repo.reload!(request), Repo.reload!(attempt), Accounting.list_ledger_entries_for_request(request.id)}
    end
  end

  for snapshot <- [nil, "[REDACTED]", %{}, %{"id" => "malformed"}, %{"id" => 12}] do
    test "deleted key finalization rejects unavailable historical principal #{inspect(snapshot)}" do
      {scope, setup, request, attempt} = admitted_http_attempt()
      assert {:ok, _} = Access.delete_api_key(scope, setup.api_key)
      stored = Repo.reload!(request)
      stored |> Ecto.Changeset.change(request_metadata: Map.put(stored.request_metadata, "api_key", unquote(Macro.escape(snapshot)))) |> Repo.update!()
      assert {:ok, %{stale_generation?: true}} = Accounting.finalize_request_with_disposition(request, attempt, known_usage_attrs())
      assert Repo.reload!(request).status == "in_progress"
      assert Repo.aggregate(from(l in LedgerEntry, where: l.request_id == ^request.id and l.entry_kind == "settlement"), :count) == 0
    end
  end

  test "nil historical foreign key retains the existing reloaded request contract" do
    {scope, setup, request, attempt} = admitted_http_attempt()
    assert {:ok, _} = Access.delete_api_key(scope, setup.api_key)
    assert {:ok, result} = Accounting.finalize_request_with_disposition(Repo.reload!(request), attempt, known_usage_attrs())
    assert result.request.status == "succeeded"
    assert result.settlement.total_tokens == 15
  end

  test "missing persisted principal cannot be restored from caller metadata" do
    {scope, setup, request, attempt} = admitted_http_attempt()
    stored = Repo.reload!(request)
    stored |> Ecto.Changeset.change(request_metadata: Map.delete(stored.request_metadata, "api_key")) |> Repo.update!()
    assert {:ok, enriched} = Accounting.persist_request_metadata(request, reload?: false)
    assert enriched.request_metadata["api_key"] == "[REDACTED]"
    assert {:ok, _} = Access.delete_api_key(scope, setup.api_key)
    assert {:ok, %{stale_generation?: true}} = Accounting.finalize_request_with_disposition(request, attempt, known_usage_attrs())
  end

  test "deleted key does not match a foreign persisted principal" do
    {scope, setup, request, attempt} = admitted_http_attempt()
    foreign = accounting_setup()
    assert {:ok, _} = Access.delete_api_key(scope, setup.api_key)
    stored = Repo.reload!(request)
    stored |> Ecto.Changeset.change(request_metadata: Map.put(stored.request_metadata, "api_key", %{"id" => foreign.api_key.id})) |> Repo.update!()
    assert {:ok, %{stale_generation?: true}} = Accounting.finalize_request_with_disposition(request, attempt, known_usage_attrs())
    assert Repo.reload!(request).status == "in_progress"
    assert Repo.aggregate(from(l in LedgerEntry, where: l.request_id == ^request.id and l.entry_kind == "settlement"), :count) == 0
  end

  for transition <- [:pause_api_key, :revoke_api_key] do
    test "admitted HTTP work settles after #{transition}" do
      {scope, setup, request, attempt} = admitted_http_attempt()
      assert {:ok, _} = apply(Access, unquote(transition), [scope, setup.api_key])
      assert {:ok, result} = Accounting.finalize_request_with_disposition(request, attempt, known_usage_attrs())
      assert result.request.status == "succeeded"
      assert result.settlement.total_tokens == 15
      assert result.settlement.api_key_id == setup.api_key.id
    end
  end

  defp admitted_http_attempt do
    scope = Scope.for_user(bootstrap_owner_fixture().user, ["instance_owner"])
    setup = accounting_setup()
    assert {:ok, reserved} = Accounting.reserve(setup.auth, setup.model, %{"model" => setup.model.exposed_model_id, "input" => []}, %{endpoint: "/v1/responses", transport: "http_json", correlation_id: Ecto.UUID.generate()})
    assert {:ok, attempt} = Accounting.create_attempt(reserved.request, setup.assignment)
    {scope, setup, reserved.request, attempt}
  end

  defp known_usage_attrs do
    %{response_status_code: 200, usage: %{status: "usage_known", source: "upstream", input_tokens: 10, output_tokens: 5, total_tokens: 15}}
  end

  test "deleting an API key preserves its settlement and pool usage history" do
    scope =
      Scope.for_user(bootstrap_owner_fixture().user, ["instance_owner"])

    setup = accounting_setup()

    assert {:ok, reserved} =
             Accounting.reserve(
               setup.auth,
               setup.model,
               %{"model" => setup.model.exposed_model_id, "input" => []},
               %{
                 endpoint: "/v1/responses",
                 transport: "http_json",
                 correlation_id: Ecto.UUID.generate()
               }
             )

    assert {:ok, attempt} = Accounting.create_attempt(reserved.request, setup.assignment)

    assert {:ok, finalized} =
             Accounting.finalize_request(reserved.request, attempt, %{
               response_status_code: 200,
               usage: %{
                 status: "usage_known",
                 source: "upstream",
                 input_tokens: 10,
                 output_tokens: 5,
                 total_tokens: 15
               }
             })

    started_at = DateTime.add(finalized.request.admitted_at, -60, :second)
    ended_at = DateTime.add(finalized.request.completed_at, 60, :second)
    pools = [setup.pool.id]
    before_usage = Reporting.token_totals_by_pool_ids(pools, started_at, ended_at)
    before_cost = Reporting.settled_cost_totals_by_pool_ids(pools, started_at, ended_at)
    assert before_usage == %{setup.pool.id => 15}
    assert before_cost[setup.pool.id] > 0

    assert {:ok, _deleted} = Access.delete_api_key(scope, setup.api_key)
    assert Repo.get!(Request, finalized.request.id).api_key_id == nil
    assert Repo.get!(Attempt, attempt.id).request_id == finalized.request.id
    assert %LedgerEntry{api_key_id: nil} = Repo.get(LedgerEntry, finalized.settlement.id)
    assert Reporting.token_totals_by_pool_ids(pools, started_at, ended_at) == before_usage
    assert Reporting.settled_cost_totals_by_pool_ids(pools, started_at, ended_at) == before_cost

    assert {:ok, 1} =
             Rollups.rebuild_for_date(DateTime.to_date(finalized.request.completed_at))

    request = Repo.reload!(finalized.request)
    settlement = Repo.reload!(finalized.settlement)
    assert :ok = Rollups.replace!(request, settlement, request, settlement)

    # Real trigger boundary: retained history can still be corrected or voided
    # without recreating a usage bucket for a missing key.
    settlement = Repo.get!(LedgerEntry, finalized.settlement.id)

    settlement
    |> Ecto.Changeset.change(%{amount_status: "voided"})
    |> Repo.update!()

    assert Reporting.token_totals_by_pool_ids(pools, started_at, ended_at) == %{}
    assert Reporting.settled_cost_totals_by_pool_ids(pools, started_at, ended_at) == %{}

    settlement
    |> Repo.reload!()
    |> Ecto.Changeset.change(%{amount_status: "recorded", total_tokens: 20})
    |> Repo.update!()

    assert Reporting.token_totals_by_pool_ids(pools, started_at, ended_at) == %{
             setup.pool.id => 20
           }

    assert Repo.aggregate(APIKeyUsageBucket, :count) == 0

    Repo.delete!(setup.pool)
    assert Repo.get(Request, finalized.request.id) == nil
    assert Repo.get(Attempt, attempt.id) == nil
    assert Repo.get(LedgerEntry, finalized.settlement.id) == nil
  end
end
