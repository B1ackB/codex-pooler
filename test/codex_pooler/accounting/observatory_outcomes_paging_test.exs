defmodule CodexPooler.Accounting.ObservatoryOutcomesPagingTest do
  use CodexPooler.DataCase, async: false

  import CodexPooler.PoolerFixtures

  alias CodexPooler.Access.APIKey
  alias CodexPooler.Access.DashboardSessions.Principal, as: DashboardPrincipal
  alias CodexPooler.Accounting.ObservatoryDashboardPrincipalSupport, as: Support
  alias CodexPooler.Accounting.Usage.Observatory
  alias CodexPooler.Repo

  @as_of ~U[2026-10-10 12:00:00Z]

  setup do
    pool = pool_fixture()
    %{api_key: api_key} = active_api_key_fixture(pool)
    api_key = api_key |> APIKey.changeset(%{dashboard_access: true}) |> Repo.update!()
    %{pool: pool, api_key: api_key, principal: principal(pool, api_key)}
  end

  @tag slow: "crosses two 200-row pages with 405 real request and ledger settlements sharing one timestamp"
  test "keyset pages exhaust more than two batches without dropping or repeating tied timestamps", context do
    timestamp = ~U[2026-10-10 11:59:59.123456Z]

    ordered =
      for tokens <- 1..405 do
        request = timed_request(context, timestamp)
        ledger_entry_fixture(request, %{total_tokens: tokens})
        {request.id, tokens}
      end
      |> Enum.sort_by(&elem(&1, 0), :desc)

    assert {:ok, first} = Observatory.read(context.principal, "7d", as_of: @as_of)
    assert length(first.outcomes) == 200
    assert first.outcomes_page == %{as_of: @as_of, has_more: true, next_cursor: %{timestamp: timestamp, id: elem(Enum.at(ordered, 199), 0)}}

    {{:ok, second}, projections} =
      Support.collect_repo_queries(fn ->
        Observatory.read_outcomes(context.principal, as_of: first.outcomes_page.as_of, before: first.outcomes_page.next_cursor)
      end)

    assert projections == [:observatory_principal, :observatory_outcomes]
    assert length(second.outcomes) == 200
    assert second.outcomes_page.next_cursor == %{timestamp: timestamp, id: elem(Enum.at(ordered, 399), 0)}
    assert second.outcomes_page.has_more

    assert {:ok, third} = Observatory.read_outcomes(context.principal, as_of: @as_of, before: second.outcomes_page.next_cursor)
    assert length(third.outcomes) == 5
    assert third.outcomes_page == %{as_of: @as_of, has_more: false, next_cursor: nil}

    assert Enum.map(first.outcomes ++ second.outcomes ++ third.outcomes, & &1.total_tokens) == Enum.map(ordered, &elem(&1, 1))
    refute Enum.any?(first.outcomes ++ second.outcomes ++ third.outcomes, &Map.has_key?(&1, :id))
  end

  test "recent outcomes use exactly the last hour independently of the chart window", context do
    for timestamp <- [~U[2026-10-09 12:00:00.000000Z], ~U[2026-10-10 10:59:59.999999Z], ~U[2026-10-10 11:00:00.000000Z], ~U[2026-10-10 11:59:59.999999Z], ~U[2026-10-10 12:00:00.000000Z]] do
      timed_request(context, timestamp)
    end

    for window <- ["1h", "5h", "24h", "7d"] do
      assert {:ok, report} = Observatory.read(context.principal, window, as_of: @as_of)
      assert Enum.map(report.outcomes, & &1.timestamp) == [~U[2026-10-10 11:59:59.999999Z], ~U[2026-10-10 11:00:00.000000Z]]
      assert report.outcomes_page == %{as_of: @as_of, has_more: false, next_cursor: nil}
      assert report.totals.requests.total == %{"1h" => 2, "5h" => 3, "24h" => 4, "7d" => 4}[window]
    end
  end

  test "every page remains key and Pool scoped and revalidates the principal", context do
    visible = timed_request(context, ~U[2026-10-10 11:59:58.000000Z])
    ledger_entry_fixture(visible, %{total_tokens: 23})
    %{api_key: other_key} = active_api_key_fixture(context.pool)
    other_pool = pool_fixture()
    timed_request(%{pool: context.pool, api_key: other_key}, ~U[2026-10-10 11:59:58.000000Z])
    timed_request(%{pool: other_pool, api_key: context.api_key}, ~U[2026-10-10 11:59:58.000000Z])
    cursor = %{timestamp: ~U[2026-10-10 11:59:59.000000Z], id: Ecto.UUID.generate()}

    assert {:ok, %{outcomes: [%{total_tokens: 23}]}} = Observatory.read_outcomes(context.principal, as_of: @as_of, before: cursor)
    assert {:error, %{code: :unauthorized}} = Observatory.read_outcomes(principal(other_pool, context.api_key), as_of: @as_of, before: cursor)

    context.api_key |> APIKey.changeset(%{dashboard_access: false}) |> Repo.update!()
    assert {:error, %{code: :unauthorized}} = Observatory.read_outcomes(context.principal, as_of: @as_of, before: cursor)
    assert {:error, %{code: :unauthorized}} = Observatory.read_outcomes(%{api_key_id: context.api_key.id, pool_id: context.pool.id}, as_of: @as_of)
  end

  test "a full final batch does not advertise a nonexistent next page", context do
    for offset <- 1..200, do: timed_request(context, DateTime.add(@as_of, -offset))
    assert {:ok, report} = Observatory.read_outcomes(context.principal, as_of: @as_of)
    assert length(report.outcomes) == 200
    assert report.outcomes_page == %{as_of: @as_of, has_more: false, next_cursor: nil}
  end

  test "invalid cursors and caller supplied scope fail before queries", context do
    valid = %{timestamp: ~U[2026-10-10 11:30:00Z], id: Ecto.UUID.generate()}

    for opts <- [
          [as_of: @as_of, api_key_id: context.api_key.id],
          [as_of: @as_of, pool_id: context.pool.id],
          [before: valid],
          [as_of: @as_of, before: %{valid | id: "invalid"}],
          [as_of: @as_of, before: %{valid | timestamp: @as_of}],
          [as_of: @as_of, before: %{valid | timestamp: DateTime.add(@as_of, -3_601)}],
          [as_of: @as_of, before: Map.put(valid, :pool_id, context.pool.id)],
          [as_of: @as_of, before: "client-controlled"],
          %{as_of: @as_of}
        ] do
      {result, projections} = Support.collect_repo_queries(fn -> Observatory.read_outcomes(context.principal, opts) end)
      assert {:error, %{code: :invalid_input}} = result
      assert projections == []
    end
  end

  test "outcomes expose known identities and compact unknown user agents", context do
    for {user_agent, offset} <- Enum.with_index(["codex-tui/1.2.3 private-marker-alpha", "custom-client/1.0 private-marker-beta", nil], 1) do
      timed_request(context, DateTime.add(@as_of, -offset), %{user_agent: user_agent})
    end

    assert {:ok, report} = Observatory.read_outcomes(context.principal, as_of: @as_of)
    assert Enum.map(report.outcomes, & &1.client.kind) == ["codex", "unknown", "unknown"]
    assert Enum.map(report.outcomes, & &1.client.label) == ["Codex", "custom-client 1.0", "Unknown client"]
    assert hd(report.outcomes).client.logo == %{asset: "codex.svg", format: :svg}
    refute Enum.any?(report.outcomes, &Map.has_key?(&1, :user_agent))
    refute inspect(report) =~ "private-marker"
    assert inspect(report) =~ "custom-client 1.0"
    refute inspect(report) =~ "1.2.3"
  end

  defp principal(pool, api_key) do
    DashboardPrincipal.new(%{api_key_id: api_key.id, pool_id: pool.id, display_name: api_key.display_name, key_prefix: api_key.key_prefix})
  end

  test "display names reach outcomes and charts without merging distinct model codes", context do
    for {code, offset} <- Enum.with_index(["sample-model-a", "sample-model-b"], 1) do
      model = model_fixture(context.pool, %{exposed_model_id: code, display_name: "Sample Model"})
      request = timed_request(context, DateTime.add(@as_of, -offset), %{model_id: model.id})
      ledger_entry_fixture(request, %{total_tokens: offset * 10})
    end

    assert {:ok, report} = Observatory.read(context.principal, "1h", as_of: @as_of)
    assert Enum.map(report.outcomes, & &1.model_display_name) == ["Sample Model", "Sample Model"]
    assert Enum.map(report.models, & &1.label) == ["sample-model-b", "sample-model-a"]
    assert Enum.map(report.models, & &1.display_name) == ["Sample Model", "Sample Model"]
    assert Enum.map(report.models, & &1.total_tokens) == [20, 10]
  end

  test "unusable display names fall back to the exposed code", context do
    for {name, offset} <- Enum.with_index(["<invalid>", String.duplicate("x", 81)], 1) do
      model = model_fixture(context.pool, %{exposed_model_id: "sample-#{offset}", display_name: name})
      timed_request(context, DateTime.add(@as_of, -offset), %{model_id: model.id})
    end

    assert {:ok, report} = Observatory.read_outcomes(context.principal, as_of: @as_of)
    assert Enum.map(report.outcomes, & &1.model_display_name) == ["sample-1", "sample-2"]
  end

  test "recent outcomes classify supported routes without guessing from substrings", context do
    routes = [
      {"/backend-api/codex/models", "models"},
      {"/v1/models", "models"},
      {"/api/codex/usage", "usage"},
      {"/wham/usage", "usage"},
      {"/backend-api/wham/usage", "usage"},
      {"/v1/usage", "usage"},
      {"/v1/responses", "responses"},
      {"/backend-api/codex/responses", "responses"},
      {"/backend-api/codex/responses/compact", "compaction"},
      {"/backend-api/files", "files"},
      {"/backend-api/files/uploaded", "files"},
      {"/v1/files", "files"},
      {"/v1/files/delete", "files"},
      {"/v1/files/content", "files"},
      {"/backend-api/transcribe", "audio"},
      {"/backend-api/codex/images/generations", "other"}
    ]

    for {{endpoint, _class}, offset} <- Enum.with_index(routes, 1) do
      timed_request(context, DateTime.add(@as_of, -offset), %{endpoint: endpoint})
    end

    assert {:ok, report} = Observatory.read_outcomes(context.principal, as_of: @as_of)
    assert Enum.map(report.outcomes, & &1.endpoint_class) == Enum.map(routes, &elem(&1, 1))
    refute Enum.any?(report.outcomes, &Map.has_key?(&1, :endpoint))
  end

  defp timed_request(context, timestamp, attrs \\ %{}) do
    timestamp = %{timestamp | microsecond: {elem(timestamp.microsecond, 0), 6}}

    context
    |> request_fixture(attrs)
    |> Ecto.Changeset.change(%{admitted_at: timestamp, completed_at: timestamp})
    |> Repo.update!()
  end
end
