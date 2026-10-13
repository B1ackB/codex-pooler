defmodule CodexPoolerWeb.Admin.RequestLogsContextPricingLiveTest do
  use CodexPoolerWeb.ConnCase, async: false

  import Ecto.Query
  import Phoenix.LiveViewTest
  import CodexPooler.PoolerFixtures

  alias CodexPooler.Accounting
  alias CodexPooler.Accounting.RequestLogs
  alias CodexPooler.Catalog.{OpenAIPricingImporter, PricingSnapshot}
  alias CodexPooler.Pools
  alias CodexPooler.Repo

  # The live page starts bounded asynchronous queries; completion, not elapsed
  # time, ends this wait even when the shared test machine is busy.
  @detection_timeout_ms 15_000
  @explanation "Reporting estimate from published API prices; not a subscription charge."
  @source_marker "synthetic-source-text-must-not-render"

  setup :register_and_log_in_user

  setup %{scope: scope} do
    identifier = "gpt-example-display-#{System.unique_integer([:positive, :monotonic])}"
    {:ok, pool} = Pools.create_pool(scope, %{slug: identifier, name: "Pricing display"})
    %{api_key: key} = active_api_key_fixture(pool)
    model = model_fixture(pool, %{exposed_model_id: identifier, upstream_model_id: identifier, pricing_ref: identifier})
    %{assignment: assignment} = upstream_assignment_fixture(pool)
    auth = %{pool: pool, api_key: key, pool_id: pool.id, api_key_id: key.id, key_prefix: key.key_prefix}
    {:ok, identifier: identifier, pool: pool, model: model, assignment: assignment, auth: auth, clock: DateTime.utc_now()}
  end

  test "drawer shows the final imported cutoff and version after later imports without changing the estimate", context do
    v1 = import_generation!(context, -300, known(272_000), 2)
    reserved = reserve!(context)
    assert {:ok, attempt} = Accounting.create_attempt(reserved.request, context.assignment)
    v2 = import_generation!(context, -200, known(500_000), 10)
    result = finalize!(context, reserved, attempt)
    import_generation!(context, -100, known(100_000), 100)

    assert reserved.reservation.details["context_pricing"]["generation"]["price_version"] == v1.price_version
    assert result.settlement.details["context_pricing"]["generation"]["price_version"] == v2.price_version
    assert Decimal.equal?(result.settlement.settled_cost_micros, 4_000_300)
    # A stale request-side estimate must not override the final ledger facts.
    Repo.update!(Ecto.Changeset.change(result.request, request_metadata: Map.put(result.request.request_metadata, "pricing", reserved.request.request_metadata["pricing"])))
    view = open_request(context, result.request)
    assert has_element?(view, "#request-log-detail-billing-cutoff", "500000 input tokens; long context above cutoff")
    assert has_element?(view, "#request-log-detail-pricing-rule", "Catalog · known")
    assert has_element?(view, "#request-log-detail-pricing-version", v2.price_version)
    refute has_element?(view, "#request-log-detail-pricing-version", v1.price_version)
    assert has_element?(view, "#request-log-detail-cost", "$4.00")
    assert_details(context, result, view)
    assert Repo.reload!(reserved.reservation) == reserved.reservation
    assert Repo.reload!(result.settlement) == result.settlement
  end

  test "legacy catalog assumption and existing physical bucket fallback remain visible", context do
    imported = import_generation!(context, -300, :legacy, 2, omit_long: true)
    result = complete!(context)
    view = open_request(context, result.request)
    assert has_element?(view, "#request-log-detail-billing-cutoff", "272000 input tokens; long context above cutoff")
    assert has_element?(view, "#request-log-detail-pricing-rule", "Legacy assumption · legacy")
    assert has_element?(view, "#request-log-detail-pricing-version", imported.price_version)
    assert has_element?(view, "#request-log-detail-price-bucket", "default (long_context requested)")
    assert_details(context, result, view)
  end

  test "uniform pricing has a recorded version and no invented numerical cutoff", context do
    import_generation!(context, -300, %{"status" => "uniform", "provenance" => provenance()}, 2, omit_long: true)
    result = complete!(context)
    view = open_request(context, result.request)
    assert has_element?(view, "#request-log-detail-billing-cutoff", "No cutoff (uniform pricing)")
    assert has_element?(view, "#request-log-detail-pricing-rule", "Catalog · uniform")
    assert_details(context, result, view)
  end

  test "unknown pricing keeps successful measured usage and unavailable cost instead of the zero ledger sentinel", context do
    import_generation!(context, -300, %{"status" => "unknown", "reason" => "missing_source", "observed_rules" => [], "provenance" => provenance()}, 2)
    result = complete!(context)
    assert_unavailable(context, result, "unknown", "missing_source")
  end

  test "invalid stored rule keeps its generation and unavailable cost", context do
    import_generation!(context, -300, known(500_000), 2)
    Repo.update_all(from(p in PricingSnapshot, where: p.model_identifier == ^context.identifier, update: [set: [config: fragment("jsonb_set(?, '{context_pricing}', 'null'::jsonb)", p.config)]]), [])
    result = complete!(context)
    assert_unavailable(context, result, "invalid", "invalid_rule")
  end

  test "historical missing facts remain absent after later imports and never resolve the current catalog", context do
    import_generation!(context, -300, :legacy, 2)
    result = complete!(context)
    request = Repo.update!(Ecto.Changeset.change(result.request, request_metadata: update_in(result.request.request_metadata, ["pricing"], &Map.delete(&1, "context_pricing"))))
    settlement = Repo.update!(Ecto.Changeset.change(result.settlement, details: Map.delete(result.settlement.details, "context_pricing")))
    import_generation!(context, -100, known(500_000), 100)
    projected = read_with_query_receipt(context, request)
    assert Map.get(projected, :context_pricing) == nil
    view = open_request(context, request)
    for suffix <- ~w(billing-cutoff pricing-rule pricing-version), do: refute(has_element?(view, "#request-log-detail-#{suffix}"))
    assert has_element?(view, "#request-log-detail-cost", "$1.60")
    assert Repo.reload!(request) == request
    assert Repo.reload!(settlement) == settlement
    record(context.test, %{historical_absence: true, settlement_unchanged: true, context_pricing: nil, cost: projected.cost})
  end

  test "genuine zero API rates render zero while retaining the reporting basis", context do
    import_generation!(context, -300, known(500_000), 0)
    result = complete!(context)
    view = open_request(context, result.request)
    assert has_element?(view, "#request-log-detail-cost", "$0.00")
    assert_details(context, result, view)
  end

  test "bounded projection rejects malformed metadata and excludes source text", context do
    import_generation!(context, -300, known(500_000), 2)
    result = complete!(context)
    facts = result.settlement.details["context_pricing"]

    malformed =
      facts
      |> Map.merge(%{"reason" => @source_marker, "provenance" => %{"locator" => @source_marker}, "long_context_input_tokens_gt" => "500000", "price_bucket" => %{}, "band" => [], "snapshot_id" => false})
      |> put_in(["generation", "price_version"], "<script>#{@source_marker}</script>")
      |> put_in(["generation", "importer_format_revision"], true)
      |> put_in(["generation", "service_tier"], @source_marker)

    Repo.update!(Ecto.Changeset.change(result.settlement, details: Map.put(result.settlement.details, "context_pricing", malformed)))
    view = open_request(context, result.request)
    refute render(view) =~ @source_marker
    refute has_element?(view, "#request-log-detail-pricing-version", "<script>")
    assert has_element?(view, "#request-log-detail-pricing-rule", "Catalog · known")
    projected = RequestLogs.get_for_scope(context.scope, result.request.id, surface: :admin)
    assert projected.context_pricing.generation.price_version == nil
    refute Map.has_key?(projected.context_pricing, :provenance)
    assert projected.context_pricing.reason == nil
    assert projected.context_pricing.long_context_input_tokens_gt == nil
    assert projected.context_pricing.price_bucket == nil
    assert projected.context_pricing.band == nil
    assert projected.context_pricing.snapshot_id == nil
    assert projected.context_pricing.generation.importer_format_revision == nil
    assert projected.context_pricing.generation.service_tier == nil
    record(context.test, %{bounded_projection: projected.context_pricing, sensitive_marker_absent: true})
  end

  defp assert_unavailable(context, result, status, reason) do
    assert result.request.status == "succeeded"
    assert result.settlement.usage_status == "usage_known"
    assert Decimal.equal?(result.settlement.settled_cost_micros, 0)
    assert result.settlement.details["settled_cost_micros"] == nil
    view = open_request(context, result.request)
    assert has_element?(view, "#request-log-detail-cost", "cost n/a")
    refute has_element?(view, "#request-log-detail-cost", "$0")
    assert has_element?(view, "#request-log-detail-billing-cutoff", "Unavailable")
    assert has_element?(view, "#request-log-detail-pricing-rule", "Unavailable · #{status} · #{reason}")
    assert_details(context, result, view)
  end

  defp assert_details(context, result, view) do
    facts = result.settlement.details["context_pricing"]
    assert has_element?(view, "#request-log-detail-pricing-version", facts["generation"]["price_version"])
    assert has_element?(view, "#request-log-detail-pricing-version", context.identifier)
    assert has_element?(view, "#request-log-detail-pricing-explanation", @explanation)
    refute render(view) =~ @source_marker
    projected = read_with_query_receipt(context, result.request)
    assert projected.context_pricing.status == facts["status"]
    assert projected.context_pricing.basis == facts["basis"]
    assert projected.context_pricing.long_context_input_tokens_gt == facts["long_context_input_tokens_gt"]
    assert projected.context_pricing.reason == facts["reason"]
    assert projected.context_pricing.price_bucket == facts["price_bucket"]
    assert projected.context_pricing.band == facts["band"]
    assert projected.context_pricing.snapshot_id == facts["snapshot_id"]
    assert projected.context_pricing.generation.price_version == facts["generation"]["price_version"]
    selectors = ~w(billing-cutoff pricing-rule pricing-version pricing-explanation cost)

    rows =
      Map.new(selectors, fn suffix ->
        selector = "#request-log-detail-#{suffix}"
        html = view |> element(selector) |> render()
        {selector, %{html: html, text: html |> LazyHTML.from_fragment() |> LazyHTML.text()}}
      end)

    record(context.test, %{request_id: result.request.id, settlement_id: result.settlement.id, persisted: facts, projected: projected.context_pricing, cost: projected.cost, rows: rows})
  end

  defp read_with_query_receipt(context, request) do
    handler = "pricing-display-#{Ecto.UUID.generate()}"
    on_exit(fn -> :telemetry.detach(handler) end)
    :ok = :telemetry.attach(handler, [:codex_pooler, :repo, :query], &__MODULE__.capture_query/4, self())
    projected = RequestLogs.get_for_scope(context.scope, request.id, surface: :admin)
    :telemetry.detach(handler)
    queries = collect_queries([])
    assert queries != []
    snapshot_queries = Enum.filter(queries, &String.contains?(&1.query, "pricing_snapshots"))
    # Existing explicit historical snapshot lookups remain legal. Current
    # catalog resolution by model/ranking must never reconstruct these facts.
    assert Enum.all?(snapshot_queries, fn query -> not Enum.member?(query.params, context.identifier) and String.contains?(query.query, "\"id\" =") end)
    record(context.test, %{query_count: length(queries), explicit_snapshot_queries: length(snapshot_queries), current_catalog_resolution: false}, "queries")
    projected
  end

  @doc false
  def capture_query(_event, _measurements, metadata, owner) do
    if self() == owner, do: send(owner, {:pricing_display_query, %{query: metadata.query, params: metadata.params}})
  end

  defp collect_queries(queries) do
    receive do
      {:pricing_display_query, query} -> collect_queries([query | queries])
    after
      0 -> queries
    end
  end

  defp open_request(context, request) do
    {:ok, view, _html} = live(context.conn, ~p"/admin/request-logs?pool_id=#{context.pool.id}&selected_request_id=#{request.id}")
    await_request_logs(view, System.monotonic_time(:millisecond) + @detection_timeout_ms)
    assert has_element?(view, "#request-log-detail-request-id", request.id)
    on_exit(fn -> if Process.alive?(view.pid), do: GenServer.stop(view.pid) end)
    view
  end

  defp await_request_logs(view, deadline) do
    render_async(view, 5_000)
    state = :sys.get_state(view.pid)

    if state.socket.assigns.request_logs_loading? or state.socket.assigns.request_logs_running? do
      if System.monotonic_time(:millisecond) >= deadline, do: flunk("request detail did not finish loading")

      receive do
      after
        1 -> await_request_logs(view, deadline)
      end
    end
  end

  defp reserve!(context) do
    assert {:ok, reserved} = Accounting.reserve(context.auth, context.model, %{"model" => context.identifier}, %{now: DateTime.add(context.clock, -250, :second), transport: "http_sse", reservation_estimate: %{input_tokens: 400_000, output_tokens: 10}})
    reserved
  end

  defp finalize!(context, reserved, attempt) do
    assert {:ok, result} = Accounting.finalize_success(reserved.request, attempt, %{status: "usage_known", input_tokens: 400_000, output_tokens: 10, total_tokens: 400_010}, %{now: DateTime.add(context.clock, -150, :second), response_status_code: 200})
    result
  end

  defp complete!(context) do
    reserved = reserve!(context)
    assert {:ok, attempt} = Accounting.create_attempt(reserved.request, context.assignment)
    finalize!(context, reserved, attempt)
  end

  defp known(cutoff), do: %{"status" => "known", "long_context_input_tokens_gt" => cutoff, "provenance" => provenance()}
  defp provenance, do: %{"locator" => @source_marker, "evidence_sha256" => String.duplicate("a", 64)}
  defp rates(input), do: %{"input" => input, "cached_input" => input / 4, "output" => input * 3}

  defp import_generation!(context, offset, rule, rate, opts \\ []) do
    generated_at = context.clock |> DateTime.add(offset, :second) |> DateTime.to_iso8601()
    path = Path.join(System.tmp_dir!(), "pricing-display-#{Ecto.UUID.generate()}.json")

    on_exit(fn ->
      File.rm!(path)
      refute File.exists?(path)
      record(context.test, %{catalog_removed: true}, "cleanup-#{offset}")
    end)

    model = %{"model" => context.identifier, "category" => "language_model", "categories" => ["language_model"], "pricing_type" => "per_1m_tokens", "pricing_types" => ["per_1m_tokens"], "prices" => %{"standard" => %{"default" => rates(rate), "long_context" => rates(rate * 2)}}}
    model = if opts[:omit_long], do: update_in(model, ["prices", "standard"], &Map.delete(&1, "long_context")), else: model
    model = if rule == :legacy, do: model, else: Map.put(model, "context_pricing", %{"version" => 1, "tiers" => %{"standard" => rule}})
    payload = %{"generated_at" => generated_at, "source" => "synthetic", "source_url" => "https://example.com/pricing", "models" => %{context.identifier => model}, "models_count" => 1, "tools" => %{"sample-tool" => %{"details" => "Synthetic tool", "price" => 0, "pricing" => "$0", "tool" => "Sample Tool"}}, "tools_count" => 1}
    payload = if rule == :legacy, do: payload, else: Map.put(payload, "schema_version", 2)
    File.write!(path, CodexPooler.JSON.encode!(payload))
    assert {:ok, imported} = OpenAIPricingImporter.import_file(path)
    imported
  end

  defp record(test, facts, phase \\ "render") do
    if directory = System.get_env("PRICING_UI_RECEIPTS") do
      slug = test |> Atom.to_string() |> String.replace(~r/[^a-zA-Z0-9]+/, "-")
      File.write!(Path.join(directory, "#{slug}-#{phase}.json"), CodexPooler.JSON.encode!(facts))
    end
  end
end
