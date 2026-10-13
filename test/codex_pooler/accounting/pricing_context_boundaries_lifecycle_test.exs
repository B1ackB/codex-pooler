defmodule CodexPooler.Accounting.PricingContextBoundariesLifecycleTest do
  use CodexPooler.DataCase, async: false

  alias CodexPooler.Accounting
  alias CodexPooler.Accounting.{Attempt, LedgerEntry, Request, RequestLogFact}
  alias CodexPooler.Catalog.{OpenAIPricingImporter, PricingSnapshot}
  alias CodexPooler.Repo
  alias Ecto.Adapters.SQL.Sandbox

  import CodexPooler.PoolerFixtures

  setup context do
    identifier = "gpt-example-lifecycle-#{System.unique_integer([:positive, :monotonic])}"
    %{pool: pool, api_key: key} = active_api_key_fixture()
    model = model_fixture(pool, %{exposed_model_id: identifier, upstream_model_id: identifier, pricing_ref: identifier})
    %{assignment: assignment} = upstream_assignment_fixture(pool)
    now = DateTime.utc_now() |> DateTime.truncate(:microsecond)

    on_exit(fn ->
      CodexPooler.DataCase.stop_sandbox(context.sandbox_owner, context.sandbox_settings_cache)

      cleanup =
        Sandbox.unboxed_run(Repo, fn ->
          assert Repo.aggregate(from(r in Request, where: r.pool_id == ^pool.id), :count) == 0
          assert Repo.aggregate(from(p in PricingSnapshot, where: p.model_identifier == ^identifier), :count) == 0
          %{requests_remaining: 0, snapshots_remaining: 0, sandbox_owner_stopped: not Process.alive?(context.sandbox_owner)}
        end)

      record(context.test, "cleanup", cleanup)
    end)

    {:ok, identifier: identifier, model: model, assignment: assignment, auth: %{pool: pool, api_key: key, pool_id: pool.id, api_key_id: key.id, key_prefix: key.key_prefix}, clock: now}
  end

  test "completion selects V2 cutoff and rates while V1 reservation and attempt remain immutable", context do
    v1 = import_generation!(context, -300, known(272_000), 2)
    reserved = reserve!(context, -250)
    assert {:ok, attempt} = Accounting.create_attempt(reserved.request, context.assignment)
    reservation_before = Repo.get!(LedgerEntry, reserved.reservation.id)
    attempt_before = Repo.get!(Attempt, attempt.id)
    snapshots_before = snapshots(context.identifier)

    assert Decimal.equal?(reservation_before.estimated_cost_micros, Decimal.new(1_600_120))
    assert reservation_before.details["context_pricing"]["long_context_input_tokens_gt"] == 272_000
    assert reservation_before.details["context_pricing"]["band"] == "long"
    assert reservation_before.details["context_pricing"]["generation"]["price_version"] == v1.price_version
    assert reserved.request.request_metadata["pricing"]["context_pricing"] == reservation_before.details["context_pricing"]
    assert attempt_before.pricing_snapshot_id == reservation_before.pricing_snapshot_id

    v2 = import_generation!(context, -200, known(500_000), 10)
    result = finalize!(context, reserved, attempt, usage(), -150)
    settlement = Repo.get!(LedgerEntry, result.settlement.id)
    request = Repo.get!(Request, result.request.id)
    facts = settlement.details["context_pricing"]
    assert facts["long_context_input_tokens_gt"] == 500_000
    assert facts["generation"]["price_version"] == v2.price_version
    assert facts["generation"]["model_identifier"] == context.identifier
    assert facts["generation"]["service_tier"] == "standard"
    assert to_string(facts["generation"]["importer_format_revision"]) == "3"
    assert facts["price_bucket"] == "default"
    assert facts["band"] == "lower"
    assert facts["snapshot_id"] == settlement.pricing_snapshot_id
    assert facts["status"] == "known"
    assert facts["basis"] == "catalog"
    assert Decimal.equal?(settlement.settled_cost_micros, Decimal.new(3_500_300))
    assert request.request_metadata["pricing"]["context_pricing"] == facts
    assert Repo.get!(LedgerEntry, reservation_before.id) == reservation_before
    assert Repo.get!(Attempt, attempt.id).pricing_snapshot_id == attempt_before.pricing_snapshot_id
    assert Enum.all?(snapshots_before, &(Repo.get!(PricingSnapshot, &1.id) == &1))

    v3 = import_generation!(context, -100, known(100_000), 100)
    assert {:ok, duplicate} = Accounting.finalize_success(reserved.request, attempt, usage(), %{now: context.clock, response_status_code: 200})
    assert duplicate.settlement.id == settlement.id
    assert Repo.get!(LedgerEntry, settlement.id) == settlement
    assert Repo.get!(Request, request.id) == request
    assert Repo.get!(LedgerEntry, reservation_before.id) == reservation_before
    assert Repo.aggregate(from(e in LedgerEntry, where: e.request_id == ^request.id), :count) == 3
    assert Enum.all?(snapshots_before, &(Repo.get!(PricingSnapshot, &1.id) == &1))

    record(context.test, "facts", %{versions: [v1.price_version, v2.price_version, v3.price_version], reservation: ledger_fact(reservation_before), settlement: ledger_fact(settlement), request: request_fact(request), attempt_snapshot_id: attempt_before.pricing_snapshot_id, reservation_unchanged: true, historical_snapshots_unchanged: true, duplicate_settlement_count: 1, imported_snapshots: Enum.map(snapshots(context.identifier), &snapshot_fact/1)})
  end

  test "unknown boundary with known usage succeeds with unavailable cost and bounded generation", context do
    imported = import_generation!(context, -300, unknown(), 2)
    reserved = reserve!(context, -250)
    assert reserved.pricing_snapshot == nil
    assert reserved.estimate.estimated_cost_micros == nil
    assert reserved.reservation.details["context_pricing"]["reason"] == "missing_source"
    assert {:ok, attempt} = Accounting.create_attempt(reserved.request, context.assignment)
    result = finalize!(context, reserved, attempt, usage(), -150)
    assert_unavailable(result, "unknown", imported.price_version)
    assert result.settlement.usage_status == "usage_known"
    assert result.settlement.input_tokens == 400_000
    assert result.settlement.details["estimated_from_reserve"] == false
    assert result.settlement.details["context_pricing"]["reason"] == "missing_source"
    assert Map.keys(result.settlement.details["context_pricing"]) |> Enum.sort() == ~w(band basis generation price_bucket reason snapshot_id status)
    record(context.test, "facts", %{reservation: ledger_fact(Repo.reload!(reserved.reservation)), settlement: ledger_fact(Repo.reload!(result.settlement)), request: request_fact(Repo.reload!(result.request))})
  end

  test "invalid persisted rule fails pricing closed without failing completed runtime usage", context do
    imported = import_generation!(context, -300, known(500_000), 2)
    Repo.update_all(from(p in PricingSnapshot, where: p.model_identifier == ^context.identifier, update: [set: [config: fragment("jsonb_set(?, '{context_pricing}', 'null'::jsonb)", p.config)]]), [])
    reserved = reserve!(context, -250)
    assert {:ok, attempt} = Accounting.create_attempt(reserved.request, context.assignment)
    result = finalize!(context, reserved, attempt, usage(), -150)
    assert_unavailable(result, "invalid", imported.price_version)
    assert result.settlement.details["context_pricing"]["reason"] == "invalid_rule"
    refute Map.has_key?(result.settlement.details["context_pricing"], "long_context_input_tokens_gt")
    record(context.test, "facts", %{settlement: ledger_fact(Repo.reload!(result.settlement)), request: request_fact(Repo.reload!(result.request))})
  end

  test "unknown usage retains its separate class and reservation estimate after a pricing update", context do
    import_generation!(context, -300, known(272_000), 2)
    reserved = reserve!(context, -250)
    assert {:ok, attempt} = Accounting.create_attempt(reserved.request, context.assignment)
    v2 = import_generation!(context, -200, unknown(), 10)
    result = finalize!(context, reserved, attempt, %{status: "usage_unknown"}, -150)
    assert_unavailable(result, "unknown", v2.price_version)
    assert result.request.usage_status == "usage_unknown"
    assert result.settlement.usage_status == "usage_unknown"
    assert result.settlement.details["estimated_from_reserve"] == true
    assert result.settlement.total_tokens == reserved.reservation.total_tokens
    assert Decimal.equal?(result.settlement.estimated_cost_micros, reserved.reservation.estimated_cost_micros)
    record(context.test, "facts", %{reservation: ledger_fact(Repo.reload!(reserved.reservation)), settlement: ledger_fact(Repo.reload!(result.settlement)), request: request_fact(Repo.reload!(result.request))})
  end

  test "retry preserves the first attempt reference and records one final coherent settlement", context do
    v1 = import_generation!(context, -300, known(272_000), 2)
    reserved = reserve!(context, -250)
    assert {:ok, first} = Accounting.create_attempt(reserved.request, context.assignment)
    assert {:ok, failed} = Accounting.record_retryable_attempt_failure(first, %{response_status_code: 502, last_error_code: "upstream_502"})
    v2 = import_generation!(context, -200, known(500_000), 10)
    assert {:ok, retry} = Accounting.create_attempt(Repo.reload!(reserved.request), context.assignment)
    result = finalize!(context, reserved, retry, usage(), -150, %{retry_count: 1})
    assert Repo.get!(Attempt, first.id) == failed
    assert result.request.retry_count == 1
    assert result.settlement.attempt_id == retry.id
    assert Repo.get!(PricingSnapshot, first.pricing_snapshot_id).price_version == v1.price_version
    assert Repo.get!(PricingSnapshot, retry.pricing_snapshot_id).price_version == v2.price_version
    assert result.settlement.details["context_pricing"]["generation"]["price_version"] == v2.price_version
    assert result.settlement.pricing_snapshot_id != retry.pricing_snapshot_id
    assert Decimal.equal?(result.settlement.settled_cost_micros, Decimal.new(3_500_300))
    assert Repo.aggregate(from(e in LedgerEntry, where: e.request_id == ^reserved.request.id and e.entry_kind == "settlement"), :count) == 1
    record(context.test, "facts", %{reservation: ledger_fact(Repo.reload!(reserved.reservation)), settlement: ledger_fact(Repo.reload!(result.settlement)), attempt_snapshot_ids: [first.pricing_snapshot_id, retry.pricing_snapshot_id], retry_count: 1, settlement_count: 1})
  end

  test "legacy import records its assumption while historical absent metadata remains absent", context do
    import_generation!(context, -300, :legacy, 2)
    reserved = reserve!(context, -250)
    assert {:ok, attempt} = Accounting.create_attempt(reserved.request, context.assignment)
    result = finalize!(context, reserved, attempt, usage(), -150)
    facts = result.settlement.details["context_pricing"]
    assert facts["status"] == "legacy"
    assert facts["basis"] == "legacy_assumption"
    assert facts["long_context_input_tokens_gt"] == 272_000
    assert facts["band"] == "long"

    # Model a pre-feature row by removing only the new facts after a real import and settlement.
    historical_request = Repo.update!(change(result.request, request_metadata: update_in(result.request.request_metadata, ["pricing"], &Map.delete(&1, "context_pricing"))))
    historical_settlement = Repo.update!(change(result.settlement, details: Map.delete(result.settlement.details, "context_pricing")))
    import_generation!(context, -100, known(500_000), 100)
    assert {:ok, duplicate} = Accounting.finalize_success(historical_request, attempt, usage(), %{now: context.clock, response_status_code: 200})
    assert duplicate.request == historical_request
    assert Repo.get!(LedgerEntry, historical_settlement.id) == historical_settlement
    refute Map.has_key?(Repo.reload!(historical_request).request_metadata["pricing"], "context_pricing")
    record(context.test, "facts", %{legacy_facts: facts, historical_request: request_fact(Repo.reload!(historical_request)), historical_settlement: ledger_fact(Repo.reload!(historical_settlement)), historical_metadata_absent: true})
  end

  test "uniform pricing records no numerical cutoff or source evidence text", context do
    rule = %{"status" => "uniform", "provenance" => Map.put(provenance(), "locator", "synthetic-source-evidence-not-for-request-metadata")}
    imported = import_generation!(context, -300, rule, 2, omit_long: true)
    reserved = reserve!(context, -250)
    assert {:ok, attempt} = Accounting.create_attempt(reserved.request, context.assignment)
    result = finalize!(context, reserved, attempt, usage(), -150)
    facts = Repo.reload!(result.settlement).details["context_pricing"]
    assert facts["status"] == "uniform"
    assert facts["basis"] == "catalog"
    assert facts["band"] == "uniform"
    assert facts["generation"]["price_version"] == imported.price_version
    assert Map.keys(facts) |> Enum.sort() == ~w(band basis generation price_bucket snapshot_id status)
    assert Map.keys(facts["generation"]) |> Enum.sort() == ~w(importer_format_revision model_identifier price_version service_tier)
    assert Decimal.equal?(result.settlement.settled_cost_micros, Decimal.new(700_060))
    record(context.test, "facts", %{settlement: ledger_fact(Repo.reload!(result.settlement)), request: request_fact(Repo.reload!(result.request))})
  end

  test "missing long price preserves physical fallback and semantic long band in the same generation", context do
    imported = import_generation!(context, -300, known(272_000), 2, omit_long: true)
    reserved = reserve!(context, -250)
    assert {:ok, attempt} = Accounting.create_attempt(reserved.request, context.assignment)
    result = finalize!(context, reserved, attempt, usage(), -150)
    facts = Repo.reload!(result.settlement).details["context_pricing"]
    assert facts["generation"]["price_version"] == imported.price_version
    assert facts["price_bucket"] == "default"
    assert facts["band"] == "long"
    assert result.settlement.details["price_bucket_fallback"] == %{"requested" => "long_context", "selected" => "default", "reason" => "long_context_pricing_absent"}
    assert result.request.request_metadata["pricing"]["context_pricing"] == facts
    assert Decimal.equal?(result.settlement.settled_cost_micros, Decimal.new(700_060))
    record(context.test, "facts", %{settlement: ledger_fact(Repo.reload!(result.settlement)), request: request_fact(Repo.reload!(result.request))})
  end

  defp reserve!(context, offset) do
    assert {:ok, reserved} = Accounting.reserve(context.auth, context.model, %{"model" => context.identifier}, %{now: DateTime.add(context.clock, offset, :second), reservation_estimate: %{input_tokens: 400_000, output_tokens: 10}})
    reserved
  end

  defp finalize!(context, reserved, attempt, usage, offset, opts \\ %{}) do
    assert {:ok, result} = Accounting.finalize_success(reserved.request, attempt, usage, Map.merge(%{now: DateTime.add(context.clock, offset, :second), response_status_code: 200}, opts))
    result
  end

  defp assert_unavailable(result, status, version) do
    assert result.request.status == "succeeded"
    assert result.request.response_status_code == 200
    assert result.request.last_error_code == nil
    assert result.settlement.details["pricing_status"] == "unpriced_context_boundary_#{status}"
    assert result.settlement.details["settled_cost_micros"] == nil
    assert Decimal.equal?(result.settlement.settled_cost_micros, Decimal.new(0))
    assert result.settlement.pricing_snapshot_id == nil
    assert result.settlement.details["context_pricing"]["generation"]["price_version"] == version
    assert result.settlement.details["context_pricing"]["basis"] == "unavailable"
    assert result.request.request_metadata["pricing"]["context_pricing"] == result.settlement.details["context_pricing"]
    assert Repo.get!(RequestLogFact, result.request.id).latest_settled_cost_micros == nil
  end

  defp usage, do: %{status: "usage_known", input_tokens: 400_000, cached_input_tokens: 100_000, cache_write_tokens: 50_000, output_tokens: 10, total_tokens: 400_010}
  defp known(cutoff), do: %{"status" => "known", "long_context_input_tokens_gt" => cutoff, "provenance" => provenance()}
  defp unknown, do: %{"status" => "unknown", "reason" => "missing_source", "observed_rules" => [], "provenance" => provenance()}
  defp provenance, do: %{"locator" => "synthetic/context-note", "evidence_sha256" => String.duplicate("a", 64)}
  defp rates(input), do: %{"input" => input, "cached_input" => input / 4, "cache_write" => input * 1.5, "output" => input * 3}
  defp snapshots(identifier), do: Repo.all(from(p in PricingSnapshot, where: p.model_identifier == ^identifier, order_by: p.id))

  defp import_generation!(context, offset, rule, rate, opts \\ []) do
    generated_at = context.clock |> DateTime.add(offset, :second) |> DateTime.to_iso8601()
    path = Path.join(System.tmp_dir!(), "pricing-lifecycle-#{Ecto.UUID.generate()}.json")

    on_exit(fn ->
      File.rm!(path)
      refute File.exists?(path)
      record(context.test, "file-#{offset}", %{path: path, removed: true})
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

  defp ledger_fact(entry), do: %{id: entry.id, request_id: entry.request_id, attempt_id: entry.attempt_id, pricing_snapshot_id: entry.pricing_snapshot_id, entry_kind: entry.entry_kind, usage_status: entry.usage_status, input_tokens: entry.input_tokens, total_tokens: entry.total_tokens, estimated_cost_micros: Decimal.to_string(entry.estimated_cost_micros), settled_cost_micros: Decimal.to_string(entry.settled_cost_micros), details: entry.details, occurred_at: entry.occurred_at, created_at: entry.created_at}
  defp request_fact(request), do: %{id: request.id, status: request.status, usage_status: request.usage_status, retry_count: request.retry_count, pricing: request.request_metadata["pricing"], reservation: request.request_metadata["reservation"], admitted_at: request.admitted_at, completed_at: request.completed_at}
  defp snapshot_fact(snapshot), do: %{id: snapshot.id, price_version: snapshot.price_version, config: snapshot.config, effective_at: snapshot.effective_at, captured_at: snapshot.captured_at, input_token_micros: Decimal.to_string(snapshot.input_token_micros), cached_input_token_micros: Decimal.to_string(snapshot.cached_input_token_micros), cache_write_token_micros: Decimal.to_string(snapshot.cache_write_token_micros), output_token_micros: Decimal.to_string(snapshot.output_token_micros)}

  defp record(test, phase, facts) do
    if directory = System.get_env("PRICING_LIFECYCLE_RECEIPTS") do
      slug = test |> Atom.to_string() |> String.replace(~r/[^a-zA-Z0-9]+/, "-")
      File.write!(Path.join(directory, "#{slug}-#{phase}.json"), CodexPooler.JSON.encode!(facts))
    end
  end
end
