defmodule CodexPooler.Accounting.PricingContextBoundariesPeerTest do
  use CodexPooler.DataCase, async: false

  alias CodexPooler.Accounting
  alias CodexPooler.Accounting.{LedgerEntry, PricingResolution}
  alias CodexPooler.Catalog.{Model, OpenAIPricingImporter, PricingSnapshot}
  alias CodexPooler.{InstancePresencePeer, PeerRegistry, UnboxedFixture}
  alias Ecto.Adapters.SQL.Sandbox

  @budget 15_000
  @v1 "2020-01-01T00:00:00Z"
  @v2 "2021-01-01T00:00:00Z"

  setup do
    run_id = Ecto.UUID.generate()
    model = "gpt-example-peer-#{run_id}"
    scratch = Path.join(System.tmp_dir!(), "pricing-boundary-peer-#{run_id}")
    name = PeerRegistry.unique_node_name("pricing_boundary_peer")
    {:ok, ownership} = Agent.start(fn -> %{peer: nil, fixture: nil, os_identity: nil} end)

    # Registered before files, committed rows or the peer exist. This state is
    # deliberately not test-supervised: it outlives a failed test process.
    on_exit(fn ->
      state = Agent.get(ownership, & &1)
      stop_peer!(state, name)

      UnboxedFixture.run_unboxed(fn ->
        InstancePresencePeer.purge_peer_state!(run_id, fn ->
          if state.fixture do
            CodexPooler.PoolerFixtures.delete_committed_pools!([state.fixture.pool.id])
            Repo.delete!(Repo.get!(CodexPooler.Upstreams.Schemas.UpstreamIdentity, state.fixture.identity.id))
            refute Repo.get(CodexPooler.Pools.Pool, state.fixture.pool.id)
          end

          Repo.delete_all(from p in PricingSnapshot, where: p.model_identifier in ^[model, "a-" <> model])
          refute Repo.exists?(from p in PricingSnapshot, where: p.model_identifier in ^[model, "a-" <> model])
        end)
      end)

      File.rm_rf!(scratch)
      refute File.exists?(scratch)
      Agent.stop(ownership)
      receipt(%{phase: "cleanup", run_id: run_id, peer_absent: true, backend_absent: true, rows_absent: true, scratch_absent: true})
    end)

    File.mkdir!(scratch)
    File.chmod!(scratch, 0o700)
    %{run_id: run_id, model: model, scratch: scratch, name: name, ownership: ownership, imports: start_supervised!(Task.Supervisor)}
  end

  @tag slow: "starts an independent BEAM Repo and commits pricing generations"
  test "peer readers see whole generations and immutable committed settlement history", ctx do
    peer = start_peer!(ctx)
    fixture = committed_fixture!(ctx)
    v1 = catalog_file!(ctx, @v1, 272_000, 2)
    v2 = catalog_file!(ctx, @v2, 500_000, 7)
    assert {:ok, %{inserted: 2}} = UnboxedFixture.run_unboxed(fn -> OpenAIPricingImporter.import_file(v1) end)

    initial = remote_lookup(peer, ctx.model)
    assert_coherent!(initial, @v1, 272_000, "long_context", 4)
    history = UnboxedFixture.run_unboxed(fn -> settle!(fixture) end)
    assert Decimal.equal?(history.settled_cost_micros, Decimal.new(1_600_120))
    assert :erpc.call(peer.remote, Repo, :get!, [LedgerEntry, history.id], @budget) == history
    previous_rows = UnboxedFixture.run_unboxed(fn -> snapshots(ctx.model) end)

    writer = held_import!(ctx, v2)
    assert_receive {:import_held, writer_pid, writer_backend, %{inserted: 1}}, @budget
    assert writer_pid == writer.pid
    assert writer_backend != peer.backend
    if System.get_env("PRICING_PEER_FAILURE") == "held_assertion", do: flunk("synthetic assertion failure while import is held")
    before_commit = remote_lookup(peer, ctx.model)
    assert_coherent!(before_commit, @v1, 272_000, "long_context", 4)

    send(writer.pid, :commit)
    assert {:ok, %{inserted: 2}} = await_import!(writer)
    after_commit = remote_lookup(peer, ctx.model)
    assert_coherent!(after_commit, @v2, 500_000, "default", 7)
    local = UnboxedFixture.run_unboxed(fn -> lookup(ctx.model) end)
    assert local == after_commit
    current_settlement = UnboxedFixture.run_unboxed(fn -> settle!(fixture) end)
    assert Decimal.equal?(current_settlement.settled_cost_micros, Decimal.new(2_800_210))
    assert :erpc.call(peer.remote, Repo, :get!, [LedgerEntry, current_settlement.id], @budget) == current_settlement
    assert :erpc.call(peer.remote, Repo, :get!, [LedgerEntry, history.id], @budget) == history
    assert UnboxedFixture.run_unboxed(fn -> Enum.filter(snapshots(ctx.model), &(&1.price_version == version(@v1))) end) == previous_rows

    current_rows = UnboxedFixture.run_unboxed(fn -> snapshots(ctx.model) end)
    conflict = catalog_file!(ctx, @v2, 600_000, 7, true)
    conflicting_writer = held_import!(ctx, conflict)
    assert_receive {:import_held, _, _, %{inserted: 1}}, @budget
    assert remote_lookup(peer, ctx.model) == after_commit
    send(conflicting_writer.pid, :commit)
    assert {:error, %{code: :concurrent_pricing_conflict}} = await_import!(conflicting_writer)
    assert UnboxedFixture.run_unboxed(fn -> snapshots(ctx.model) end) == current_rows
    assert UnboxedFixture.run_unboxed(fn -> snapshots("a-" <> ctx.model) end) == []
    assert remote_lookup(peer, ctx.model) == after_commit
    assert :erpc.call(peer.remote, Repo, :get!, [LedgerEntry, history.id], @budget) == history

    receipt(%{phase: "atomic_generations", node_a: to_string(node()), node_b: to_string(peer.remote), backend_a: writer_backend, backend_b: peer.backend, before: observation(initial), during: observation(before_commit), after: observation(after_commit), settlement_id: history.id, settled_cost_micros: Decimal.to_string(history.settled_cost_micros), new_settlement_id: current_settlement.id, new_settled_cost_micros: Decimal.to_string(current_settlement.settled_cost_micros), historical_snapshot_ids: Enum.map(previous_rows, & &1.id), historical_unchanged: true, conflict_rollback: true})
  end

  @tag slow: "causal split-query control on an independent BEAM reader"
  test "coherence assertion rejects a commit between separate boundary and rate reads", ctx do
    peer = start_peer!(ctx)
    v1 = catalog_file!(ctx, @v1, 272_000, 2)
    v2 = catalog_file!(ctx, @v2, 500_000, 7)
    assert {:ok, _} = UnboxedFixture.run_unboxed(fn -> OpenAIPricingImporter.import_file(v1) end)
    boundary_read = remote_lookup(peer, ctx.model)
    assert_coherent!(boundary_read, @v1, 272_000, "long_context", 4)
    writer = held_import!(ctx, v2)
    assert_receive {:import_held, _, _, %{inserted: 1}}, @budget
    send(writer.pid, :commit)
    assert {:ok, %{inserted: 2}} = await_import!(writer)

    # Deliberately reproduce the old two-statement algorithm: retain the V1
    # classification, then query the newest rate in that bucket after V2 commits.
    %{rows: [[snapshot_id]]} = :erpc.call(peer.remote, Repo, :query!, ["SELECT id::text FROM pricing_snapshots WHERE model_identifier = $1 AND config->>'price_bucket' = $2 ORDER BY effective_at DESC, captured_at DESC, id DESC LIMIT 1", [ctx.model, boundary_read.price_bucket]], @budget)
    newer_rate = :erpc.call(peer.remote, Repo, :get!, [PricingSnapshot, snapshot_id], @budget)
    assert newer_rate.price_version == version(@v2)
    mixed = %{boundary_read | snapshot: newer_rate}
    receipt(%{phase: "split_query_observed", boundary_version: version(@v1), rate_version: newer_rate.price_version, boundary_cutoff: 272_000, rate_cutoff: newer_rate.config["context_pricing"]["long_context_input_tokens_gt"], rate: Decimal.to_string(newer_rate.input_token_micros)})
    if System.get_env("PRICING_PEER_SPLIT_CONTROL") == "1", do: assert_coherent!(mixed, @v1, 272_000, "long_context", 4)
    error = assert_raise ExUnit.AssertionError, fn -> assert_coherent!(mixed, @v1, 272_000, "long_context", 4) end
    assert error.message =~ "mixed pricing generation"
    assert_coherent!(remote_lookup(peer, ctx.model), @v2, 500_000, "default", 7)
    receipt(%{phase: "split_query_control", startup: "ok", imports: "ok", mutation: "retain V1 boundary; commit V2; read newest rate independently by old bucket", boundary_version: version(@v1), rate_version: newer_rate.price_version, coherence_assertion_rejected: true})
  end

  @tag slow: "interrupts an uncommitted import observed by another BEAM"
  test "interrupted writer rolls back and repeated imports leave the peer coherent", ctx do
    peer = start_peer!(ctx)
    v1 = catalog_file!(ctx, @v1, 272_000, 2)
    v2 = catalog_file!(ctx, @v2, 500_000, 7)
    assert {:ok, _} = UnboxedFixture.run_unboxed(fn -> OpenAIPricingImporter.import_file(v1) end)

    for _ <- 1..2 do
      writer = held_import!(ctx, v2)
      assert_receive {:import_held, _, _, %{inserted: 1}}, @budget
      assert_coherent!(remote_lookup(peer, ctx.model), @v1, 272_000, "long_context", 4)
      monitor = Process.monitor(writer.pid)
      assert nil == Task.shutdown(writer, :brutal_kill)
      assert_receive {:DOWN, ^monitor, :process, _, :killed}, @budget
      assert_coherent!(remote_lookup(peer, ctx.model), @v1, 272_000, "long_context", 4)
    end

    assert {:ok, %{inserted: 2}} = UnboxedFixture.run_unboxed(fn -> OpenAIPricingImporter.import_file(v2) end)
    assert_coherent!(remote_lookup(peer, ctx.model), @v2, 500_000, "default", 7)
    receipt(%{phase: "interrupted_import", interrupted_transactions: 2, partial_generation_visible: false, resumed_import: "complete_v2"})
  end

  @tag slow: "proves cleanup after a partial peer setup or failed assertion"
  test "failure injection retains teardown ownership before resource acquisition", ctx do
    peer = start_peer!(ctx)
    path = catalog_file!(ctx, @v1, 272_000, 2)
    assert {:ok, _} = UnboxedFixture.run_unboxed(fn -> OpenAIPricingImporter.import_file(path) end)
    assert_coherent!(remote_lookup(peer, ctx.model), @v1, 272_000, "long_context", 4)

    if System.get_env("PRICING_PEER_FAILURE") == "assertion", do: flunk("synthetic assertion failure after committed import")
  end

  defp start_peer!(ctx) do
    peer = InstancePresencePeer.start_presence_peer!(ctx.name)
    os_pid = :erpc.call(peer.remote, :os, :getpid, [], @budget) |> List.to_string()
    os_identity = InstancePresencePeer.capture_os_process_identity!(os_pid)
    Agent.update(ctx.ownership, &Map.merge(&1, %{peer: peer, os_identity: os_identity}))
    receipt(%{phase: "peer_acquired", run_id: ctx.run_id, peer_node: to_string(peer.remote), peer_os_pid: os_pid})

    if System.get_env("PRICING_PEER_FAILURE") == "startup", do: raise("synthetic peer bootstrap failure after VM acquisition")

    config = Repo.config() |> Keyword.merge(pool: DBConnection.ConnectionPool, pool_size: 1, parameters: [application_name: InstancePresencePeer.peer_application_name(ctx.run_id)])
    {:ok, _} = :erpc.call(peer.remote, Application, :ensure_all_started, [:ecto_sql], @budget)
    {:ok, _} = :erpc.call(peer.remote, Application, :ensure_all_started, [:postgrex], @budget)
    repo_pid = :erpc.call(peer.remote, CodexPooler.Gateway.Transports.WebsocketOwnerNodeHarness, :start_repo, [config], @budget)
    assert is_pid(repo_pid)
    %{rows: [[backend, database]]} = :erpc.call(peer.remote, Repo, :query!, ["SELECT pg_backend_pid(), current_database()"], @budget)
    assert database == Keyword.fetch!(Repo.config(), :database)
    assert peer.remote != node()
    receipt(%{phase: "peer_ready", run_id: ctx.run_id, peer_node: to_string(peer.remote), peer_os_pid: os_pid, backend: backend, database: database})
    if System.get_env("PRICING_PEER_FAILURE") == "startup_repo", do: raise("synthetic peer bootstrap failure after Repo acquisition")
    Map.put(peer, :backend, backend)
  end

  defp stop_peer!(%{peer: nil}, name), do: PeerRegistry.assert_peer_absent!(name)

  defp stop_peer!(state, name) do
    try do
      :peer.stop(state.peer.peer)
    catch
      :exit, _already_stopped -> :ok
    end

    InstancePresencePeer.assert_os_process_stopped!(state.os_identity, budget_ms: @budget)
    PeerRegistry.assert_peer_absent!(name, peer_node: state.peer.remote)
  end

  defp committed_fixture!(ctx) do
    UnboxedFixture.run_unboxed(fn ->
      {:ok, fixture} =
        Repo.transaction(fn ->
          fixture = CodexPooler.AccountingTestSupport.accounting_setup()
          Repo.update_all(from(binding in CodexPooler.Access.APIKeyPolicyBinding, where: binding.api_key_id == ^fixture.api_key.id), set: [max_tokens_per_day: 2_000_000, max_tokens_per_week: 10_000_000])
          Repo.delete!(fixture.pricing)
          model = fixture.model |> Ecto.Changeset.change(pricing_ref: ctx.model, upstream_model_id: ctx.model) |> Repo.update!()
          fixture = %{fixture | model: model}
          Agent.update(ctx.ownership, &Map.put(&1, :fixture, fixture))
          fixture
        end)

      fixture
    end)
  end

  defp settle!(fixture) do
    {:ok, reserved} = Accounting.reserve(fixture.auth, fixture.model, %{"model" => fixture.model.exposed_model_id, "max_output_tokens" => 1}, %{correlation_id: Ecto.UUID.generate()})
    {:ok, attempt} = Accounting.create_attempt(reserved.request, fixture.assignment)
    {:ok, result} = Accounting.finalize_success(reserved.request, attempt, %{status: "usage_known", input_tokens: 400_000, output_tokens: 10, total_tokens: 400_010}, %{response_status_code: 200})
    result.settlement
  end

  defp held_import!(ctx, path) do
    parent = self()
    handler = {__MODULE__, make_ref()}
    on_exit(fn -> :telemetry.detach(handler) end)

    writer =
      Task.Supervisor.async_nolink(ctx.imports, fn ->
        receive do
          :start -> Sandbox.unboxed_run(Repo, fn -> OpenAIPricingImporter.import_file(path) end)
        after
          @budget -> raise "import start barrier timed out"
        end
      end)

    :ok = :telemetry.attach(handler, [:codex_pooler, :repo, :query], &__MODULE__.hold_first_insert/4, %{writer: writer.pid, parent: parent, inserts: :atomics.new(1, [])})
    send(writer.pid, :start)
    writer
  end

  defp await_import!(writer) do
    monitor = Process.monitor(writer.pid)
    result = Task.await(writer, @budget)
    assert_receive {:DOWN, ^monitor, :process, _, reason} when reason in [:normal, :noproc], @budget
    result
  end

  @doc false
  @spec hold_first_insert(list(atom()), map(), map(), map()) :: :ok
  def hold_first_insert(_event, _measurements, metadata, config) do
    if self() == config.writer and String.starts_with?(metadata.query, "INSERT INTO \"pricing_snapshots\"") and :atomics.add_get(config.inserts, 1, 1) == 1 do
      %{rows: [[backend]]} = Repo.query!("SELECT pg_backend_pid()")
      send(config.parent, {:import_held, self(), backend, %{inserted: 1}})

      receive do
        :commit -> :ok
      after
        @budget -> Repo.rollback(:import_release_timeout)
      end
    end

    :ok
  end

  defp remote_lookup(peer, identifier), do: :erpc.call(peer.remote, PricingResolution, :lookup, lookup_args(identifier), @budget)
  defp lookup(identifier), do: apply(PricingResolution, :lookup, lookup_args(identifier))

  defp lookup_args(identifier), do: [%Model{pricing_ref: identifier, upstream_model_id: identifier, exposed_model_id: identifier}, identifier, %{}, %{reservation_estimate: %{input_tokens: 400_000}}, ~U[2026-01-01 00:00:00.000000Z]]

  defp assert_coherent!(pricing, generated_at, cutoff, bucket, input_rate) do
    expected_version = version(generated_at)
    assert pricing.snapshot.price_version == expected_version, "mixed pricing generation"
    assert pricing.context_pricing["generation"]["price_version"] == expected_version
    assert pricing.context_pricing["long_context_input_tokens_gt"] == cutoff
    assert pricing.snapshot.config["context_pricing"]["long_context_input_tokens_gt"] == cutoff
    assert pricing.price_bucket == bucket
    assert pricing.snapshot.config["price_bucket"] == bucket
    assert Decimal.equal?(pricing.snapshot.input_token_micros, Decimal.new(input_rate))
    assert Decimal.equal?(PricingResolution.cost_micros(pricing.snapshot, %{input_tokens: 400_000, output_tokens: 10}), Decimal.new(400_000 * input_rate + 30 * input_rate))
  end

  defp observation(pricing), do: %{version: pricing.snapshot.price_version, cutoff: pricing.context_pricing["long_context_input_tokens_gt"], bucket: pricing.price_bucket, input_rate: Decimal.to_string(pricing.snapshot.input_token_micros), cost_micros: Decimal.to_string(PricingResolution.cost_micros(pricing.snapshot, %{input_tokens: 400_000, output_tokens: 10}))}
  defp version(generated_at), do: generated_at <> ":importer-format-3"
  defp snapshots(model), do: Repo.all(from p in PricingSnapshot, where: p.model_identifier == ^model, order_by: p.id)
  defp receipt(value), do: CodexPooler.TestDiagnostics.puts(fn -> CodexPooler.JSON.encode!(value) end)

  defp catalog_file!(ctx, generated_at, cutoff, rate, conflict \\ false) do
    model = %{"model" => ctx.model, "category" => "language_model", "categories" => ["language_model"], "pricing_type" => "per_1m_tokens", "pricing_types" => ["per_1m_tokens"], "prices" => %{"standard" => %{"default" => rates(rate), "long_context" => rates(rate * 2)}}, "context_pricing" => %{"version" => 1, "tiers" => %{"standard" => %{"status" => "known", "long_context_input_tokens_gt" => cutoff, "provenance" => %{"locator" => "synthetic/context-note", "evidence_sha256" => String.duplicate("a", 64)}}}}}
    models = %{ctx.model => model}
    models = if conflict, do: Map.put(models, "a-" <> ctx.model, Map.put(model, "model", "a-" <> ctx.model)), else: models
    payload = %{"schema_version" => 2, "generated_at" => generated_at, "source" => "synthetic", "source_url" => "https://example.com/pricing", "models" => models, "models_count" => map_size(models), "tools" => %{"sample-tool" => %{"details" => "Synthetic tool", "price" => 0, "pricing" => "$0", "tool" => "Sample Tool"}}, "tools_count" => 1}
    path = Path.join(ctx.scratch, "#{cutoff}-#{rate}.json")
    File.write!(path, CodexPooler.JSON.encode!(payload))
    path
  end

  defp rates(input), do: %{"input" => input, "cached_input" => input / 4, "output" => input * 3}
end
