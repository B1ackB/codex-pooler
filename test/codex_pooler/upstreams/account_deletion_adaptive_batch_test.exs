defmodule CodexPooler.Upstreams.AccountDeletionAdaptiveBatchTest do
  use CodexPooler.DataCase, async: false

  alias CodexPooler.Access.APIKey
  alias CodexPooler.Accounting.LedgerEntry
  alias CodexPooler.Audit.AuditEvent
  alias CodexPooler.Pools.Pool
  alias CodexPooler.UnboxedFixture
  alias CodexPooler.Upstreams
  alias CodexPooler.Upstreams.Schemas.UpstreamIdentity

  import CodexPooler.PoolerFixtures

  @batch_key "permanent_deletion_batch_size"

  setup do
    name = String.to_atom("adaptive_deletion_repo_#{System.unique_integer([:positive])}")
    config = Repo.config() |> Keyword.put(:name, name) |> Keyword.put(:pool, DBConnection.ConnectionPool) |> Keyword.put(:pool_size, 2)
    start_supervised!({Repo, config})
    Repo.put_dynamic_repo(name)
    :ok
  end

  test "cancellation retains committed progress and smaller batches persist across worker passes" do
    %{identity: identity, request: request} = history_fixture(751)
    before = history_amounts(request.id)

    install_cancellation_trigger(identity.id, "changed_count > 100 AND detached_count > changed_count")

    for limit <- [250, 125, 62] do
      assert {:error, %{code: :upstream_deletion_busy}} = continue(identity.id)
      assert Repo.get!(UpstreamIdentity, identity.id).metadata[@batch_key] == limit
      assert detached_count(request.id) == 500
      assert history_amounts(request.id) == before
    end

    assert :deleted = continue(identity.id)
    refute Repo.get(UpstreamIdentity, identity.id)
    assert detached_count(request.id) == 751
    assert history_amounts(request.id) == before
    assert :gone = continue(identity.id)
  end

  test "the minimum batch remains one when even one row is cancelled" do
    %{identity: identity, request: request} = history_fixture(1, %{@batch_key => 1})
    install_cancellation_trigger(identity.id, "changed_count > 0")

    for _pass <- 1..2 do
      assert {:error, %{code: :upstream_deletion_busy}} = continue(identity.id)
      assert Repo.get!(UpstreamIdentity, identity.id).metadata[@batch_key] == 1
      assert detached_count(request.id) == 0
    end
  end

  for value <- [nil, 0, -1, 501, "2", %{}] do
    test "invalid persisted batch size #{inspect(value)} falls back to the bounded default" do
      assert_invalid_limit(unquote(Macro.escape(value)))
    end
  end

  for code <- ["deadlock_detected", "lock_not_available"] do
    test "#{code} leaves the learned batch size unchanged" do
      assert_non_cancellation(unquote(code))
    end
  end

  defp assert_invalid_limit(value) do
    %{identity: identity} = history_fixture(1, %{@batch_key => value})
    install_cancellation_trigger(identity.id, "changed_count > 0")
    assert {:error, %{code: :upstream_deletion_busy}} = continue(identity.id)
    assert Repo.get!(UpstreamIdentity, identity.id).metadata[@batch_key] == 250
  end

  defp assert_non_cancellation(code) do
    %{identity: identity} = history_fixture(1, %{@batch_key => 62})
    install_cancellation_trigger(identity.id, "changed_count > 0", code)
    assert {:error, %{code: :upstream_deletion_busy}} = continue(identity.id)
    assert Repo.get!(UpstreamIdentity, identity.id).metadata[@batch_key] == 62
  end

  defp history_fixture(count, metadata \\ %{}) do
    pool_id = Ecto.UUID.generate()
    identity_id = Ecto.UUID.generate()

    UnboxedFixture.register_unboxed_cleanup!(fn ->
      Repo.delete_all(from event in AuditEvent, where: event.target_id == ^identity_id)
      delete_committed_pools!([pool_id])
      Repo.delete_all(from identity in UpstreamIdentity, where: identity.id == ^identity_id)
    end)

    now = DateTime.utc_now()
    pool = Repo.insert!(%Pool{id: pool_id, name: "Deletion sample", slug: "deletion-#{pool_id}", status: "archived", created_at: now, updated_at: now})
    key = Repo.insert!(%APIKey{pool_id: pool.id, display_name: "Deletion key", key_prefix: "deletion-#{pool_id}", key_hash: :crypto.strong_rand_bytes(32), status: "revoked", revoked_at: now, created_at: now})
    request = request_fixture(%{pool: pool, api_key: key})
    metadata = Map.put(metadata, "permanent_deletion_requested_at", DateTime.to_iso8601(now))
    identity = Repo.insert!(%UpstreamIdentity{id: identity_id, chatgpt_account_id: "sample-#{identity_id}", account_label: "Sample deleted account", status: "deleted", onboarding_method: "import", created_at: now, updated_at: now, metadata: metadata})

    rows =
      Enum.map(1..count, fn _ ->
        %{id: Ecto.UUID.generate(), request_id: request.id, pool_id: pool.id, api_key_id: key.id, upstream_identity_id: identity.id, entry_kind: "adjustment", amount_status: "recorded", usage_status: "usage_known", transport: "http_json", total_tokens: 17, settled_cost_micros: Decimal.new(23), occurred_at: now, created_at: now}
      end)

    assert {^count, nil} = Repo.insert_all(LedgerEntry, rows)
    %{identity: identity, request: request}
  end

  defp install_cancellation_trigger(identity_id, condition, code \\ "query_canceled") do
    name = "adaptive_deletion_#{System.unique_integer([:positive])}"

    UnboxedFixture.register_unboxed_cleanup!(fn ->
      Repo.query!("DROP TRIGGER IF EXISTS #{name} ON ledger_entries")
      Repo.query!("DROP FUNCTION IF EXISTS #{name}()")
    end)

    Repo.query!("""
    CREATE FUNCTION #{name}() RETURNS trigger LANGUAGE plpgsql AS $$
    DECLARE changed_count integer; detached_count integer;
    BEGIN
      SELECT count(*) INTO changed_count FROM old_rows WHERE upstream_identity_id = '#{identity_id}'::uuid;
      SELECT count(*) INTO detached_count FROM ledger_entries WHERE request_id IN (SELECT request_id FROM old_rows) AND upstream_identity_id IS NULL;
      IF #{condition} THEN
        RAISE EXCEPTION 'sample batch cancellation' USING ERRCODE = '#{code}';
      END IF;
      RETURN NULL;
    END $$
    """)

    Repo.query!("CREATE TRIGGER #{name} AFTER UPDATE ON ledger_entries REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION #{name}()")
  end

  defp continue(identity_id), do: Upstreams.continue_account_deletion(identity_id, nil, System.monotonic_time(:millisecond) + 30_000)
  defp detached_count(request_id), do: Repo.aggregate(from(entry in LedgerEntry, where: entry.request_id == ^request_id and is_nil(entry.upstream_identity_id)), :count)
  defp history_amounts(request_id), do: Repo.all(from entry in LedgerEntry, where: entry.request_id == ^request_id, order_by: entry.id, select: {entry.id, entry.request_id, entry.total_tokens, entry.settled_cost_micros})
end
