defmodule CodexPoolerWeb.Admin.StatsClientCancellationsLiveTest do
  use CodexPoolerWeb.ConnCase, async: false

  import Phoenix.LiveViewTest
  import CodexPooler.PoolerFixtures
  import CodexPooler.UnboxedFixture
  import Ecto.Query

  alias CodexPooler.Accounting
  alias CodexPooler.Pools
  alias CodexPooler.Repo
  alias Ecto.Adapters.SQL.Sandbox

  @detection_timeout_ms 15_000

  setup context do
    if context[:committed] do
      committed_setup!(context)
    else
      register_and_log_in_user(context)
    end
  end

  # The request and success-rate cards name client cancellations instead of
  # counting them as failures (findings#292).
  test "the request cards count client cancellations apart and say the success rate leaves them out", %{conn: conn, scope: scope} do
    {:ok, pool} = Pools.create_pool(scope, %{slug: "stats-client-cancel-live-#{System.unique_integer([:positive])}", name: "Stats Client Cancel"})
    %{api_key: api_key} = active_api_key_fixture(pool)
    context = %{pool: pool, api_key: api_key}

    request_fixture(context, %{status: "succeeded"})
    request_fixture(context, %{status: "failed", last_error_code: "client_disconnected", response_status_code: 499, transport: "websocket"})
    request_fixture(context, %{status: "failed", last_error_code: "client_disconnected", response_status_code: 499, transport: "websocket", request_metadata: %{"downstream_interruption" => %{"origin" => "server", "cause" => "idle_timeout"}}})

    {:ok, view, _html} = live(conn, ~p"/admin/stats?pool_id=#{pool.id}")
    _ = await_stats_dashboard(view, System.monotonic_time(:millisecond) + @detection_timeout_ms)

    assert has_element?(view, "#stats-kpi-requests", "3")
    assert has_element?(view, "#stats-kpi-requests", "1 succeeded · 1 failed · 1 client cancelled")
    assert has_element?(view, "#stats-kpi-success-rate", "50.0%")
    assert has_element?(view, "#stats-kpi-success-rate", "Completed; excludes client cancellations")
  end

  @tag committed: true, slow: "observes the real one-second Stats event debounce and committed dashboard rebuild"
  test "mounted totals reload a committed unknown-to-known cancellation without changing its outcome", %{conn: conn, accounting: setup} do
    {request, attempt} = reserve_attempt!(setup)
    assert {:ok, _} = Accounting.finalize_failure(request, attempt, %{last_error_code: "client_disconnected", response_status_code: 499})
    {:ok, view, _} = live(conn, ~p"/admin/stats?pool_id=#{setup.pool.id}")
    _ = await_stats_dashboard(view, deadline())
    assert has_element?(view, "#stats-kpi-requests", "0 succeeded · 0 failed · 1 client cancelled")
    assert has_element?(view, "#stats-kpi-tokens [data-role='metric-card-value']", "0")
    generation = :sys.get_state(view.pid).socket.assigns.stats_dashboard_generation

    assert {:ok, {:ok, correction}} =
             run_unboxed(fn ->
               Repo.transaction(fn -> Accounting.finalize_success_with_disposition(request, attempt, known_usage()) end)
             end)

    assert correction.request.status == "failed"
    await_corrected_totals!(view, generation, deadline())
    assert has_element?(view, "#stats-kpi-requests", "0 succeeded · 0 failed · 1 client cancelled")
    assert has_element?(view, "#stats-kpi-tokens [data-role='metric-card-value']", "120")
    assert has_element?(view, "#stats-kpi-cost", "$0.0014")
    assert Process.alive?(view.pid)

    # The cockpit retains its explicit refresh/remount contract.
    {:ok, cockpit, _} = live(conn, ~p"/admin/upstreams/#{setup.identity.id}")
    _ = render_async(cockpit, 5_000)
    assert has_element?(cockpit, "#request-health-client-cancelled", "1")
  end

  @tag committed: true, slow: "observes the real one-second Stats event debounce and committed dashboard rebuild"
  test "mounted totals reload an outcome-only correction while keeping measured tokens and cost", %{conn: conn, accounting: setup} do
    {request, attempt} = reserve_attempt!(setup)
    assert {:ok, initial} = Accounting.finalize_success_with_disposition(request, attempt, known_usage())
    {:ok, view, _} = live(conn, ~p"/admin/stats?pool_id=#{setup.pool.id}")
    _ = await_stats_dashboard(view, deadline())
    assert has_element?(view, "#stats-kpi-requests", "1 succeeded · 0 failed")
    assert has_element?(view, "#stats-kpi-tokens [data-role='metric-card-value']", "120")
    cost = view |> element("#stats-kpi-cost") |> render()
    generation = :sys.get_state(view.pid).socket.assigns.stats_dashboard_generation

    assert {:ok, {:ok, correction}} =
             run_unboxed(fn ->
               Repo.transaction(fn ->
                 Accounting.reconcile_request_with_disposition(request, attempt, %{delivery_failure: %{status: "failed", response_status_code: 499, error_code: "client_disconnected"}})
               end)
             end)

    assert correction.settlement.id == initial.settlement.id
    await_corrected_totals!(view, generation, deadline())
    assert has_element?(view, "#stats-kpi-requests", "0 succeeded · 0 failed · 1 client cancelled")
    assert has_element?(view, "#stats-kpi-tokens [data-role='metric-card-value']", "120")
    assert view |> element("#stats-kpi-cost") |> render() == cost
    assert Process.alive?(view.pid)
  end

  defp reserve_attempt!(setup) do
    {:ok, %{request: request}} = Accounting.reserve(setup.auth, setup.model, %{}, %{transport: "websocket", endpoint: "/backend-api/codex/responses", requested_model: setup.model.exposed_model_id, correlation_id: Ecto.UUID.generate()})
    {:ok, attempt} = Accounting.create_attempt(request, setup.assignment, %{transport: "websocket"})
    {request, attempt}
  end

  defp known_usage, do: %{status: "usage_known", input_tokens: 100, output_tokens: 20, total_tokens: 120}
  defp deadline, do: System.monotonic_time(:millisecond) + @detection_timeout_ms

  defp await_corrected_totals!(view, generation, limit) do
    state = await_stats_dashboard(view, limit)
    assigns = state.socket.assigns

    if assigns.stats_dashboard_generation > generation and assigns.dashboard.kpis.tokens.total_tokens == 120 and assigns.dashboard.kpis.requests.client_cancelled == 1 do
      :ok
    else
      assert System.monotonic_time(:millisecond) < limit, "mounted Stats did not observe the committed correction"

      receive do
      after
        10 -> await_corrected_totals!(view, generation, limit)
      end
    end
  end

  defp committed_setup!(context) do
    CodexPooler.DataCase.stop_sandbox(context.sandbox_owner, context.sandbox_settings_cache)
    Sandbox.mode(Repo, :auto)
    on_exit(fn -> Sandbox.mode(Repo, :manual) end)
    %{user: owner, token: token} = CodexPooler.AccountsFixtures.committed_bootstrap_owner_fixture!()
    label = "stats-correction-#{System.unique_integer([:positive])}"

    register_unboxed_cleanup!(fn ->
      identities = Repo.all(from identity in CodexPooler.Upstreams.Schemas.UpstreamIdentity, where: identity.account_label == ^label, select: identity.id)
      pools = Repo.all(from assignment in CodexPooler.Upstreams.Schemas.PoolUpstreamAssignment, where: assignment.upstream_identity_id in ^identities, select: assignment.pool_id)
      delete_committed_pools!(pools)
      Repo.delete_all(from identity in CodexPooler.Upstreams.Schemas.UpstreamIdentity, where: identity.id in ^identities)
      Repo.delete_all(from pricing in CodexPooler.Catalog.PricingSnapshot, where: pricing.price_version == ^label)
    end)

    {:ok, setup} = Repo.transaction(fn -> CodexPooler.AccountingTestSupport.accounting_setup(%{account_label: label, price_version: label}) end)
    %{conn: log_in_user(context.conn, owner, token), accounting: setup}
  end

  defp await_stats_dashboard(view, deadline) do
    _ = render_async(view, 5_000)
    state = :sys.get_state(view.pid)

    if Map.get(state.socket.assigns, :dashboard_loading?, false) or Map.get(state.socket.assigns, :stats_dashboard_running?, false) do
      if System.monotonic_time(:millisecond) >= deadline, do: flunk("stats dashboard did not finish loading")

      receive do
      after
        1 -> await_stats_dashboard(view, deadline)
      end
    else
      state
    end
  end
end
