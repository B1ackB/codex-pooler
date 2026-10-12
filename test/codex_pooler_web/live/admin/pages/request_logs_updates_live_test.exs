defmodule CodexPoolerWeb.Admin.RequestLogsUpdatesLiveTest do
  use CodexPoolerWeb.ConnCase, async: false

  import Phoenix.LiveViewTest
  import CodexPooler.PoolerFixtures
  import CodexPooler.UnboxedFixture
  import Ecto.Query

  alias CodexPooler.Accounting
  alias CodexPooler.Accounts
  alias CodexPooler.Accounts.Scope
  alias CodexPooler.Events
  alias CodexPooler.Repo
  alias Ecto.Adapters.SQL.Sandbox

  @timeout 15_000

  setup context do
    CodexPooler.DataCase.stop_sandbox(context.sandbox_owner, context.sandbox_settings_cache)
    Sandbox.mode(Repo, :auto)
    on_exit(fn -> Sandbox.mode(Repo, :manual) end)
    %{user: owner, token: token} = CodexPooler.AccountsFixtures.committed_bootstrap_owner_fixture!()
    label = "request-updates-#{System.unique_integer([:positive])}"

    register_unboxed_cleanup!(fn ->
      identities = Repo.all(from identity in CodexPooler.Upstreams.Schemas.UpstreamIdentity, where: identity.account_label == ^label, select: identity.id)
      pools = Repo.all(from assignment in CodexPooler.Upstreams.Schemas.PoolUpstreamAssignment, where: assignment.upstream_identity_id in ^identities, select: assignment.pool_id)
      delete_committed_pools!(pools)
      Repo.delete_all(from identity in CodexPooler.Upstreams.Schemas.UpstreamIdentity, where: identity.id in ^identities)
      Repo.delete_all(from pricing in CodexPooler.Catalog.PricingSnapshot, where: pricing.price_version == ^label)
    end)

    {:ok, setup} = Repo.transaction(fn -> CodexPooler.AccountingTestSupport.accounting_setup(%{account_label: label, price_version: label}) end)
    %{conn: log_in_user(context.conn, owner, token), accounting: setup, owner: owner}
  end

  for pinned <- [false, true], drawer <- [false, true], paused <- [false, true] do
    @tag pinned: pinned, drawer: drawer, paused: paused
    test "committed matrix pinned=#{pinned} drawer=#{drawer} paused=#{paused}", context do
      %{conn: conn, accounting: setup, pinned: pinned, drawer: drawer, paused: paused} = context
      {request, attempt} = reserve_attempt!(setup)
      view = mount_logs!(conn, setup, request, pinned, drawer)
      initial_rows = rows(view)
      initial_detail = detail(view)
      generation = assigns(view).request_logs_generation
      if paused, do: pause(view, true)

      # Both mutations publish through the real committed accounting producer.
      assert {:ok, _} = Accounting.finalize_failure(request, attempt, %{last_error_code: "client_disconnected", response_status_code: 499})
      assert {:ok, correction} = Accounting.finalize_success_with_disposition(request, attempt, known_usage())
      assert correction.request.status == "failed"
      {arrival, _attempt} = reserve_attempt!(setup)
      await_event!(view, arrival.id)

      if paused do
        assert rows(view) == initial_rows
        assert detail(view) == initial_detail
        assert assigns(view).request_logs_generation == generation
      else
        settle!(view)
        if pinned or drawer, do: assert(rows(view) == initial_rows), else: assert(has_element?(view, "#request-log-row-#{arrival.id}"))
        if drawer, do: assert_corrected_detail(view, request)
        if pinned, do: assert(has_element?(view, "[data-role='request-log-newer-count']", "1 newer"))
      end

      if drawer do
        render_click(view, "close_request_log")
        assert_patch(view)
        settle!(view)
        refute has_element?(view, "#request-log-detail-request-id")
        if paused or pinned, do: assert(rows(view) == initial_rows), else: assert(has_element?(view, "#request-log-row-#{arrival.id}"))
      end

      if paused do
        before_resume = assigns(view).request_logs_generation
        pause(view, false)
        settle!(view)
        assert assigns(view).request_logs_generation == before_resume + 1
      end

      if pinned do
        assert rows(view) == initial_rows
        assert has_element?(view, "[data-role='request-log-newer-count']", "1 newer")
      else
        assert has_element?(view, "#request-log-row-#{arrival.id}")
        assert has_element?(view, "#request-log-row-#{request.id}[data-display-status='client_cancelled']")
        assert has_element?(view, "#request-log-row-#{request.id} [data-role='token-totals']", "120")
      end
    end
  end

  test "a refresh launched before pause cannot apply table or detail and resume reloads once", %{conn: conn, accounting: setup} do
    {request, attempt} = reserve_attempt!(setup)
    view = mount_logs!(conn, setup, request, false, true)
    initial_rows = rows(view)
    initial_detail = detail(view)
    barrier = block_query!(view)
    assert {:ok, _} = Accounting.finalize_failure(request, attempt, %{last_error_code: "client_disconnected", response_status_code: 499})
    query = await_query!(barrier)
    pause(view, true)
    assert {:ok, _} = Accounting.finalize_success_with_disposition(request, attempt, known_usage())
    await_event!(view, request.id)
    release_query!(barrier, query)
    settle!(view)
    assert rows(view) == initial_rows
    assert detail(view) == initial_detail
    generation = assigns(view).request_logs_generation
    pause(view, false)
    settle!(view)
    assert assigns(view).request_logs_generation == generation + 1
    assert rows(view) == initial_rows
    assert_corrected_detail(view, request)
  end

  test "an in-flight completion alone is held and resumes once without another event", %{conn: conn, accounting: setup} do
    {request, _attempt} = reserve_attempt!(setup)
    view = mount_logs!(conn, setup, request, false, false)
    initial_rows = rows(view)
    barrier = block_query!(view)
    {arrival, _attempt} = reserve_attempt!(setup)
    query = await_query!(barrier)
    pause(view, true)
    assert assigns(view).live_updates_held == %{}
    release_query!(barrier, query)
    settle!(view)
    assert rows(view) == initial_rows
    assert assigns(view).live_updates_held == %{}
    generation = assigns(view).request_logs_generation
    pause(view, false)
    settle!(view)
    assert assigns(view).request_logs_generation == generation + 1
    assert has_element?(view, "#request-log-row-#{arrival.id}")
  end

  test "explicit detail opens while paused and committed revocation removes held rows and detail", %{accounting: setup, owner: owner} do
    {request, _attempt} = reserve_attempt!(setup)
    scope = Scope.for_user(owner, ["instance_owner"])
    email = CodexPooler.AccountsFixtures.unique_user_email()

    register_unboxed_cleanup!(fn ->
      ids = Repo.all(from user in Accounts.User, where: user.email == ^email, select: user.id)
      CodexPooler.AccountsFixtures.delete_user_graph!(ids)
    end)

    %{user: operator} = CodexPooler.AccountsFixtures.operator_fixture(scope, %{"email" => email, "role" => "instance_admin", "password_change_required" => "false"})
    operator_pool_assignment_fixture(operator, setup.pool, created_by_user_id: owner.id)
    assert {:ok, %{token: token}} = Accounts.login_user(%{"email" => email, "password" => CodexPooler.AccountsFixtures.valid_user_password()})
    conn = log_in_user(build_conn(), operator, token)
    view = mount_logs!(conn, setup, request, false, false)
    pause(view, true)
    open!(view, request)
    assert has_element?(view, "#request-log-detail-request-id", request.id)
    assert {:ok, _} = Accounts.update_operator(scope, operator, %{"pool_ids" => []})
    pid = view.pid
    assert_receive {:trace, ^pid, :receive, {CodexPoolerWeb.Admin.NotificationCenterHooks, :viewer_visibility_changed}}, @timeout
    settle!(view)
    refute has_element?(view, "#request-log-row-#{request.id}")
    refute has_element?(view, "#request-log-detail-request-id")
    assert assigns(view).live_updates_paused?
    assert assigns(view).visible_pool_ids == []
  end

  test "opening a drawer while an automatic table query runs freezes its current rows", %{conn: conn, accounting: setup} do
    {request, _attempt} = reserve_attempt!(setup)
    view = mount_logs!(conn, setup, request, false, false)
    initial_rows = rows(view)
    barrier = block_query!(view)
    {arrival, _attempt} = reserve_attempt!(setup)
    query = await_query!(barrier)
    open!(view, request)
    release_query!(barrier, query)
    settle!(view)
    assert rows(view) == initial_rows
    assert has_element?(view, "#request-log-detail-request-id", request.id)
    render_click(view, "close_request_log")
    assert_patch(view)
    settle!(view)
    assert has_element?(view, "#request-log-row-#{arrival.id}")
  end

  test "closing a drawer while a query runs and pausing holds rows until resume", %{conn: conn, accounting: setup} do
    {request, _attempt} = reserve_attempt!(setup)
    view = mount_logs!(conn, setup, request, false, true)
    initial_rows = rows(view)
    barrier = block_query!(view)
    {arrival, _attempt} = reserve_attempt!(setup)
    query = await_query!(barrier)
    pause(view, true)
    render_click(view, "close_request_log")
    assert_patch(view)
    release_query!(barrier, query)
    settle!(view)
    assert rows(view) == initial_rows
    refute has_element?(view, "#request-log-detail-request-id")
    pause(view, false)
    settle!(view)
    assert has_element?(view, "#request-log-row-#{arrival.id}")
  end

  test "late A detail never replaces newly selected B or a freshly reopened A", %{conn: conn, accounting: setup} do
    {a, attempt} = reserve_attempt!(setup)
    {b, _attempt} = reserve_attempt!(setup)
    view = mount_logs!(conn, setup, a, false, true)
    barrier = block_query!(view, a.id)
    assert {:ok, _} = Accounting.finalize_failure(a, attempt, %{last_error_code: "client_disconnected", response_status_code: 499})
    query = await_query!(barrier)
    open!(view, b)
    release_query!(barrier, query)
    settle!(view)
    assert has_element?(view, "#request-log-detail-request-id", b.id)

    open!(view, a)
    barrier = block_query!(view, a.id)
    assert {:ok, _} = Accounting.finalize_success_with_disposition(a, attempt, known_usage())
    query = await_query!(barrier)
    # The completed SELECT already captured A's old metadata. A committed fixture
    # update without another invalidation isolates selection ordering from a later
    # refresh that could otherwise hide a transient stale overwrite.
    a |> Repo.reload!() |> Ecto.Changeset.change(correlation_id: "sample-new-selection") |> Repo.update!()
    open!(view, b)
    open!(view, a)
    assert has_element?(view, "#request-log-detail-correlation-id", "sample-new-selection")
    current_detail = detail(view)
    release_query!(barrier, query)
    settle!(view)
    assert detail(view) == current_detail
    assert_corrected_detail(view, a)
  end

  test "filter navigation supersedes a blocked automatic refresh even while paused", %{conn: conn, accounting: setup} do
    {request, attempt} = reserve_attempt!(setup)
    view = mount_logs!(conn, setup, request, false, true)
    barrier = block_query!(view)
    assert {:ok, _} = Accounting.finalize_failure(request, attempt, %{last_error_code: "client_disconnected", response_status_code: 499})
    query = await_query!(barrier)
    pause(view, true)
    render_click(view, "select_status_filter", %{"status" => "succeeded"})
    assert_patch(view)
    release_query!(barrier, query)
    settle!(view)
    assert has_element?(view, "#request-log-empty-state")
    refute has_element?(view, "#request-log-row-#{request.id}")
    assert assigns(view).filter_values["status"] == "succeeded"
  end

  test "rollback and idle resume do not refresh while duplicate committed changes coalesce", %{conn: conn, accounting: setup} do
    {request, attempt} = reserve_attempt!(setup)
    view = mount_logs!(conn, setup, request, false, false)
    generation = assigns(view).request_logs_generation
    pause(view, true)

    assert {:error, :deliberate_rollback} =
             Repo.transaction(fn ->
               reserve_attempt!(setup)
               Repo.rollback(:deliberate_rollback)
             end)

    # A same-producer marker on the actual PubSub subscription orders rollback silence.
    marker = make_ref()
    Phoenix.PubSub.broadcast(CodexPooler.PubSub, Events.pubsub_topic(setup.pool), {Events, %{pool_id: setup.pool.id, topics: ["task7_marker"], payload: %{marker: marker}}})
    assert_receive {:trace, _pid, :receive, {Events, %{payload: %{marker: ^marker}}}}, @timeout
    assert Enum.all?(assigns(view).live_updates_held, fn {{_pool, topics}, _event} -> "request_logs" not in topics end)
    pause(view, false)
    settle!(view)
    assert assigns(view).request_logs_generation == generation
    pause(view, true)

    for _ <- 1..3 do
      assert {:ok, _} = Accounting.finalize_failure(request, attempt, %{last_error_code: "client_disconnected", response_status_code: 499})
    end

    await_event!(view, request.id)
    pause(view, false)
    settle!(view)
    assert assigns(view).request_logs_generation == generation + 1
    assert has_element?(view, "#request-log-row-#{request.id}[data-display-status='client_cancelled']")
  end

  defp reserve_attempt!(setup) do
    {:ok, %{request: request}} = Accounting.reserve(setup.auth, setup.model, %{}, %{transport: "websocket", endpoint: "/backend-api/codex/responses", requested_model: setup.model.exposed_model_id, correlation_id: Ecto.UUID.generate()})
    {:ok, attempt} = Accounting.create_attempt(request, setup.assignment, %{transport: "websocket"})
    {request, attempt}
  end

  defp known_usage, do: %{status: "usage_known", input_tokens: 100, output_tokens: 20, total_tokens: 120}

  defp mount_logs!(conn, setup, request, pinned, drawer) do
    params = %{"pool_id" => setup.pool.id}
    params = if pinned, do: Map.merge(params, %{"as_of" => DateTime.to_iso8601(request.admitted_at), "as_of_id" => request.id}), else: params
    params = if drawer, do: Map.put(params, "selected_request_id", request.id), else: params
    {:ok, view, _} = live(conn, ~p"/admin/request-logs?#{params}")
    settle!(view)
    on_exit(fn -> if Process.alive?(view.pid), do: GenServer.stop(view.pid) end)
    :erlang.trace(view.pid, true, [:receive, {:tracer, self()}])
    view
  end

  defp pause(view, paused), do: render_hook(view, "set_live_updates", %{"paused" => paused})

  defp open!(view, request) do
    render_click(view, "open_request_log", %{"request-id" => request.id})
    assert_patch(view)
  end

  defp assigns(view), do: :sys.get_state(view.pid).socket.assigns

  defp rows(view) do
    rows = view |> render() |> LazyHTML.from_document() |> LazyHTML.query("#request-logs-table tr")
    {LazyHTML.attribute(rows, "id"), LazyHTML.attribute(rows, "data-status"), LazyHTML.text(rows)}
  end

  defp detail(view), do: view |> element("#request-log-detail-sidebar") |> render()

  defp assert_corrected_detail(view, request) do
    assert has_element?(view, "#request-log-detail-request-id", request.id)
    assert has_element?(view, "#request-log-detail-outcome", "Client cancelled")
    assert has_element?(view, "#request-log-detail-token-counts", "120")
    assert has_element?(view, "#request-log-detail-cost", "$0.0014")
  end

  defp await_event!(view, request_id) do
    pid = view.pid
    assert_receive {:trace, ^pid, :receive, {Events, %{payload: %{"request_id" => ^request_id}}}}, @timeout
    _ = assigns(view)
  end

  defp settle!(view), do: settle!(view, System.monotonic_time(:millisecond) + @timeout)

  defp settle!(view, deadline) do
    render_async(view, @timeout)
    state = assigns(view)

    if state.request_logs_running? or state.request_logs_loading? or is_reference(state.request_logs_reload_timer) do
      assert System.monotonic_time(:millisecond) < deadline, "request logs did not settle"

      receive do
      after
        10 -> settle!(view, deadline)
      end
    end
  end

  defp block_query!(view, selected_id \\ nil) do
    handler = {__MODULE__, make_ref()}
    parent = self()
    dumped_id = if selected_id, do: Ecto.UUID.dump!(selected_id)
    on_exit(fn -> :telemetry.detach(handler) end)

    :ok =
      :telemetry.attach(
        handler,
        [:codex_pooler, :repo, :query],
        fn _event, _measurements, metadata, _config ->
          if metadata[:source] == "requests" and view.pid in Process.get(:"$callers", []) and (is_nil(dumped_id) or dumped_id in metadata.params) do
            monitor = Process.monitor(parent)
            send(parent, {handler, self()})

            receive do
              {^handler, :release} -> :ok
              {:DOWN, ^monitor, :process, ^parent, _} -> exit(:shutdown)
            after
              15_000 -> raise "request-log query barrier was not released"
            end

            Process.demonitor(monitor, [:flush])
          end
        end,
        nil
      )

    handler
  end

  defp await_query!(handler) do
    assert_receive {^handler, pid}, @timeout
    on_exit(fn -> send(pid, {handler, :release}) end)
    :telemetry.detach(handler)
    pid
  end

  defp release_query!(handler, pid), do: send(pid, {handler, :release})
end
