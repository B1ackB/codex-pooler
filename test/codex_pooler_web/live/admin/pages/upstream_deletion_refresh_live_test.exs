defmodule CodexPoolerWeb.Admin.UpstreamDeletionRefreshLiveTest do
  use CodexPoolerWeb.ConnCase, async: false

  import CodexPooler.PoolerFixtures
  import CodexPooler.AccountsFixtures
  import Phoenix.LiveViewTest

  alias CodexPooler.Accounts
  alias CodexPooler.Jobs.UpstreamDeletionWorker
  alias CodexPooler.Repo
  alias CodexPooler.Upstreams.Schemas.UpstreamIdentity

  setup :register_and_log_in_user

  test "list monitors pending deletion and observes job-only failure", %{conn: conn} do
    {identity, job} = pending_deletion()
    {:ok, view, _} = live(conn, ~p"/admin/upstreams")
    render_async(view)
    assert is_reference(assigns(view).saved_reset_status_timer)
    job |> Ecto.Changeset.change(state: "discarded") |> Repo.update!()
    send(view.pid, :refresh_saved_reset_status_tick)
    render(view)
    render_async(view)
    assert has_element?(view, "#upstream-account-#{identity.id}", "Deletion failed - retry Delete")
    assert is_reference(assigns(view).saved_reset_status_timer)
  end

  test "cockpit monitors deletion without Pool subscriptions and observes job-only failure", %{conn: conn} do
    {identity, job} = pending_deletion()
    {:ok, view, _} = live(conn, ~p"/admin/upstreams/#{identity.id}")
    render_async(view)
    assert assigns(view).subscribed_pool_ids == MapSet.new()
    assert is_reference(assigns(view).saved_reset_status_timer)
    job |> Ecto.Changeset.change(state: "discarded") |> Repo.update!()
    cockpit_tick(view)
    assert has_element?(view, "#upstream-cockpit-status", "Deletion failed - retry Delete")
    assert is_reference(assigns(view).saved_reset_status_timer)
  end

  test "list removes completed deletion and preserves unrelated cards", %{conn: conn} do
    {identity, job} = pending_deletion()
    other = active_upstream_identity_fixture(%{account_label: "Sample other account"})
    {:ok, view, _} = live(conn, ~p"/admin/upstreams")
    render_async(view)
    assert :ok = UpstreamDeletionWorker.perform(job)
    refute Repo.get(UpstreamIdentity, identity.id)
    list_tick(view)
    refute has_element?(view, "#upstream-account-#{identity.id}")
    assert has_element?(view, "#upstream-account-#{other.id}")
    assert assigns(view).saved_reset_status_timer == nil
  end

  test "cockpit redirects after real cleanup without any Pool subscription", %{conn: conn} do
    {identity, job} = pending_deletion()
    {:ok, view, _} = live(conn, ~p"/admin/upstreams/#{identity.id}")
    render_async(view)
    assert :ok = UpstreamDeletionWorker.perform(job)
    refute Repo.get(UpstreamIdentity, identity.id)
    send(view.pid, {:saved_reset_status_tick, assigns(view).saved_reset_status_timer_token})
    flash = assert_redirect(view, ~p"/admin/upstreams", 15_000)
    assert flash["info"] == "Upstream account permanently deleted"
  end

  test "paused list retains pending state until resume rechecks the database", %{conn: conn} do
    {identity, job} = pending_deletion()
    {:ok, view, _} = live(conn, ~p"/admin/upstreams")
    render_async(view)
    render_click(view, "set_live_updates", %{"paused" => true})
    job |> Ecto.Changeset.change(state: "discarded") |> Repo.update!()
    list_tick(view)
    assert has_element?(view, "#upstream-account-#{identity.id}", "Deletion in progress")
    render_click(view, "set_live_updates", %{"paused" => false})
    render(view)
    render_async(view)
    assert has_element?(view, "#upstream-account-#{identity.id}", "Deletion failed - retry Delete")
  end

  test "paused cockpit retains pending state until resume rechecks the database", %{conn: conn} do
    {identity, job} = pending_deletion()
    {:ok, view, _} = live(conn, ~p"/admin/upstreams/#{identity.id}")
    render_async(view)
    render_click(view, "set_live_updates", %{"paused" => true})
    job |> Ecto.Changeset.change(state: "discarded") |> Repo.update!()
    cockpit_tick(view)
    assert has_element?(view, "#upstream-cockpit-status", "Deletion in progress")
    render_click(view, "set_live_updates", %{"paused" => false})
    render(view)
    render_async(view)
    assert has_element?(view, "#upstream-cockpit-status", "Deletion failed - retry Delete")
  end

  test "failed list and cockpit observe an external retry without a Pool event", %{conn: conn} do
    {identity, job} = pending_deletion()
    job |> Ecto.Changeset.change(state: "discarded") |> Repo.update!()
    {:ok, view, _} = live(conn, ~p"/admin/upstreams/#{identity.id}")
    {:ok, list, _} = live(conn, ~p"/admin/upstreams")
    render_async(view)
    render_async(list)
    assert assigns(view).subscribed_pool_ids == MapSet.new()
    assert is_reference(assigns(view).saved_reset_status_timer)
    assert is_reference(assigns(list).saved_reset_status_timer)
    assert :ok = Oban.retry_job(job.id)
    cockpit_tick(view)
    list_tick(list)
    assert has_element?(view, "#upstream-cockpit-status", "Deletion in progress")
    assert has_element?(list, "#upstream-account-#{identity.id}", "Deletion in progress")
    assert is_reference(assigns(view).saved_reset_status_timer)
    assert :ok = UpstreamDeletionWorker.perform(job)
    list_tick(list)
    refute has_element?(list, "#upstream-account-#{identity.id}")
    send(view.pid, {:saved_reset_status_tick, assigns(view).saved_reset_status_timer_token})
    flash = assert_redirect(view, ~p"/admin/upstreams", 15_000)
    assert flash["info"] == "Upstream account permanently deleted"
  end

  test "failed cockpit observes a retry completed entirely between status polls", %{conn: conn} do
    {identity, job} = pending_deletion()
    job |> Ecto.Changeset.change(state: "discarded") |> Repo.update!()
    {:ok, view, _} = live(conn, ~p"/admin/upstreams/#{identity.id}")
    render_async(view)
    assert is_reference(assigns(view).saved_reset_status_timer)
    assert :ok = Oban.retry_job(job.id)
    assert :ok = UpstreamDeletionWorker.perform(job)
    send(view.pid, {:saved_reset_status_tick, assigns(view).saved_reset_status_timer_token})
    flash = assert_redirect(view, ~p"/admin/upstreams", 15_000)
    assert flash["info"] == "Upstream account permanently deleted"
  end

  test "historical deleted accounts without a deletion lifecycle are not polled", %{conn: conn} do
    identity = active_upstream_identity_fixture(%{account_label: "Sample historical account"})
    identity |> Ecto.Changeset.change(status: "deleted") |> Repo.update!()
    {:ok, cockpit, _} = live(conn, ~p"/admin/upstreams/#{identity.id}")
    {:ok, list, _} = live(conn, ~p"/admin/upstreams")
    render_async(cockpit)
    render_async(list)
    assert assigns(cockpit).saved_reset_status_timer == nil
    assert assigns(list).saved_reset_status_timer == nil
  end

  test "filter navigation removes pending targets and stale ticks cannot restore them", %{conn: conn} do
    {identity, _job} = pending_deletion()
    other = active_upstream_identity_fixture(%{account_label: "Sample visible account"})
    {:ok, view, _} = live(conn, ~p"/admin/upstreams")
    render_async(view)
    render_click(view, "select_status_filter", %{"status" => "active"})
    assert_patch(view, ~p"/admin/upstreams?status=active")
    list_tick(view)
    refute has_element?(view, "#upstream-account-#{identity.id}")
    assert has_element?(view, "#upstream-account-#{other.id}")
    assert assigns(view).saved_reset_status_timer == nil
  end

  test "obsolete cockpit timer token cannot apply a changed job state", %{conn: conn} do
    {identity, job} = pending_deletion()
    {:ok, view, _} = live(conn, ~p"/admin/upstreams/#{identity.id}")
    render_async(view)
    job |> Ecto.Changeset.change(state: "discarded") |> Repo.update!()
    send(view.pid, {:saved_reset_status_tick, make_ref()})
    render(view)
    assert has_element?(view, "#upstream-cockpit-status", "Deletion in progress")
    cockpit_tick(view)
    assert has_element?(view, "#upstream-cockpit-status", "Deletion failed - retry Delete")
  end

  test "non-owner completion reports unavailability without inferring revoked access", %{scope: scope} do
    {identity, job, pool} = assigned_pending_deletion()
    {_operator, conn} = operator_conn(scope, pool)
    {:ok, view, _} = live(conn, ~p"/admin/upstreams/#{identity.id}")
    render_async(view)
    assert :ok = UpstreamDeletionWorker.perform(job)
    send(view.pid, {:saved_reset_status_tick, assigns(view).saved_reset_status_timer_token})
    flash = assert_redirect(view, ~p"/admin/upstreams", 15_000)
    assert flash["info"] == "Upstream account is no longer available"
  end

  test "actual access revocation during pending deletion retains access-change semantics", %{scope: scope} do
    {identity, _job, pool} = assigned_pending_deletion()
    {operator, conn} = operator_conn(scope, pool)
    {:ok, view, _} = live(conn, ~p"/admin/upstreams/#{identity.id}")
    render_async(view)
    render_click(view, "set_live_updates", %{"paused" => true})
    assert {:ok, _operator} = Accounts.update_operator(scope, operator, %{"pool_ids" => []})
    flash = assert_redirect(view, ~p"/admin/upstreams", 15_000)
    assert flash["info"] == "Your Pool access changed"
    assert Repo.get(UpstreamIdentity, identity.id)
  end

  test "closing an unrelated dialog resumes pending deletion refresh", %{conn: conn} do
    {identity, job} = pending_deletion()
    other = active_upstream_identity_fixture(%{account_label: "Sample rename account"})
    {:ok, view, _} = live(conn, ~p"/admin/upstreams")
    render_async(view)
    render_click(view, "open_rename_account", %{"id" => other.id})
    job |> Ecto.Changeset.change(state: "discarded") |> Repo.update!()
    list_tick(view)
    assert has_element?(view, "#upstream-account-#{identity.id}", "Deletion in progress")
    assert assigns(view).renaming_account.identity.id == other.id
    render_click(view, "cancel_rename_account")
    list_tick(view)
    assert has_element?(view, "#upstream-account-#{identity.id}", "Deletion failed - retry Delete")
    assert is_reference(assigns(view).saved_reset_status_timer)
  end

  defp pending_deletion do
    identity = active_upstream_identity_fixture(%{account_label: "Sample pending account"})
    mark_pending(identity)
  end

  defp assigned_pending_deletion do
    pool = pool_fixture()
    %{identity: identity} = upstream_assignment_fixture(pool, %{identity_status: "deleted", assignment_status: "deleted", eligibility_status: "ineligible"})
    {identity, job} = mark_pending(identity)
    {identity, job, pool}
  end

  defp mark_pending(identity) do
    identity = identity |> Ecto.Changeset.change(status: "deleted", metadata: %{"permanent_deletion_requested_at" => DateTime.to_iso8601(DateTime.utc_now())}) |> Repo.update!()
    {:ok, job} = Oban.insert(UpstreamDeletionWorker.new(%{"upstream_identity_id" => identity.id}))
    {identity, job}
  end

  defp operator_conn(scope, pool) do
    %{user: operator} = operator_fixture(scope, %{"role" => "instance_admin", "password_change_required" => "false"})
    operator_pool_assignment_fixture(operator, pool, created_by_user_id: scope.user.id)
    assert {:ok, %{token: token}} = Accounts.login_user(%{"email" => operator.email, "password" => valid_user_password()})
    {operator, log_in_user(build_conn(), operator, token)}
  end

  defp cockpit_tick(view) do
    send(view.pid, {:saved_reset_status_tick, assigns(view).saved_reset_status_timer_token})
    render(view)
    render_async(view)
  end

  defp list_tick(view) do
    send(view.pid, :refresh_saved_reset_status_tick)
    render(view)
    render_async(view)
  end

  defp assigns(view), do: :sys.get_state(view.pid).socket.assigns
end
