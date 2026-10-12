defmodule CodexPoolerWeb.Admin.RequestLogsCommitLiveTest do
  use CodexPoolerWeb.ConnCase, async: false

  import Phoenix.LiveViewTest
  import CodexPooler.PoolerFixtures

  alias CodexPooler.Accounting
  alias CodexPooler.Accounting.RequestLifecycle.Reservation
  alias CodexPooler.Accounts.Scope
  alias CodexPooler.Events
  alias CodexPooler.Gateway.Persistence.SessionContinuity
  alias CodexPooler.Gateway.Websocket
  alias CodexPooler.Pools
  alias CodexPooler.Repo
  alias Ecto.Adapters.SQL.Sandbox

  @detection_timeout_ms 15_000

  test "an accepted row converges after a nested reservation held past the refresh debounce commits", context do
    CodexPooler.DataCase.stop_sandbox(context.sandbox_owner, context.sandbox_settings_cache)
    Sandbox.mode(Repo, :auto)
    on_exit(fn -> Sandbox.mode(Repo, :manual) end)
    %{user: owner, token: token} = CodexPooler.AccountsFixtures.committed_bootstrap_owner_fixture!()
    conn = log_in_user(context.conn, owner, token)
    scope = Scope.for_user(owner, ["instance_owner"])
    {:ok, pool} = Pools.create_pool(scope, %{slug: "request-commit-#{System.unique_integer([:positive])}", name: "Committed requests"})
    setup = active_api_key_fixture(pool, %{scope: scope})
    model = model_fixture(pool)
    {:ok, session} = Websocket.start_codex_session(setup, %{accepted_turn_state: Ecto.UUID.generate()})
    opts = %{codex_session: session, endpoint: "/backend-api/codex/responses", requested_model: model.exposed_model_id, correlation_id: Ecto.UUID.generate(), transport: "websocket"}
    {:ok, %{request: request}} = Accounting.claim_websocket_turn(setup, model, opts)
    assert request.status == "accepted"
    row = "#request-log-row-#{request.id}"
    {:ok, view, _} = live(conn, ~p"/admin/request-logs?pool_id=#{pool.id}")
    await_row!(view, row <> "[data-status='accepted']", deadline())
    generation = :sys.get_state(view.pid).socket.assigns.request_logs_generation
    assert :ok = Events.subscribe_pool(pool, "request_logs")
    parent = self()
    release = make_ref()
    writer = Task.async(fn -> reserve_held!(setup, model, Map.put(opts, :turn_claim, request), parent, release) end)
    assert_receive :reservation_held, @detection_timeout_ms
    # Reproduce the production bug: the 250 ms debounce finishes while the outer commit is held.
    refute_receive {Events, %{reason: "request_reserved"}}, 300
    assert has_element?(view, row <> "[data-status='accepted']")
    send(writer.pid, release)
    assert {:ok, %{request: %{status: "in_progress"}}} = Task.await(writer, @detection_timeout_ms)
    assert_receive {Events, %{reason: "request_reserved"}}, @detection_timeout_ms
    await_row!(view, row <> "[data-status='in_progress']", deadline())
    assert :sys.get_state(view.pid).socket.assigns.request_logs_generation == generation + 1
    refute has_element?(view, row <> "[data-status='accepted']")
    assert Process.alive?(view.pid)
  end

  defp reserve_held!(setup, model, opts, parent, release) do
    Sandbox.unboxed_run(Repo, fn ->
      SessionContinuity.mailbox_admission_transaction(
        fn -> Reservation.mailbox_admission_session_ids(setup, model, opts) end,
        fn ->
          {:ok, reserved} = Accounting.reserve(setup, model, %{}, opts)
          send(parent, :reservation_held)
          await_release!(release, reserved)
        end,
        Reservation.mailbox_admission_exhausted_error()
      )
    end)
  end

  defp await_release!(release, reserved) do
    receive do
      ^release -> reserved
    after
      15_000 -> Repo.rollback(:release_timeout)
    end
  end

  defp deadline, do: System.monotonic_time(:millisecond) + @detection_timeout_ms

  defp await_row!(view, selector, limit) do
    _ = render_async(view, 5_000)

    if has_element?(view, selector) do
      :ok
    else
      assert System.monotonic_time(:millisecond) < limit, "mounted request logs did not show #{selector}"

      receive do
      after
        10 -> await_row!(view, selector, limit)
      end
    end
  end
end
