defmodule CodexPoolerWeb.Admin.RequestLogsClientCancelledLiveTest do
  use CodexPoolerWeb.ConnCase, async: false

  import Phoenix.LiveViewTest
  import CodexPooler.PoolerFixtures

  alias CodexPooler.Pools

  # A client cancellation is recorded `failed` with `client_disconnected`: 499
  # on a websocket, the 200 an HTTP stream had already sent. The request log
  # shows it as its own class and filters it on its own, while the Pooler-side
  # cuts that share the 499 stay failures (findings#292).
  @detection_timeout_ms 15_000

  setup :register_and_log_in_user

  setup %{scope: scope} do
    {:ok, pool} = Pools.create_pool(scope, %{slug: "client-cancel-logs-#{System.unique_integer([:positive])}", name: "Client Cancel Logs"})
    %{api_key: api_key} = active_api_key_fixture(pool)
    %{assignment: assignment} = upstream_assignment_fixture(pool, %{account_label: "Client cancel upstream"})
    context = %{pool: pool, api_key: api_key, assignment: assignment}

    rows = %{
      cancelled_websocket: log_row!(context, %{status: "failed", last_error_code: "client_disconnected", response_status_code: 499, transport: "websocket"}),
      cancelled_stream: log_row!(context, %{status: "failed", last_error_code: "client_disconnected", response_status_code: 200, transport: "http_sse"}),
      drained: log_row!(context, %{status: "failed", last_error_code: "owner_drained", response_status_code: 499, transport: "websocket"}),
      recovered: log_row!(context, %{status: "failed", last_error_code: "dead_execution_recovered", response_status_code: 499, transport: "http_sse"}),
      succeeded: log_row!(context, %{status: "succeeded"})
    }

    %{pool: pool, rows: rows}
  end

  test "a client cancellation shows its own label and warning tone while 499 cuts on the Pooler's side stay failed", %{conn: conn, pool: pool, rows: rows} do
    {:ok, view, _html} = live_request_logs(conn, ~p"/admin/request-logs?pool_id=#{pool.id}")

    for key <- [:cancelled_websocket, :cancelled_stream] do
      row = "#request-log-row-#{rows[key].id}"
      assert has_element?(view, "#{row}[data-status='failed'][data-display-status='client_cancelled']")
      assert has_element?(view, "#{row} [data-role='status-text'].text-warning", "Client cancelled")
      assert has_element?(view, "#{row} [data-role='status-icon'][data-status='client_cancelled'] .hero-stop-circle")
      assert has_element?(view, "#request-log-#{rows[key].id}-errors.text-warning", "client_disconnected")
      assert has_element?(view, "#request-log-#{rows[key].id}-errors .hero-exclamation-triangle.text-warning")
      refute has_element?(view, "#{row} [data-role='status-text'].text-error")
    end

    for key <- [:drained, :recovered] do
      row = "#request-log-row-#{rows[key].id}"
      assert has_element?(view, "#{row}[data-status='failed'][data-display-status='failed']")
      assert has_element?(view, "#{row} [data-role='status-text'].text-error", "Failed")
      assert has_element?(view, "#{row} [data-role='status-icon'][data-status='failed'] .hero-x-circle")
      assert has_element?(view, "#request-log-#{rows[key].id}-errors.text-error")
    end
  end

  test "the status filter lists client cancellations on their own and says that Failed leaves them out", %{conn: conn, pool: pool, rows: rows} do
    {:ok, view, _html} = live_request_logs(conn, ~p"/admin/request-logs?pool_id=#{pool.id}")

    assert has_element?(view, "#request-log-status-filter [data-role='status-filter-option'][data-status='client_cancelled']", "Client cancelled")
    assert has_element?(view, "#request-log-status-filter [data-role='status-filter-option'][data-status='client_cancelled'] .hero-stop-circle")
    # Nothing writes the recorded `cancelled` status, so it has no option next to the class.
    refute has_element?(view, "#request-log-status-filter [data-role='status-filter-option'][data-status='client_cancelled']", "Cancelled")
    refute has_element?(view, "#request-log-status-filter [data-role='status-filter-option'][data-status='cancelled']")
    assert has_element?(view, "#request-log-status-filter [data-role='status-filter-option'][data-status='failed'] [data-role='status-filter-option-detail']", "excl. client cancelled")

    view
    |> element("#request-log-status-filter [data-role='status-filter-option'][data-status='client_cancelled']")
    |> render_click()

    assert_patch(view, ~p"/admin/request-logs?pool_id=#{pool.id}&status=client_cancelled")
    _ = await_request_logs(view)

    assert has_element?(view, "#filters_status[type='hidden'][value='client_cancelled']")
    assert listed(view, rows) == [:cancelled_stream, :cancelled_websocket]
    refute has_element?(view, "#request-log-filter-errors")

    view
    |> element("#request-log-status-filter [data-role='status-filter-option'][data-status='failed']")
    |> render_click()

    assert_patch(view, ~p"/admin/request-logs?pool_id=#{pool.id}&status=failed")
    _ = await_request_logs(view)

    assert listed(view, rows) == [:drained, :recovered]
    assert has_element?(view, "#request-log-status-filter [data-role='status-filter-trigger'][title*='not counting requests the client cancelled']", "Failed")
    assert has_element?(view, "#request-log-status-filter [data-role='status-filter-detail']", "excl. client cancelled")

    {:ok, any_view, _html} = live_request_logs(conn, ~p"/admin/request-logs?pool_id=#{pool.id}")
    assert listed(any_view, rows) == [:cancelled_stream, :cancelled_websocket, :drained, :recovered, :succeeded]
    refute has_element?(any_view, "#request-log-status-filter [data-role='status-filter-detail']")
  end

  test "the recorded cancelled status is not a filter value any more", %{conn: conn, pool: pool} do
    {:ok, view, _html} = live_request_logs(conn, ~p"/admin/request-logs?pool_id=#{pool.id}&status=cancelled")

    assert has_element?(view, "#request-log-filter-errors", "Status filter is not supported")
  end

  # The recorded `cancelled` status is permitted by the database and written by
  # nothing: a row that carried it would look like any status the page does not
  # know, with no warning tone of its own.
  test "a request recorded with the cancelled status nothing writes has no tone of its own", %{conn: conn, pool: pool} do
    %{api_key: api_key} = active_api_key_fixture(pool)
    %{assignment: assignment} = upstream_assignment_fixture(pool, %{account_label: "Recorded cancelled upstream"})
    request = log_row!(%{pool: pool, api_key: api_key, assignment: assignment}, %{status: "cancelled", last_error_code: "request_cancelled", response_status_code: 499})

    {:ok, view, _html} = live_request_logs(conn, ~p"/admin/request-logs?pool_id=#{pool.id}&request_id=#{request.id}")

    row = "#request-log-row-#{request.id}"
    assert has_element?(view, "#{row} [data-role='status-text']", "Cancelled")
    refute has_element?(view, "#{row} [data-role='status-text'].text-warning")
    refute has_element?(view, "#{row} [data-role='status-icon'] .hero-no-symbol")
    assert has_element?(view, "#request-log-#{request.id}-errors", "request_cancelled")
    refute has_element?(view, "#request-log-#{request.id}-errors.text-warning")

    render_click(element(view, "#request-log-#{request.id}-open-details"))
    _ = assert_patch(view)

    assert has_element?(view, "#request-log-detail-sidebar header span", "Cancelled")
    refute has_element?(view, "#request-log-detail-sidebar header span.text-warning")
  end

  test "the detail drawer names the class and the status it was recorded with", %{conn: conn, pool: pool, rows: rows} do
    {:ok, view, _html} = live_request_logs(conn, ~p"/admin/request-logs?pool_id=#{pool.id}&status=client_cancelled")

    render_click(element(view, "#request-log-#{rows.cancelled_stream.id}-open-details"))
    _ = assert_patch(view)

    assert has_element?(view, "#request-log-detail-sidebar header span.text-warning", "Client cancelled")
    assert has_element?(view, "#request-log-detail-status", "Client cancelled (recorded as failed)")
    assert has_element?(view, "#request-log-detail-error-code", "client_disconnected")
    assert has_element?(view, "#request-log-detail-response-status", "200")
  end

  test "accepted is selectable and URL-valid with an execution reservation explanation", %{conn: conn, pool: pool} do
    %{api_key: api_key} = active_api_key_fixture(pool)
    request = request_fixture(%{pool: pool, api_key: api_key}, %{status: "accepted", completed_at: nil, usage_status: "usage_unknown"})
    {:ok, view, _html} = live_request_logs(conn, ~p"/admin/request-logs?pool_id=#{pool.id}")

    assert has_element?(view, "#request-log-status-filter [data-status='accepted'] .hero-clock")
    view |> element("#request-log-status-filter [data-role='status-filter-option'][data-status='accepted']") |> render_click()
    assert_patch(view, ~p"/admin/request-logs?pool_id=#{pool.id}&status=accepted")
    _ = await_request_logs(view)
    assert has_element?(view, "#filters_status[value='accepted']")
    assert has_element?(view, "#request-log-row-#{request.id}[data-status='accepted'] [data-role='status-text'].text-info", "Accepted")
    assert has_element?(view, "#request-log-row-#{request.id} [data-role='status-label'][title*='execution reservation is pending']")
    refute has_element?(view, "#request-log-filter-errors")

    {:ok, url_view, _html} = live_request_logs(conn, ~p"/admin/request-logs?pool_id=#{pool.id}&status=accepted&selected_request_id=#{request.id}")
    assert has_element?(url_view, "#request-log-detail-status", "Accepted")
    assert has_element?(url_view, "#request-log-detail-status-meaning", "execution reservation is pending")
    assert has_element?(url_view, "#request-log-detail-sidebar header span.text-info", "Accepted")
  end

  test "cancellation copy permits automatic interruption and connection loss without inferring zero usage", %{conn: conn, pool: pool, rows: rows} do
    {:ok, view, _html} = live_request_logs(conn, ~p"/admin/request-logs?pool_id=#{pool.id}&status=client_cancelled&selected_request_id=#{rows.cancelled_stream.id}")
    assert has_element?(view, "#request-log-status-filter [data-role='status-filter-detail']", "excl. failure totals")
    assert has_element?(view, "#request-log-detail-status-meaning", "automatic client interruption or connection loss")
    assert has_element?(view, "#request-log-detail-status-meaning", "does not imply zero usage")
    assert has_element?(view, "#request-log-detail-status-meaning", "Excluded from failure totals")
  end

  test "corrected measured cancellation and known zero stay distinct from unknown usage", %{conn: conn, pool: pool} do
    %{api_key: api_key} = active_api_key_fixture(pool)
    %{assignment: assignment} = upstream_assignment_fixture(pool)
    context = %{pool: pool, api_key: api_key, assignment: assignment}

    for {state, usage_status, tokens, micros, pricing} <- [
          {:corrected, "usage_known", 120, 12_300, "priced"},
          {:zero, "usage_known", 0, 0, "priced"},
          {:zero_unpriced, "usage_known", 0, 0, "unpriced_missing_model"},
          {:unknown, "usage_unknown", 0, 0, "priced"}
        ] do
      request = log_row!(context, %{status: "failed", last_error_code: "client_disconnected", usage_status: "usage_unknown"})

      if state == :corrected do
        ledger_entry_fixture(request, %{amount_status: "voided", usage_status: "usage_unknown", total_tokens: 0})
      end

      ledger_entry_fixture(request, %{
        usage_status: usage_status,
        input_tokens: tokens,
        output_tokens: 0,
        total_tokens: tokens,
        settled_cost_micros: micros,
        details: %{"pricing_status" => pricing, "settled_cost_micros" => if(usage_status == "usage_known" and pricing == "priced", do: to_string(micros))}
      })

      {:ok, view, _html} = live_request_logs(conn, ~p"/admin/request-logs?pool_id=#{pool.id}&request_id=#{request.id}&selected_request_id=#{request.id}")
      row = "#request-log-row-#{request.id}"
      assert has_element?(view, "#{row}[data-status='failed'][data-display-status='client_cancelled']", "Client cancelled")
      assert has_element?(view, "#request-log-detail-status", "Client cancelled (recorded as failed)")

      if usage_status == "usage_known" do
        assert has_element?(view, "#{row} [data-role='token-totals']", to_string(tokens))
        refute has_element?(view, "#{row} [data-role='usage-placeholder']")

        if pricing == "priced" do
          expected = if state == :corrected, do: "$0.0123", else: "$0.00"
          assert has_element?(view, "#{row} [data-role='cost']", expected)
        else
          assert has_element?(view, "#{row} [data-role='cost']", "cost n/a")
          refute has_element?(view, "#{row} [data-role='cost']", "$0")
        end
      else
        assert has_element?(view, "#{row} [data-role='usage-placeholder']", "—")
        assert has_element?(view, "#{row} [data-role='cost-placeholder']", "—")
        refute has_element?(view, "#{row} [data-role='token-totals']")
        refute has_element?(view, "#{row} [data-role='cost']")
      end
    end
  end

  defp listed(view, rows) do
    rows
    |> Enum.filter(fn {_key, request} -> has_element?(view, "#request-log-row-#{request.id}") end)
    |> Enum.map(fn {key, _request} -> key end)
    |> Enum.sort()
  end

  defp log_row!(%{pool: pool, api_key: api_key, assignment: assignment}, attrs) do
    status = Map.fetch!(attrs, :status)

    request =
      request_fixture(%{pool: pool, api_key: api_key}, %{
        status: status,
        usage_status: Map.get(attrs, :usage_status, "usage_known"),
        transport: Map.get(attrs, :transport, "http_json"),
        response_status_code: Map.get(attrs, :response_status_code, 200),
        last_error_code: Map.get(attrs, :last_error_code),
        correlation_id: "client-cancel-log-#{System.unique_integer([:positive])}"
      })

    attempt_fixture(request, assignment, %{
      status: if(status == "succeeded", do: "succeeded", else: "failed"),
      network_error_code: Map.get(attrs, :last_error_code),
      upstream_status_code: Map.get(attrs, :response_status_code, 200)
    })

    request
  end

  defp live_request_logs(conn, path) do
    with {:ok, view, html} <- live(conn, path) do
      _ = await_request_logs(view)
      {:ok, view, html}
    end
  end

  defp await_request_logs(view),
    do: await_request_logs(view, System.monotonic_time(:millisecond) + @detection_timeout_ms)

  defp await_request_logs(view, deadline) do
    _ = render_async(view, 5_000)
    state = :sys.get_state(view.pid)

    if state.socket.assigns.request_logs_loading? or state.socket.assigns.request_logs_running? do
      if System.monotonic_time(:millisecond) >= deadline, do: flunk("request logs did not finish loading")

      receive do
      after
        1 -> await_request_logs(view, deadline)
      end
    else
      state
    end
  end
end
