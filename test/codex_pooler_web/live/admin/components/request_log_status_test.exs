defmodule CodexPoolerWeb.Admin.RequestLogStatusTest do
  use ExUnit.Case, async: true

  import Phoenix.LiveViewTest

  alias CodexPoolerWeb.Admin.RequestLogFilterForm
  alias CodexPoolerWeb.Admin.RequestLogsPresentation

  test "each outcome has its established icon and color beside the readable status" do
    for {status, label, icon, tone} <- [
          {"accepted", "Accepted", "hero-clock", "text-info"},
          {"in_progress", "In progress", "hero-clock", "text-info"},
          {"succeeded", "Succeeded", "hero-check-circle", "text-success"},
          {"failed", "Failed", "hero-x-circle", "text-error"},
          {"rejected", "Rejected", "hero-shield-exclamation", "text-error"},
          # Nothing writes the recorded `cancelled` status: it has no look of its own.
          {"cancelled", "Cancelled", "hero-question-mark-circle", "text-base-content/65"},
          {nil, "Unknown", "hero-question-mark-circle", "text-base-content/65"}
        ] do
      latency = if status in ["accepted", "in_progress"], do: nil, else: 19_100

      document =
        render_component(&RequestLogsPresentation.request_log_timestamp_cell/1,
          request_log: %{id: "sample-request", status: status, admitted_at: ~U[2026-09-26 08:10:00Z], latency_ms: latency},
          datetime_preferences: %{datetime_format: "short", timezone: "Etc/UTC"},
          prefix: "request-log"
        )
        |> LazyHTML.from_fragment()

      icon_node = LazyHTML.query(document, "[data-role='status-icon'][aria-hidden='true']")
      assert LazyHTML.query(icon_node, ".#{icon}") |> Enum.count() == 1
      assert LazyHTML.attribute(icon_node, "class") |> hd() |> String.split() |> Enum.member?(tone)
      assert LazyHTML.query(document, "[data-role='status-text']") |> LazyHTML.text() == label

      if latency do
        assert LazyHTML.query(document, "[data-role='status-label'] [data-role='latency']") |> LazyHTML.text() |> String.trim() == "in 19.1s"
      else
        assert Enum.empty?(LazyHTML.query(document, "[data-role='latency']"))
      end
    end
  end

  test "status form accepts real states and the cancellation class but rejects arbitrary values" do
    for status <- ~w(accepted in_progress succeeded rejected) do
      {filters, values, errors} = RequestLogFilterForm.parse_filters(%{"status" => status}, nil, MapSet.new())
      assert filters == [status: status]
      assert values["status"] == status
      assert errors == []
      assert RequestLogFilterForm.query_params(values) == %{"status" => status}
    end

    assert {[status: "failed", client_cancelled: false], _, []} = RequestLogFilterForm.parse_filters(%{"status" => "failed"}, nil, MapSet.new())
    assert {[client_cancelled: true], _, []} = RequestLogFilterForm.parse_filters(%{"status" => "client_cancelled"}, nil, MapSet.new())

    for status <- ["cancelled", "not_a_status", "succeeded OR true"] do
      assert {[], _, [%{field: :status, message: "Status filter is not supported"}]} = RequestLogFilterForm.parse_filters(%{"status" => status}, nil, MapSet.new())
    end
  end
end
