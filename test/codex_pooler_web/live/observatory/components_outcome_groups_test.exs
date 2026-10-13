defmodule CodexPoolerWeb.Observatory.ComponentsOutcomeGroupsTest do
  use ExUnit.Case, async: true

  import Phoenix.LiveViewTest

  alias CodexPooler.Accounting.ClientIdentity
  alias CodexPoolerWeb.Observatory.Components.{Activity, Telemetry}
  alias CodexPoolerWeb.Observatory.Presentation

  test "unknown client fallbacks render escaped text and keep different clients separate" do
    rows =
      Enum.map(["sample-a/1.0 (Sample OS)", "sample-b/2.0", "<script>alert(1)</script>", nil], fn agent ->
        %{endpoint_class: "models", total_tokens: 0, client: ClientIdentity.observatory_identity(agent)}
      end)

    fragment = rows |> Presentation.build_outcomes() |> render_outcomes()
    assert LazyHTML.query(fragment, "[data-role='observatory-outcome-row']") |> Enum.count() == 4
    assert LazyHTML.query(fragment, "[data-role='outcome-client']") |> Enum.map(&(LazyHTML.text(&1) |> String.trim())) == ["sample-a 1.0", "sample-b 2.0", "<script>alert(1)</script>", "Unknown client"]
    assert LazyHTML.query(fragment, "script") |> Enum.empty?()
  end

  test "model swatches center beside known model names and use a neutral fallback outside the chart" do
    outcomes =
      Presentation.build_outcomes([
        %{endpoint_class: "responses", model: "sample-model", model_display_name: "Sample Model"},
        %{endpoint_class: "responses", model: "Unknown model"},
        %{endpoint_class: "models", model: "sample-model"}
      ])

    assert hd(outcomes).model_color == "var(--admin-chart-other-models)"
    fragment = render_outcomes(outcomes)
    swatches = LazyHTML.query(fragment, "[data-role='model-swatch']")
    assert Enum.count(swatches) == 1
    assert LazyHTML.query(fragment, "[data-role='model-swatch'].self-center.size-2[aria-hidden='true']") |> Enum.count() == 1
  end

  test "recent models use human names with safe code fallback and retain utility placeholders" do
    outcomes =
      Presentation.build_outcomes([
        %{endpoint_class: "responses", model: "sample-model", model_display_name: "Sample Model"},
        %{endpoint_class: "responses", model: "sample-model", model_display_name: ""},
        %{endpoint_class: "models", model: "sample-model", model_display_name: "Sample Model"}
      ])

    assert render_outcomes(outcomes) |> LazyHTML.query("[data-role='outcome-model']") |> Enum.map(&LazyHTML.text/1) == ["Sample Model", "sample-model", "Not applicable"]
  end

  test "activity labels and model applicability survive presentation and rendering" do
    classes = ["models", "usage", "responses", "compaction", "files", "audio"]

    outcomes = Presentation.build_outcomes(Enum.map(classes, &%{endpoint_class: &1, model: "sample-model", reasoning_effort: "high", total_tokens: 0}))
    fragment = render_outcomes(outcomes)
    assert LazyHTML.query(fragment, "[data-label='Activity']") |> Enum.map(&LazyHTML.text/1) == ["Catalog", "Usage", "Generation", "Compaction", "Files", "Transcription"]
    assert LazyHTML.query(fragment, "[data-role='outcome-model']") |> Enum.map(&LazyHTML.text/1) == ["Not applicable", "Not applicable", "sample-model", "sample-model", "Not applicable", "sample-model"]
    assert LazyHTML.query(fragment, "[data-role='outcome-effort']") |> Enum.count() == 3
    assert LazyHTML.query(fragment, "[data-role='outcome-speed']") |> Enum.count() == 3
  end

  test "catalog groups preserve applicability while missing generation models remain separate" do
    outcomes = Presentation.build_outcomes(Enum.map(["models", "models", "usage", "responses", "responses"], &%{endpoint_class: &1, total_tokens: 0}))
    fragment = render_outcomes(outcomes)
    assert LazyHTML.query(fragment, "[data-role='observatory-outcome-row']") |> Enum.count() == 4
    assert LazyHTML.query(fragment, "[data-role='observatory-outcome-group']") |> LazyHTML.text() =~ "Not applicable · 2 requests"
    assert LazyHTML.query(fragment, "[data-role='outcome-model']") |> Enum.map(&LazyHTML.text/1) == ["Not applicable", "Unknown model", "Unknown model"]
  end

  test "consecutive requests without a model and without tokens fold into one row" do
    outcomes = [
      outcome("Unknown model", "other", 0),
      outcome("Unknown model", "other", 0),
      outcome("Unknown model", "other", 0),
      outcome("alpha-model", "responses", 10),
      outcome("Unknown model", "other", 0)
    ]

    fragment = render_outcomes(outcomes)
    rows = LazyHTML.query(fragment, "[data-role='observatory-outcome-row']")

    assert Enum.count(rows) == 3
    assert LazyHTML.query(fragment, "[data-role='observatory-outcome-group']") |> Enum.count() == 1
    assert LazyHTML.query(fragment, "[data-role='observatory-outcome-group']") |> LazyHTML.text() =~ "3 requests"
    assert rows |> Enum.at(1) |> LazyHTML.text() =~ "alpha-model"
    assert rows |> Enum.at(2) |> LazyHTML.text() =~ "Unknown model"
  end

  test "requests with a different endpoint or with tokens are not folded" do
    outcomes = [
      outcome("Unknown model", "other", 0),
      outcome("Unknown model", "files", 0),
      outcome("Unknown model", "files", 5)
    ]

    fragment = render_outcomes(outcomes)

    assert LazyHTML.query(fragment, "[data-role='observatory-outcome-row']") |> Enum.count() == 3
    assert LazyHTML.query(fragment, "[data-role='observatory-outcome-group']") |> Enum.empty?()
  end

  test "shows every loaded request after grouping without a fixed row cutoff" do
    outcomes =
      List.duplicate(outcome("Unknown model", "other", 0), 40) ++
        Enum.map(1..70, &outcome("sample-model-#{&1}", "responses", &1))

    fragment = render_outcomes(outcomes)
    rows = LazyHTML.query(fragment, "[data-role='observatory-outcome-row']") |> Enum.to_list()

    assert length(rows) == 71
    assert hd(rows) |> LazyHTML.text() =~ "40 requests"
    assert List.last(rows) |> LazyHTML.text() =~ "sample-model-70"

    attributed = render_outcomes(Enum.map(1..70, &outcome("sample-model-#{&1}", "responses", &1)))
    assert LazyHTML.query(attributed, "[data-role='outcome-model']") |> Enum.count() == 70
  end

  test "compact rows show total tokens, cache percentage and normalized client" do
    request =
      outcome("sample-model", "responses", 100)
      |> Map.merge(%{
        effort: "high",
        speed_level: 2,
        client: ClientIdentity.from_kind("opencode"),
        tokens: %{
          total: 100,
          label: "100",
          cache_percentage_label: "(75.0% cached)"
        }
      })

    fragment = render_outcomes([request])

    assert LazyHTML.query(fragment, "[data-role='outcome-speed'][data-speed-level='2']") |> LazyHTML.text() =~ "Fast (priority tier)"
    assert LazyHTML.query(fragment, "[data-role='outcome-effort']") |> LazyHTML.text() =~ "high"
    assert LazyHTML.query(fragment, "[data-role='outcome-tokens']") |> LazyHTML.text() == "100"
    assert LazyHTML.query(fragment, "[data-role='outcome-cached']") |> LazyHTML.text() |> String.trim() == "(75.0% cached)"
    assert LazyHTML.query(fragment, "[data-role='outcome-client'][data-client-kind='opencode']") |> LazyHTML.text() |> String.trim() == "opencode"
    assert LazyHTML.query(fragment, "[data-role='outcome-client'] .request-client-logo") |> Enum.count() == 1
  end

  test "requests from different clients remain distinct even without a model" do
    first = Map.put(outcome("Unknown model", "other", 0), :client, ClientIdentity.from_kind("codex"))
    second = Map.put(first, :client, ClientIdentity.from_kind("opencode"))
    assert render_outcomes([first, second]) |> LazyHTML.query("[data-role='observatory-outcome-row']") |> Enum.count() == 2
  end

  test "an unavailable trend renders no trend element" do
    html =
      render_component(&Telemetry.telemetry/1, %{
        overview: %{
          success_rate: %{
            measure: %{value: "90.0", unit: "%"},
            trend: %{label: "not available", tone: :neutral, direction: :unavailable}
          },
          cache_rate: %{
            measure: %{value: "25.0", unit: "%"},
            trend: %{label: "+10.0 pp", tone: :success, direction: :up}
          }
        },
        models: []
      })

    fragment = LazyHTML.from_fragment(html)

    assert LazyHTML.query(fragment, "#observatory-success-trend") |> Enum.empty?()
    assert LazyHTML.query(fragment, "#observatory-cache-trend[data-direction='up']") != []
    refute html =~ "not available"
  end

  defp render_outcomes(outcomes) do
    html =
      render_component(&Activity.activity/1, %{
        traffic: %{
          total_label: "0 tokens",
          chart: %{
            categories: "[]",
            series: "[]",
            units: "[]",
            value_kinds: "[]",
            yaxis: "[]",
            colors: "[]"
          },
          fallback: %{rows: [], total_label: "0 tokens"}
        },
        outcomes: outcomes,
        window: "7d"
      })

    LazyHTML.from_fragment(html)
  end

  defp outcome(model, endpoint, tokens) do
    %{
      timestamp: "Oct 02, 07:38:56",
      model: model,
      endpoint: endpoint,
      status: %{label: "Succeeded", tone: :success, data_status: "ok"},
      tokens: %{total: tokens, label: Integer.to_string(tokens)},
      cost: %{label: "-"}
    }
  end
end
