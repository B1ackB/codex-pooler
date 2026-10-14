defmodule CodexPoolerWeb.Telemetry.LifecycleAuthorizationTelemetryTest do
  use ExUnit.Case, async: false

  alias CodexPoolerWeb.Telemetry

  @event [:codex_pooler, :gateway, :lifecycle_authorization]
  @count_name [:codex_pooler, :gateway, :lifecycle_authorization, :count]
  @duration_name [:codex_pooler, :gateway, :lifecycle_authorization, :duration, :seconds]

  test "declares bounded lifecycle authorization counter and duration metrics" do
    count = metric(@count_name)
    duration = metric(@duration_name)

    assert count.event_name == @event
    assert count.measurement == :count
    assert count.tags == [:phase, :operation, :transport, :outcome]

    assert duration.event_name == @event
    assert is_function(duration.measurement, 1)
    assert duration.measurement.(%{duration_ms: 7}) == 0.007
    assert duration.unit == :second
    assert duration.tags == [:phase, :operation, :transport, :outcome]
  end

  test "sanitizes unknown and high-cardinality tag values" do
    metric = metric(@count_name)

    assert metric.tag_values.(%{
             phase: "lifecycle_authorization",
             operation: "writer_delivery",
             transport: "websocket",
             outcome: "authorized"
           }) == %{
             phase: "lifecycle_authorization",
             operation: "writer_delivery",
             transport: "websocket",
             outcome: "authorized"
           }

    assert metric.tag_values.(%{
             phase: "request-4711",
             operation: "writer_delivery:#{String.duplicate("x", 120)}",
             transport: "http",
             outcome: "provider-secret"
           }) == %{
             phase: "unknown",
             operation: "unknown",
             transport: "unknown",
             outcome: "unknown"
           }
  end

  test "emits metadata-only count and duration samples" do
    registry = :lifecycle_authorization_metric_test
    metrics = [metric(@count_name), metric(@duration_name)]
    start_supervised!({TelemetryMetricsPrometheus.Core, metrics: metrics, name: registry, start_async: false})

    assert :ok = Telemetry.emit_lifecycle_authorization(:internal_delivery, :stale_generation, System.monotonic_time())

    body = TelemetryMetricsPrometheus.Core.scrape(registry)

    assert body =~ ~r/codex_pooler_gateway_lifecycle_authorization_count\{[^}]*phase="lifecycle_authorization"/
    assert body =~ ~r/operation="internal_delivery"/
    assert body =~ ~r/transport="websocket"/
    assert body =~ ~r/outcome="stale_generation"/
    assert body =~ "codex_pooler_gateway_lifecycle_authorization_duration_seconds_count"
    refute body =~ "request_id"
    refute body =~ "session_id"
    refute body =~ "pool_id"
  end

  defp metric(name), do: Enum.find(Telemetry.prometheus_metrics(), &(&1.name == name))
end
