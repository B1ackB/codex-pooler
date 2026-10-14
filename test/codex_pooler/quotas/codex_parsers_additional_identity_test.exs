defmodule CodexPooler.Quotas.CodexParsersAdditionalIdentityTest do
  use ExUnit.Case, async: true

  alias CodexPooler.Quotas.{CapacityFacts, WindowClassifier}
  alias CodexPooler.Quotas.Evidence.CodexParsers

  @observed_at ~U[2026-08-25 10:00:00Z]

  for slot <- ["primary", "secondary"],
      {label, duration} <- [{"missing", :missing}, {"null", nil}, {"text", "bad"}, {"zero", 0}, {"negative", -1}, {"fractional", 18_000.5}, {"numeric string", "18000"}, {"maximum plus one", 31_536_001}, {"oversized", 9_223_372_036_854_775_808}] do
    test "legacy #{slot} #{label} duration cannot create quota evidence" do
      assert_invalid_legacy_duration(unquote(slot), unquote(duration))
    end
  end

  for {slot, duration, kind, minutes} <- [
        {"primary", 18_000, "primary", 300},
        {"secondary", 604_800, "secondary", 10_080},
        {"primary", 604_800, "secondary", 10_080},
        {"primary", 1, "primary", 1},
        {"primary", 61, "primary", 2},
        {"primary", 31_536_000, "primary", 525_600}
      ] do
    test "legacy #{slot} explicit #{duration} seconds preserves its duration" do
      payload = legacy_duration_payload(unquote(slot), unquote(duration))
      assert {:ok, %{windows: [], account_availability: nil}} = CodexParsers.parse_codex_usage_result(payload, @observed_at)
      assert [window] = CodexParsers.legacy_usage_windows_for_strict_result(payload, @observed_at)
      assert {window.window_kind, window.window_minutes} == {unquote(kind), unquote(minutes)}
      assert window.metadata["limit_window_seconds"] == unquote(duration)
      assert window.reset_at == DateTime.add(@observed_at, 900)
    end
  end

  test "legacy missing duration drops only the invalid window and additional meter" do
    payload = legacy_duration_payload("primary", 18_000)
    malformed = legacy_duration_payload("secondary", :missing)["rate_limit"]["secondary_window"]

    payload =
      payload
      |> put_in(["rate_limit", "secondary_window"], malformed)
      |> Map.put("additional_rate_limits", [%{"limit_name" => "sample-model", "metered_feature" => "sample_meter", "rate_limit" => %{"secondary_window" => malformed}}])

    assert {:ok, [window]} = CodexParsers.parse_codex_usage_payload(payload, @observed_at)
    assert {window.quota_key, window.window_kind, window.window_minutes} == {"account", "primary", 300}
    refute WindowClassifier.saved_reset_window?(window)
  end

  test "plan-bearing incomplete strict receipts do not borrow legacy duration evidence" do
    payload = Map.put(legacy_duration_payload("primary", 18_000), "plan_type", "sample_plan")
    assert {:ok, %{windows: [], account_availability: availability}} = CodexParsers.parse_codex_usage_result(payload, @observed_at)
    assert availability.basis == :conflict
    assert CodexParsers.legacy_usage_windows_for_strict_result(payload, @observed_at) == []
  end

  defp assert_invalid_legacy_duration(slot, duration) do
    payload = legacy_duration_payload(slot, duration)
    assert {:ok, strict} = CodexParsers.parse_codex_usage_result(payload, @observed_at)
    assert strict.windows == []
    assert strict.account_availability == nil
    refute CapacityFacts.authority_observed?(strict.capacity_facts)
    assert CodexParsers.legacy_usage_windows_for_strict_result(payload, @observed_at) == []
    assert {:error, %{code: :upstream_quota_unusable}} = CodexParsers.parse_codex_usage_payload(payload, @observed_at)
  end

  defp legacy_duration_payload(slot, duration) do
    window = %{"used_percent" => 25, "reset_at" => @observed_at |> DateTime.add(900) |> DateTime.to_unix()}
    window = if duration == :missing, do: window, else: Map.put(window, "limit_window_seconds", duration)
    %{"rate_limit" => %{"#{slot}_window" => window}}
  end

  test "windows-only compatibility accepts legacy selected windows while strict results reject them" do
    legacy_payload = %{
      "plan_type" => "sample_plan",
      "rate_limit" => %{
        "primary_window" => %{
          "used_percent" => 25,
          "limit_window_seconds" => 18_000,
          "reset_after_seconds" => 900
        }
      }
    }

    assert {:ok, [legacy_window]} =
             CodexParsers.parse_codex_usage_payload(legacy_payload, @observed_at)

    assert legacy_window.window_minutes == 300
    assert legacy_window.reset_at == DateTime.add(@observed_at, 900, :second)

    assert {:ok, %{windows: [], account_availability: availability}} =
             CodexParsers.parse_codex_usage_result(legacy_payload, @observed_at)

    assert availability.state == :unknown
    assert availability.basis == :conflict
    assert availability.account_windows == :unknown
  end

  test "same-label additional meters retain the shared legacy window identity" do
    assert {:ok, evidences} =
             CodexParsers.parse_codex_usage_payload(same_label_meter_payload(), @observed_at)

    assert length(evidences) == 2

    assert evidences
           |> MapSet.new(&{&1.quota_key, &1.window_kind, &1.window_minutes})
           |> MapSet.size() == 1
  end

  test "same-label additional meters retain distinct canonical identities" do
    assert {:ok, evidences} =
             CodexParsers.parse_codex_usage_payload(same_label_meter_payload(), @observed_at)

    assert Enum.map(evidences, &{&1.raw_limit_id, &1.raw_metered_feature, &1.used_percent}) == [
             {"meter_alpha", "meter_alpha", Decimal.new("31.0")},
             {"meter_beta", "meter_beta", Decimal.new("71.0")}
           ]
  end

  test "synthetic Reserve-shaped weekly payload preserves its wire identity without a model substitution" do
    payload = %{
      "additional_rate_limits" => [
        %{
          "limit_name" => "gpt-reserve",
          "metered_feature" => "base_model_inference",
          "rate_limit" => %{
            "primary_window" => %{
              "used_percent" => 25,
              "limit_window_seconds" => 604_800,
              "reset_after_seconds" => 604_800,
              "reset_at" => 1_778_000_000
            }
          }
        }
      ]
    }

    assert {:ok, [evidence]} = CodexParsers.parse_codex_usage_payload(payload, @observed_at)

    assert evidence.quota_key == "gpt_reserve"
    assert evidence.quota_scope == "model"
    assert evidence.model == "gpt-reserve"
    assert evidence.raw_limit_name == "gpt-reserve"
    assert evidence.raw_metered_feature == "base_model_inference"
    assert evidence.window_kind == "secondary"
    assert evidence.window_minutes == 10_080
  end

  test "exact duplicate meter identity deterministically retains highest pressure" do
    payload = %{
      "additional_rate_limits" => [
        additional_limit("meter_alpha", 83),
        additional_limit("meter_alpha", 19)
      ]
    }

    assert {:ok, [evidence]} = CodexParsers.parse_codex_usage_payload(payload, @observed_at)
    assert evidence.raw_metered_feature == "meter_alpha"
    assert Decimal.equal?(evidence.used_percent, Decimal.new("83.0"))
  end

  test "blank metered feature falls back to the trimmed raw limit id" do
    limit =
      "   "
      |> additional_limit(44)
      |> Map.put("limit_id", "  provider_meter_fallback  ")

    assert {:ok, [evidence]} =
             CodexParsers.parse_codex_usage_payload(
               %{"additional_rate_limits" => [limit]},
               @observed_at
             )

    assert evidence.raw_metered_feature == "provider_meter_fallback"
    assert evidence.raw_limit_id == "provider_meter_fallback"
    refute evidence.raw_limit_id == evidence.raw_limit_name
  end

  test "display label alone never becomes canonical meter identity" do
    limit = additional_limit("   ", 44)

    assert {:ok, [evidence]} =
             CodexParsers.parse_codex_usage_payload(
               %{"additional_rate_limits" => [limit]},
               @observed_at
             )

    assert evidence.raw_limit_name == "Shared weekly limit"
    assert evidence.raw_metered_feature == nil
    assert evidence.raw_limit_id == nil
  end

  # findings#238: a usage-body limit_name is a display label, bounded as
  # printable ASCII of at most 80 bytes; anything else is fingerprinted, never
  # erased, on the label, its identity part and the derived display label.
  test "a usage-body limit_name inside the label bound stays cleartext" do
    label = "Shared weekly limit (beta) v2.1"
    limit = "meter_clear" |> additional_limit(44) |> Map.put("limit_name", label)

    assert {:ok, [evidence]} =
             CodexParsers.parse_codex_usage_payload(
               %{"additional_rate_limits" => [limit]},
               @observed_at
             )

    assert evidence.raw_limit_name == label
    assert evidence.limit_name == label
    assert evidence.raw_metered_feature == "meter_clear"
  end

  test "a usage-body limit_name outside the label bound is fingerprinted" do
    overlong = "Shared weekly limit " <> String.duplicate("x", 80)
    control_characters = "Sharedweekly limit"

    for label <- [overlong, control_characters] do
      limit = "meter_bounded" |> additional_limit(44) |> Map.put("limit_name", label)

      assert {:ok, [evidence]} =
               CodexParsers.parse_codex_usage_payload(
                 %{"additional_rate_limits" => [limit]},
                 @observed_at
               )

      assert evidence.raw_limit_name == fingerprint(label)
      assert evidence.limit_name == fingerprint(label)
      assert evidence.raw_metered_feature == "meter_bounded"
      refute inspect(evidence) =~ "Shared"
    end
  end

  test "a label-only usage limit outside the bound keeps a fingerprinted identity" do
    overlong = "Label only meter " <> String.duplicate("y", 80)
    limit = "   " |> additional_limit(44) |> Map.put("limit_name", overlong)

    assert {:ok, [evidence]} =
             CodexParsers.parse_codex_usage_payload(
               %{"additional_rate_limits" => [limit]},
               @observed_at
             )

    assert evidence.raw_limit_name == fingerprint(overlong)
    refute inspect(evidence) =~ "Label only meter"
  end

  test "a rate-limit error limit_name takes the same label bound" do
    overlong = "Error dialect label " <> String.duplicate("z", 80)

    payload = %{
      "limit_name" => overlong,
      "metered_feature" => "meter_error",
      "reset_at" => 1_778_000_000,
      "window_minutes" => 300,
      "used_percent" => 50
    }

    assert [evidence] = CodexParsers.parse_rate_limit_error(payload, @observed_at)
    assert evidence.raw_limit_name == fingerprint(overlong)
    refute inspect(evidence) =~ "Error dialect label"

    assert [clear] =
             CodexParsers.parse_rate_limit_error(
               Map.put(payload, "limit_name", "Error dialect label"),
               @observed_at
             )

    assert clear.raw_limit_name == "Error dialect label"
  end

  # findings#240: a usage-body display_label takes the same printable-label
  # bound as limit_name before it becomes the operator-facing label.
  test "a usage-body display_label inside the label bound stays cleartext" do
    label = "Vendor label (beta) v2.1"
    limit = "meter_display_clear" |> additional_limit(44) |> Map.put("display_label", label)

    assert {:ok, [evidence]} =
             CodexParsers.parse_codex_usage_payload(
               %{"additional_rate_limits" => [limit]},
               @observed_at
             )

    assert evidence.display_label == label
    assert evidence.raw_limit_name == "Shared weekly limit"
    assert evidence.raw_metered_feature == "meter_display_clear"
  end

  test "a usage-body display_label outside the label bound is fingerprinted" do
    overlong = "Vendor label " <> String.duplicate("q", 80)
    control_characters = "Vendor label"

    for label <- [overlong, control_characters] do
      limit = "meter_display_bounded" |> additional_limit(44) |> Map.put("display_label", label)

      assert {:ok, [evidence]} =
               CodexParsers.parse_codex_usage_payload(
                 %{"additional_rate_limits" => [limit]},
                 @observed_at
               )

      assert evidence.display_label == fingerprint(label)
      assert evidence.raw_limit_name == "Shared weekly limit"
      assert evidence.raw_metered_feature == "meter_display_bounded"
      refute inspect(evidence) =~ "Vendor"
    end
  end

  # findings#240: with no limit_name, the meter's identity falls back to the
  # body's model, model_id or model_identifier. Those are provider-controlled
  # strings, so they take the model-identifier bound: an ASCII identifier of
  # at most 80 bytes stays cleartext, anything else is fingerprinted, never
  # erased, and a blank one is absent.
  test "a model fallback inside the identifier bound stays cleartext" do
    for {key, model} <- [
          {"model", "gpt-6.1-sol"},
          {"model_id", "gpt_6.astra:v2"},
          {"model_identifier", "example-org/model-1"}
        ] do
      limit =
        "meter_model_clear"
        |> additional_limit(44)
        |> Map.delete("limit_name")
        |> Map.put(key, model)

      assert {:ok, [evidence]} =
               CodexParsers.parse_codex_usage_payload(
                 %{"additional_rate_limits" => [limit]},
                 @observed_at
               )

      assert evidence.limit_name == model
      assert evidence.raw_limit_name == model
      assert evidence.model == model
      assert evidence.raw_metered_feature == "meter_model_clear"
    end
  end

  test "a model fallback outside the identifier bound is fingerprinted" do
    overlong = "gpt-leak" <> String.duplicate("x", 76)
    spaced = "gpt 5.6 terra"
    control_characters = "gpt- terra"

    # The display label derives from the same field, so the raw content must
    # not survive there in capitalized or split form either; hex fingerprints
    # cannot contain these markers.
    for {key, model, leak_marker} <- [
          {"model", overlong, ~r/leakx/i},
          {"model_id", spaced, ~r/5\.6 terra/i},
          {"model_identifier", control_characters, ~r/terra/i}
        ] do
      limit =
        "meter_model_bounded"
        |> additional_limit(44)
        |> Map.delete("limit_name")
        |> Map.put(key, model)

      assert {:ok, [evidence]} =
               CodexParsers.parse_codex_usage_payload(
                 %{"additional_rate_limits" => [limit]},
                 @observed_at
               )

      assert evidence.limit_name == fingerprint(model)
      assert evidence.raw_limit_name == fingerprint(model)
      assert evidence.model == fingerprint(model)
      assert evidence.raw_metered_feature == "meter_model_bounded"
      refute inspect(evidence) =~ model
      refute inspect(evidence) =~ leak_marker
    end
  end

  test "the model fallback keeps first-present precedence and treats a blank model as absent" do
    limit =
      "meter_model_precedence"
      |> additional_limit(44)
      |> Map.delete("limit_name")
      |> Map.merge(%{
        "model" => "   ",
        "model_id" => "gpt-precedence",
        "model_identifier" => "gpt-ignored"
      })

    assert {:ok, [evidence]} =
             CodexParsers.parse_codex_usage_payload(
               %{"additional_rate_limits" => [limit]},
               @observed_at
             )

    assert evidence.limit_name == "gpt-precedence"
    assert evidence.model == "gpt-precedence"
    refute inspect(evidence) =~ "gpt-ignored"
  end

  defp fingerprint(value) do
    "sha256_" <>
      (:crypto.hash(:sha256, value)
       |> Base.encode16(case: :lower)
       |> String.slice(0, 12))
  end

  defp same_label_meter_payload do
    %{
      "additional_rate_limits" => [
        additional_limit("meter_alpha", 31),
        additional_limit("meter_beta", 71)
      ]
    }
  end

  defp additional_limit(metered_feature, used_percent) do
    %{
      "limit_name" => "Shared weekly limit",
      "metered_feature" => metered_feature,
      "rate_limit" => %{
        "primary_window" => %{
          "used_percent" => used_percent,
          "limit_window_seconds" => 604_800,
          "reset_after_seconds" => 604_800,
          "reset_at" => 1_778_000_000
        }
      }
    }
  end
end
