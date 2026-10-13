defmodule CodexPoolerWeb.Observatory.PresentationOutcomeDetailsTest do
  use ExUnit.Case, async: true

  alias CodexPooler.Accounting.ClientIdentity
  alias CodexPoolerWeb.Observatory.Presentation

  test "compact outcome tokens show cache as a percentage of input only" do
    [outcome] = Presentation.build_outcomes([token_row(47_400, 47_000, 46_342, 400)])

    assert outcome.tokens.label == "47.4k"
    assert outcome.tokens.cache_percentage_label == "98.6%"
    assert outcome.tokens.total == 47_400
  end

  test "zero cached input remains a known zero percentage" do
    [outcome] = Presentation.build_outcomes([token_row(100, 80, 0, 20)])
    assert outcome.tokens.cache_percentage_label == "0%"
  end

  for {scenario, row} <- [
        {"zero input", %{total_tokens: 20, input_tokens: 0, cached_input_tokens: 0, output_tokens: 20}},
        {"missing cached count", %{total_tokens: 100, input_tokens: 80, output_tokens: 20}},
        {"missing output count", %{total_tokens: 80, input_tokens: 80, cached_input_tokens: 60}},
        {"negative cached count", %{total_tokens: 100, input_tokens: 80, cached_input_tokens: -1, output_tokens: 20}},
        {"cached exceeds input", %{total_tokens: 100, input_tokens: 80, cached_input_tokens: 81, output_tokens: 20}},
        {"inconsistent total", %{total_tokens: 100, input_tokens: 80, cached_input_tokens: 60, output_tokens: 30}},
        {"noninteger count", %{total_tokens: 100, input_tokens: 80, cached_input_tokens: 60.0, output_tokens: 20}}
      ] do
    test "omits cache percentage for #{scenario}" do
      [outcome] = Presentation.build_outcomes([unquote(Macro.escape(row))])
      assert outcome.tokens.cache_percentage_label == nil
    end
  end

  test "page presentation caps a page at 200 rows and handles missing rows" do
    page = Presentation.build_outcomes(List.duplicate(token_row(1, 1, 0, 0), 201))

    assert length(page) == 200
    assert Presentation.build_outcomes(nil) == []
  end

  test "client display comes only from canonical client identity fields" do
    client = %{kind: "codex_desktop", label: "arbitrary label", icon: "arbitrary icon", logo: %{asset: "https://example.com/image.svg"}}
    [outcome] = Presentation.build_outcomes([%{client: client, user_agent: "arbitrary raw agent"}])

    assert outcome.client == ClientIdentity.from_kind("codex_desktop")
    assert outcome.client.label == "Codex Desktop"
    refute inspect(outcome) =~ "arbitrary"
    refute inspect(outcome) =~ "example.com"

    for client <- [nil, %{kind: "unknown-client"}, %{kind: %{malformed: true}}] do
      [unknown] = Presentation.build_outcomes([%{client: client}])
      assert unknown.client == %{ClientIdentity.from_kind(nil) | label: "Unknown client"}
    end
  end

  defp token_row(total, input, cached, output),
    do: %{total_tokens: total, input_tokens: input, cached_input_tokens: cached, output_tokens: output}
end
