defmodule CodexPooler.Accounting.ClientIdentityTest do
  use ExUnit.Case, async: true

  alias CodexPooler.Accounting.ClientIdentity

  test "classification and normalized kind lookup share static display metadata" do
    for user_agent <- ["codex-tui/1.2.3", "Codex Desktop/1.2.3", "OpenAI/Python 1.2.3", "hermes/1.2.3", "litellm/1.2.3"] do
      identity = ClientIdentity.classify(user_agent)
      assert ClientIdentity.from_kind(identity.kind) == identity
      refute inspect(identity) =~ "1.2.3"
    end
  end

  test "unknown inputs cannot supply their own display metadata" do
    unknown = ClientIdentity.classify(nil)
    assert unknown.kind == "unknown"
    assert unknown.label == "Client"
    assert unknown.logo == nil

    for value <- [nil, 123, %{}, "unrecognized", "../../custom.svg"] do
      assert ClientIdentity.from_kind(value) == unknown
    end

    assert ClientIdentity.classify("custom-client/1.2.3 private-marker") == unknown
  end

  test "computer use belongs to the Codex family without exposing user agent details" do
    for user_agent <- ["codex-computer-use/1.2.3 (Sample OS; arm64) private-marker", " CODEX-COMPUTER-USE/1.2.3 "] do
      identity = ClientIdentity.classify(user_agent)
      assert identity == ClientIdentity.from_kind("codex_computer_use")
      assert %{label: "Codex Computer Use", logo: %{asset: "codex.svg", format: :svg}} = identity
      refute inspect(identity) =~ "private-marker"
      refute inspect(identity) =~ "1.2.3"
    end

    for user_agent <- ["other/codex-computer-use/1.2.3", "codex-computer-use-helper/1.2.3"] do
      assert ClientIdentity.classify(user_agent).kind == "unknown"
    end
  end

  test "the stock Home Assistant SDK user agent keeps SDK attribution" do
    identity = ClientIdentity.classify("AsyncOpenAI/Python 3.10.0")

    assert %{kind: "openai_python", label: "OpenAI Python SDK", logo: %{asset: "openai.svg"}} = identity
    assert ClientIdentity.from_kind(identity.kind) == identity
  end

  test "Observatory unknown clients expose bounded product and version with a generic icon" do
    assert %{label: "sample-app 1.2.3", icon: "hero-window", logo: nil} = ClientIdentity.observatory_identity("sample-app/1.2.3 (Sample OS) extra-details")
    assert ClientIdentity.observatory_identity(nil).label == "Unknown client"
    assert ClientIdentity.observatory_identity("  ").label == "Unknown client"
    assert String.length(ClientIdentity.compact_user_agent(String.duplicate("a", 100))) == 72
    assert ClientIdentity.compact_user_agent("sample\nagent") == "sample agent"
    assert ClientIdentity.compact_user_agent(<<255>>) == nil
  end
end
