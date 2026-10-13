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

  test "the stock Home Assistant SDK user agent keeps SDK attribution" do
    identity = ClientIdentity.classify("AsyncOpenAI/Python 3.10.0")

    assert %{kind: "openai_python", label: "OpenAI Python SDK", logo: %{asset: "openai.svg"}} = identity
    assert ClientIdentity.from_kind(identity.kind) == identity
  end
end
