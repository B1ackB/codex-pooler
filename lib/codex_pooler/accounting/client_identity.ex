defmodule CodexPooler.Accounting.ClientIdentity do
  @moduledoc """
  Normalized client identities with static labels and local icon metadata.

  Classification returns static metadata. Observatory additionally uses a
  bounded compact user-agent label for unrecognized clients.
  """

  @type css_class :: String.t() | [String.t() | false | nil]
  @type logo :: %{asset: String.t(), format: :svg | :png}
  @type classification :: %{
          kind: String.t(),
          label: String.t(),
          icon: String.t(),
          icon_class: css_class(),
          logo: logo() | nil
        }

  @patterns [
    {:prefix, "codex desktop/", "codex_desktop", "Codex Desktop", "hero-computer-desktop", :info},
    {:prefix, "codex-computer-use/", "codex_computer_use", "Codex Computer Use", "hero-computer-desktop", :info},
    {:custom, :codex_cli, "codex", "Codex", "hero-command-line", :info},
    {:contains, ["opencode"], "opencode", "opencode", "hero-code-bracket-square", :primary},
    {:contains, ["openclaw"], "openclaw", "OpenClaw", "hero-bolt", :primary},
    {:contains, ["hermes"], "hermes", "Hermes Agent", "hero-paper-airplane", :primary},
    {:prefix, "windmill/", "windmill", "Windmill", "hero-arrow-path-rounded-square", :primary},
    {:contains, ["aider"], "aider", "Aider", "hero-pencil-square", :primary},
    {:contains, ["continue"], "continue", "Continue", "hero-arrow-path-rounded-square", :primary},
    {:contains, ["cline"], "cline", "Cline", "hero-command-line", :primary},
    {:contains, ["goose"], "goose", "Goose", "hero-sparkles", :primary},
    {:prefix, "deepseek-harness/", "deepseek_harness", "DeepSeek Harness", "hero-sparkles", :primary},
    {:prefix, "omp/", "omp", "Oh My Pi", "hero-command-line", :primary},
    {:prefix, "pi/", "pi", "Pi", "hero-command-line", :primary},
    {:prefix, "cursor/", "cursor", "Cursor", "hero-cursor-arrow-rays", :primary},
    {:prefix, "kilo-code/", "kilo", "Kilo Code", "hero-code-bracket-square", :primary},
    {:prefix, "kilocode/", "kilo", "Kilo Code", "hero-code-bracket-square", :primary},
    {:prefix, "openhands/", "openhands", "OpenHands", "hero-command-line", :primary},
    {:prefix, "trae/", "trae", "Trae", "hero-command-line", :primary},
    {:prefix, "litellm/", "litellm", "LiteLLM", "hero-server-stack", :neutral},
    {:prefix, "openai/python", "openai_python", "OpenAI Python SDK", "hero-code-bracket", :success},
    {:prefix, "asyncopenai/python", "openai_python", "OpenAI Python SDK", "hero-code-bracket", :success},
    {:prefix, "openai/js", "openai_node", "OpenAI Node SDK", "hero-cube", :success},
    {:custom, :vercel_ai_sdk, "vercel_ai_sdk", "Vercel AI SDK", "hero-sparkles", :success},
    {:custom, :python_runtime, "python", "Python", "hero-code-bracket", :warning},
    {:custom, :node_runtime, "node", "Node.js", "hero-cube", :warning},
    {:prefix, "curl/", "curl", "curl", "hero-command-line", :neutral},
    {:custom, :elixir_http, "elixir_http", "Elixir HTTP", "hero-beaker", :neutral},
    {:prefix, "codex-pooler-", "codex_pooler", "Codex Pooler", "hero-cube-transparent", :neutral}
  ]

  @logo_names %{
    "codex_desktop" => "codex",
    "codex_computer_use" => "codex",
    "codex" => "codex",
    "opencode" => "opencode",
    "openclaw" => "openclaw",
    "hermes" => "hermesagent",
    "windmill" => "windmill",
    "aider" => "aider",
    "continue" => "continue",
    "cline" => "cline",
    "goose" => "goose",
    "deepseek_harness" => "deepseek-harness",
    "omp" => "omp",
    "pi" => "pi",
    "cursor" => "cursor",
    "kilo" => "kilocode",
    "openhands" => "openhands",
    "trae" => "trae",
    "litellm" => "litellm",
    "openai_python" => "openai",
    "openai_node" => "openai",
    "vercel_ai_sdk" => "vercel",
    "python" => "python",
    "node" => "nodedotjs",
    "curl" => "curl",
    "elixir_http" => "elixir",
    "codex_pooler" => "codex-pooler"
  }

  @spec classify(String.t() | nil) :: classification()
  def classify(user_agent) when is_binary(user_agent) do
    normalized = user_agent |> String.trim() |> String.downcase()

    Enum.find_value(@patterns, client("unknown", "Client", "hero-window", :neutral), fn pattern ->
      classify_pattern(pattern, normalized)
    end)
  end

  def classify(nil), do: client("unknown", "Client", "hero-window", :neutral)

  @spec compact_user_agent(term()) :: String.t() | nil
  def compact_user_agent(value) when is_binary(value) do
    if String.valid?(value) do
      value = value |> String.replace(~r/[[:cntrl:]]/u, " ") |> String.trim()

      compact =
        case Regex.run(~r/^([^\/\(]+)\/([^\s\(]+)(?:\s+\(([^)]*)\))?/, value) do
          [_match, product, version | _ignored] -> String.trim(product) <> " " <> String.trim(version)
          _ -> value
        end

      cond do
        compact == "" -> nil
        String.length(compact) > 72 -> String.slice(compact, 0, 69) <> "..."
        true -> compact
      end
    end
  end

  def compact_user_agent(_value), do: nil

  @spec observatory_identity(String.t() | nil) :: classification()
  def observatory_identity(user_agent) do
    case classify(user_agent) do
      %{kind: "unknown"} = identity -> %{identity | label: compact_user_agent(user_agent) || "Unknown client"}
      identity -> identity
    end
  end

  @spec from_kind(term()) :: classification()
  def from_kind(kind) do
    Enum.find_value(@patterns, client("unknown", "Client", "hero-window", :neutral), fn
      {_match, _pattern, ^kind, label, icon, tone} -> client(kind, label, icon, tone)
      _other -> nil
    end)
  end

  defp classify_pattern({:prefix, prefix, kind, label, icon, tone}, user_agent) do
    if String.starts_with?(user_agent, prefix), do: client(kind, label, icon, tone)
  end

  defp classify_pattern({:contains, needles, kind, label, icon, tone}, user_agent) do
    if Enum.any?(needles, &String.contains?(user_agent, &1)), do: client(kind, label, icon, tone)
  end

  defp classify_pattern({:custom, name, kind, label, icon, tone}, user_agent) do
    if custom_match?(name, user_agent), do: client(kind, label, icon, tone)
  end

  defp client(kind, label, icon, tone) do
    %{
      kind: kind,
      label: label,
      icon: icon,
      icon_class: ["size-3.5 shrink-0", icon_tone_class(tone)],
      logo: logo_for(kind)
    }
  end

  defp custom_match?(:codex_cli, user_agent) do
    Enum.any?(
      ["codex cli/", "codex-tui/", "codex_exec/", "codex_cli_rs/", "codex_vscode/", "codex-chrome-extension-sidepanel/"],
      &String.starts_with?(user_agent, &1)
    )
  end

  defp custom_match?(:vercel_ai_sdk, user_agent) do
    String.starts_with?(user_agent, "ai/") and
      (String.contains?(user_agent, "ai-sdk") or String.contains?(user_agent, "runtime/node.js"))
  end

  defp custom_match?(:python_runtime, user_agent) do
    String.starts_with?(user_agent, "python-requests/") or
      String.starts_with?(user_agent, "python-urllib/")
  end

  defp custom_match?(:node_runtime, user_agent),
    do: user_agent == "node" or String.starts_with?(user_agent, "node/")

  defp custom_match?(:elixir_http, user_agent),
    do: String.starts_with?(user_agent, "mint/") or String.starts_with?(user_agent, "req/")

  defp logo_for(kind) do
    case Map.get(@logo_names, kind) do
      nil -> nil
      "litellm" -> %{asset: "litellm-32.png", format: :png}
      name -> %{asset: "#{name}.svg", format: :svg}
    end
  end

  defp icon_tone_class(:info), do: "text-info"
  defp icon_tone_class(:primary), do: "text-primary"
  defp icon_tone_class(:success), do: "text-success"
  defp icon_tone_class(:warning), do: "text-warning"
  defp icon_tone_class(_tone), do: "text-base-content/45"
end
