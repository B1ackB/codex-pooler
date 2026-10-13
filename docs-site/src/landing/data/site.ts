export const SITE_URL = "https://www.codex-pooler.com";
export const REPO_URL = "https://github.com/icoretech/codex-pooler";
export const RELEASES_URL = `${REPO_URL}/releases`;
export const LICENSE_URL = `${REPO_URL}/blob/main/LICENSE.md`;
export const HELM_URL =
	"https://github.com/icoretech/helm/tree/main/charts/codex-pooler";
export const X_URL = "https://x.com/icoretech_inc";
export const REDDIT_URL = "https://reddit.com/r/CodexPooler";

// The docs are served under /docs on this host. Keep every docs link behind this helper.
export const docs = (path = "/") => `/docs${path}`;
export const DOCS_URL = docs("/");

export const QUICKSTART_URL = docs("/getting-started/quick-start/");

export interface Tool {
	/** File name under public/logos, without extension. */
	logo: string;
	name: string;
	guide: string;
}

// The tools with a public setup guide, in the order the README presents them.
export const TOOLS: Tool[] = [
	{
		logo: "codex",
		name: "Codex CLI & Desktop",
		guide: docs("/clients/codex-cli-desktop/"),
	},
	{ logo: "opencode", name: "OpenCode", guide: docs("/clients/opencode/") },
	{ logo: "openclaw", name: "OpenClaw", guide: docs("/clients/openclaw/") },
	{
		logo: "hermesagent",
		name: "Hermes Agent",
		guide: docs("/clients/hermes/"),
	},
	{ logo: "pi", name: "Pi", guide: docs("/clients/pi/") },
	{ logo: "omp", name: "OMP", guide: docs("/clients/omp/") },
	{ logo: "omo", name: "OMO", guide: docs("/clients/omo-native-senpi/") },
	{ logo: "cursor", name: "Cursor", guide: docs("/clients/cursor/") },
	{ logo: "kilocode", name: "Kilo Code", guide: docs("/clients/kilo-code/") },
	{ logo: "trae", name: "Trae", guide: docs("/clients/trae/") },
	{ logo: "aider", name: "Aider", guide: docs("/clients/aider/") },
	{ logo: "continue", name: "Continue", guide: docs("/clients/continue/") },
	{ logo: "cline", name: "Cline", guide: docs("/clients/cline/") },
	{ logo: "goose", name: "Goose", guide: docs("/clients/goose/") },
	{
		logo: "deepseek-harness",
		name: "DeepSeek Harness",
		guide: docs("/clients/deepseek-harness/"),
	},
	{ logo: "windmill", name: "Windmill", guide: docs("/clients/windmill/") },
	{ logo: "openhands", name: "OpenHands", guide: docs("/clients/openhands/") },
	{
		logo: "python",
		name: "OpenAI Python",
		guide: docs("/clients/openai-compatible/"),
	},
	{
		logo: "nodedotjs",
		name: "OpenAI Node",
		guide: docs("/clients/openai-compatible/"),
	},
	{
		logo: "vercel",
		name: "Vercel AI SDK",
		guide: docs("/clients/openai-compatible/"),
	},
	{
		logo: "home-assistant",
		name: "Home Assistant",
		guide: docs("/clients/home-assistant/"),
	},
];
