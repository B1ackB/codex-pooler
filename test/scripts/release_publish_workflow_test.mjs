import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import {
	mkdirSync,
	mkdtempSync,
	readFileSync,
	rmSync,
	writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

const workflow = readFileSync(
	new URL("../../.github/workflows/build.yml", import.meta.url),
	"utf8",
);
const step = (name) => {
	const start = workflow.indexOf(`      - name: ${name}\n`);
	assert.ok(start >= 0, `missing workflow step: ${name}`);
	const end = workflow.indexOf("\n      - name:", start + 1);
	return workflow.slice(start, end < 0 ? workflow.length : end);
};
const script = (name) =>
	step(name)
		.split("        run: |\n")[1]
		.split("\n")
		.map((line) => line.replace(/^ {10}/, ""))
		.join("\n");

test("publication queues every pending release and recovery steps also run for reused images", () => {
	assert.match(
		workflow,
		/group: release-image-publication\n\s+queue: max\n\s+cancel-in-progress: false/,
	);
	for (const name of ["Prepare release assets", "Upload release assets"]) {
		assert.match(
			step(name),
			/if: steps\.publication\.outputs\.ready == 'true'\n/,
		);
		assert.doesNotMatch(step(name), /image_action|already_published/);
	}
	assert.doesNotMatch(
		step("Require verified revision and select monotonic release aliases"),
		/sleep |deadline=/,
	);
	assert.match(
		workflow,
		/ {2}image:\n {4}needs: select\n {4}if: needs\.select\.outputs\.ready == 'true' && needs\.select\.outputs\.image_action == 'build'\n/,
	);
	assert.match(
		step("Recover aliases from the existing immutable image"),
		/image_action == 'reuse'/,
	);
});

test("each platform is built natively and only the publish job tags the merged index", () => {
	assert.match(workflow, /runner: ubuntu-26\.04-arm/);
	assert.doesNotMatch(workflow, /setup-qemu-action/);
	const build = step("Build and push the platform image by digest");
	assert.match(build, /platforms: \$\{\{ matrix\.platform \}\}/);
	assert.match(build, /push-by-digest=true,name-canonical=true,push=true/);
	assert.match(
		build,
		/cache-to: type=gha,mode=max,scope=\$\{\{ matrix\.slug \}\},ignore-error=true/,
		"a cache export failure must not fail a publication",
	);
	assert.doesNotMatch(build, /\n\s+tags:/, "a platform image is never tagged");
	assert.match(workflow, / {2}publish:\n {4}needs: \[select, image\]\n/);
	const publish = workflow.slice(workflow.indexOf("  publish:\n"));
	assert.match(publish, /group: release-image-publication\n\s+queue: max/);
	assert.doesNotMatch(
		workflow.slice(0, workflow.indexOf("  publish:\n")),
		/imagetools create/,
		"only the publish job may tag",
	);
	const merge = step("Publish the multi-architecture image");
	assert.match(merge, /image_action == 'build'/);
	assert.match(merge, /Expected one digest per platform/);
	assert.match(merge, /imagetools create/);
});

test("waking the chart repository follows the image and never blocks publication", () => {
	const wake = step("Wake the Helm chart update");
	assert.match(wake, /if: steps\.publication\.outputs\.ready == 'true'\n/);
	assert.match(wake, /continue-on-error: true/);
	assert.match(wake, /HELM_WAKE_TOKEN/);
	assert.match(
		wake,
		/gh workflow run wake-renovate\.yml --repo icoretech\/helm/,
	);
	assert.ok(
		workflow.indexOf("- name: Wake the Helm chart update") >
			workflow.indexOf("- name: Upload release assets"),
		"the wake must come after the image and its release assets",
	);
});

test("the actual release packaging step produces a readable archive, checksum and digest receipt on recovery", (t) => {
	const root = mkdtempSync(join(tmpdir(), "release-assets-"));
	t.after(() => rmSync(root, { recursive: true, force: true }));
	mkdirSync(join(root, "scripts/self-host"), { recursive: true });
	for (const file of [
		"README.md",
		"docker-compose.yml",
		".env.example",
		"scripts/self-host/generate-env.sh",
		"scripts/self-host/totp-upgrade-preflight.sh",
		"scripts/self-host/totp-upgrade-preflight.exs",
	])
		writeFileSync(join(root, file), `sample ${file}\n`);
	const digest = `sha256:${"a".repeat(64)}`;
	execFileSync("bash", ["-c", script("Prepare release assets")], {
		cwd: root,
		env: {
			...process.env,
			VERSION: "1.2.3",
			RELEASE_TAG: "v1.2.3",
			IMAGE_DIGEST: digest,
			IMAGE_TAGS: "example/image:1.2.3\nexample/image:latest",
		},
	});
	const base = "codex-pooler-self-host-1.2.3";
	const files = execFileSync("tar", ["-tzf", `dist/${base}.tar.gz`], {
		cwd: root,
		encoding: "utf8",
	});
	assert.match(files, /scripts\/self-host\/generate-env.sh/);
	for (const extension of ["sh", "exs"]) {
		const file = `scripts/self-host/totp-upgrade-preflight.${extension}`;
		assert.ok(files.split("\n").includes(`${base}/${file}`));
		assert.equal(
			execFileSync("tar", ["-xOzf", `dist/${base}.tar.gz`, `${base}/${file}`], {
				cwd: root,
				encoding: "utf8",
			}),
			readFileSync(join(root, file), "utf8"),
		);
	}
	assert.match(files, /\.env.example/);
	execFileSync("sha256sum", ["-c", `${base}.tar.gz.sha256`], {
		cwd: join(root, "dist"),
	});
	const receipt = readFileSync(
		join(root, "dist/codex-pooler-image-digests-1.2.3.txt"),
		"utf8",
	);
	assert.ok(receipt.includes(`image_digest=${digest}`));
	assert.ok(receipt.includes("example/image:latest"));
});
