import assert from "node:assert/strict";
import test from "node:test";
import { ObservatoryToolbar } from "./observatory_toolbar.mjs";

test("toolbar height follows resize and cleans up outside LiveView patches", (t) => {
	let callback;
	let disconnected = false;
	const original = globalThis.ResizeObserver;
	t.after(() => {
		globalThis.ResizeObserver = original;
	});
	globalThis.ResizeObserver = class {
		constructor(handler) {
			callback = handler;
		}
		observe() {}
		disconnect() {
			disconnected = true;
		}
	};
	let height = 48;
	const properties = new Map();
	const hook = {
		el: {
			ownerDocument: {
				documentElement: {
					style: {
						setProperty: (key, value) => properties.set(key, value),
						removeProperty: (key) => properties.delete(key),
					},
				},
			},
			getBoundingClientRect: () => ({ height }),
		},
	};
	ObservatoryToolbar.mounted.call(hook);
	assert.equal(properties.get("--observatory-toolbar-height"), "48px");
	height = 85;
	callback();
	assert.equal(properties.get("--observatory-toolbar-height"), "85px");
	ObservatoryToolbar.destroyed.call(hook);
	assert.equal(disconnected, true);
	assert.equal(properties.size, 0);
});
