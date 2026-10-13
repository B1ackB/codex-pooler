export const ObservatoryToolbar = {
	mounted() {
		this.observatoryRoot = this.el.ownerDocument.documentElement;
		this.updateToolbarHeight = () => {
			this.observatoryRoot.style.setProperty(
				"--observatory-toolbar-height",
				`${this.el.getBoundingClientRect().height}px`,
			);
		};
		this.toolbarObserver = new ResizeObserver(this.updateToolbarHeight);
		this.toolbarObserver.observe(this.el);
		this.updateToolbarHeight();
	},
	updated() {
		this.updateToolbarHeight();
	},
	destroyed() {
		this.toolbarObserver.disconnect();
		this.observatoryRoot.style.removeProperty("--observatory-toolbar-height");
	},
};
