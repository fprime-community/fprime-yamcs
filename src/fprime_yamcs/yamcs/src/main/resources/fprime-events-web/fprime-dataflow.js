/**
 * fprime-dataflow.js:
 *
 * Yamcs web extension providing the GDS-style data-flow orb: a floating
 * indicator visible on every yamcs-web page that turns into a green orb
 * while telemetry or events are flowing and into a red X once neither has
 * been seen for DATA_TIMEOUT_MS (mirroring the orb in the fprime-gds UI).
 *
 * Telemetry flow is detected from the processor's TM statistics stream
 * (received-packet count deltas); event flow from the event subscription.
 *
 * The <fprime-dataflow-orb> element is instantiated by the
 * <fprime-yamcs> initializer defined in fprime-events.js.
 */

// Matches the fprime-gds default (config_init.js dataTimeout: 5 seconds)
const DATA_TIMEOUT_MS = 5000;

const ORB_SIZE = 30;

const ORB_STYLE = `
  :host {
    position: fixed;
    right: 16px;
    bottom: 16px;
    z-index: 10000;
    display: block;
    width: ${ORB_SIZE}px;
    height: ${ORB_SIZE}px;
    font-family: Roboto, sans-serif;
    cursor: default;
  }
  .orb {
    width: 100%;
    height: 100%;
    border-radius: 50%;
    box-sizing: border-box;
    display: flex;
    align-items: center;
    justify-content: center;
    font-weight: bold;
    font-size: ${Math.round(ORB_SIZE * 0.6)}px;
    line-height: 1;
    user-select: none;
    color: #fff;
  }
  /* Green orb: data flowing (fprime-gds success.svg color) */
  .orb.flowing {
    background: #4caf50;
    box-shadow: 0 0 6px 2px rgba(76, 175, 80, 0.6);
    animation: fp-orb-pulse 2s ease-in-out infinite;
  }
  /* Red X: no data flow (fprime-gds error.svg) */
  .orb.stale {
    background: #ff0000;
    border-radius: 4px;
  }
  /* Grey: no instance selected, nothing to monitor */
  .orb.idle {
    background: #9e9e9e;
    opacity: 0.6;
  }
  @keyframes fp-orb-pulse {
    0%, 100% { box-shadow: 0 0 4px 1px rgba(76, 175, 80, 0.4); }
    50% { box-shadow: 0 0 10px 4px rgba(76, 175, 80, 0.8); }
  }
  .detail {
    position: absolute;
    right: 0;
    bottom: ${ORB_SIZE + 8}px;
    display: none;
    background: rgba(0, 0, 0, 0.85);
    color: #fff;
    font-size: 12px;
    border-radius: 4px;
    padding: 6px 10px;
    white-space: nowrap;
  }
  :host(:hover) .detail {
    display: block;
  }
`;

class FprimeDataflowOrbElement extends HTMLElement {
  constructor() {
    super();
    this._service = null;
    this._connectionSubscription = null;
    this._eventSubscription = null;
    this._statsSubscription = null;
    this._instance = null;
    this._context = null;
    this._flags = { telemetry: false, events: false };
    this._timeouts = { telemetry: null, events: null };
    this._receivedPackets = null;
  }

  set extensionService(service) {
    this._service = service;
    this.render();
    // Re-subscribe whenever the instance/processor context changes
    this._connectionSubscription = service.yamcs.connectionInfo$.subscribe(
      (info) => this.connect(info),
    );
  }

  disconnectedCallback() {
    if (this._connectionSubscription) {
      this._connectionSubscription.unsubscribe();
      this._connectionSubscription = null;
    }
    this.disconnect();
  }

  disconnect() {
    for (const subscription of [this._eventSubscription, this._statsSubscription]) {
      if (subscription) {
        subscription.cancel();
      }
    }
    this._eventSubscription = null;
    this._statsSubscription = null;
    for (const key in this._timeouts) {
      clearTimeout(this._timeouts[key]);
      this._timeouts[key] = null;
    }
  }

  connect(connectionInfo) {
    const instance = connectionInfo?.instance?.name || null;
    const processor = connectionInfo?.processor?.name || "realtime";
    const context = instance ? `${instance}/${processor}` : null;
    if (context === this._context) {
      return;
    }
    this.disconnect();
    this._instance = instance;
    this._context = context;
    this._flags = { telemetry: false, events: false };
    this._receivedPackets = null;
    if (!instance) {
      this.update();
      return;
    }
    const client = this._service.yamcs.yamcsClient;
    this._eventSubscription = client.createEventSubscription(
      { instance },
      () => this.bump("events"),
    );
    this._statsSubscription = client.createTMStatisticsSubscription(
      { instance, processor },
      (statistics) => this.processStatistics(statistics),
    );
    this.update();
  }

  /** Marks telemetry active when the total received-packet count grows */
  processStatistics(statistics) {
    let total = 0;
    for (const entry of statistics.tmstats || []) {
      total += Number(entry.receivedPackets || 0);
    }
    const previous = this._receivedPackets;
    this._receivedPackets = total;
    if (previous !== null && total > previous) {
      this.bump("telemetry");
    }
  }

  /** Marks a flow active, and schedules it stale after the data timeout */
  bump(key) {
    this._flags[key] = true;
    clearTimeout(this._timeouts[key]);
    this._timeouts[key] = setTimeout(() => {
      this._flags[key] = false;
      this.update();
    }, DATA_TIMEOUT_MS);
    this.update();
  }

  render() {
    const root = this.shadowRoot || this.attachShadow({ mode: "open" });
    root.innerHTML = "";

    const style = document.createElement("style");
    style.textContent = ORB_STYLE;
    root.appendChild(style);

    this._orb = document.createElement("div");
    this._orb.className = "orb";
    root.appendChild(this._orb);

    this._detail = document.createElement("div");
    this._detail.className = "detail";
    root.appendChild(this._detail);

    this.update();
  }

  update() {
    if (!this._orb) {
      return;
    }
    const flowing = this._flags.telemetry || this._flags.events;
    if (!this._instance) {
      this._orb.className = "orb idle";
      this._orb.textContent = "";
      this._detail.textContent = "F´ data flow: no instance selected";
    } else if (flowing) {
      this._orb.className = "orb flowing";
      this._orb.textContent = "";
      this._detail.textContent = this.detailText();
    } else {
      this._orb.className = "orb stale";
      this._orb.textContent = "\u2715";
      this._detail.textContent = this.detailText();
    }
  }

  detailText() {
    const describe = (active) => (active ? "flowing" : "none");
    return (
      `F´ data flow — telemetry: ${describe(this._flags.telemetry)}, ` +
      `events: ${describe(this._flags.events)}`
    );
  }
}

// Guarded: a stale bundle double-load must not throw on re-registration
if (!customElements.get("fprime-dataflow-orb")) {
  customElements.define("fprime-dataflow-orb", FprimeDataflowOrbElement);
}
