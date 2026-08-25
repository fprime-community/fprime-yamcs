/**
 * fprime-events.js:
 *
 * Yamcs web extension providing a GDS-style F Prime event display: whole-row
 * severity coloring and filtering on event ID, name, severity, and time.
 *
 * The table is virtualized: only the rows that fit the viewport (plus a
 * small overscan) exist in the DOM, with spacer rows standing in for the
 * rest. Scrolling to the top pages older events in from the Yamcs archive
 * (infinite scroll-back); "Follow latest" keeps the view pinned to the
 * newest event.
 *
 * Two custom elements are defined:
 *  - <fprime-yamcs>: initializer element (named after the Yamcs plugin id),
 *    instantiated by yamcs-web at startup to register the sidebar item and
 *    mount the data-flow orb (see fprime-dataflow.js).
 *  - <fprime-events>: the event display page, mounted at /ext/fprime-events.
 */

const SEVERITIES = [
  "FATAL",
  "WARNING_HI",
  "WARNING_LO",
  "ACTIVITY_HI",
  "ACTIVITY_LO",
  "COMMAND",
  "DIAGNOSTIC",
];

// Color-blind-safe row colors (Okabe-Ito derived; red FATAL is safe since no
// green is used): warning family is a dark-to-light lightness ramp
const SEVERITY_COLORS = {
  FATAL: "#E03C31",
  WARNING_HI: "#E69F00",
  WARNING_LO: "#F0E442",
  ACTIVITY_HI: "#3D9FD9",
  ACTIVITY_LO: "#CCCCCC",
  COMMAND: "#C9B3E6",
  DIAGNOSTIC: "transparent",
};

// Fallback for events without fprime_severity extra (e.g. pre-existing archive)
const YAMCS_SEVERITY_FALLBACK = {
  CRITICAL: "FATAL",
  SEVERE: "FATAL",
  ERROR: "FATAL",
  DISTRESS: "WARNING_HI",
  WARNING: "WARNING_LO",
  WATCH: "ACTIVITY_HI",
  INFO: "ACTIVITY_LO",
};

const EVENT_SOURCE = "FPrimeEventProcessor";
const MAX_EVENTS = 50000;
const PAGE_LIMIT = 500;
const ROW_HEIGHT = 26;
const OVERSCAN = 10;

const PAGE_STYLE = `
  :host {
    display: block;
    height: 100%;
    font-family: Roboto, sans-serif;
    font-size: 12px;
    color: rgba(0, 0, 0, 0.87);
  }
  .fp-events {
    display: flex;
    flex-direction: column;
    height: 100%;
    padding: 8px 16px 16px 16px;
    box-sizing: border-box;
  }
  .fp-toolbar {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 8px;
    padding: 8px 0;
  }
  .fp-toolbar input[type="text"] {
    flex: 0 1 320px;
    padding: 5px 8px;
    border: 1px solid #ccc;
    border-radius: 3px;
    font-size: 12px;
  }
  .fp-severities {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 2px;
  }
  .fp-severities label {
    display: inline-flex;
    align-items: center;
    gap: 3px;
    border: 1px solid rgba(0, 0, 0, 0.2);
    border-radius: 3px;
    padding: 2px 6px;
    cursor: pointer;
    user-select: none;
  }
  .fp-time-range {
    display: inline-flex;
    align-items: center;
    gap: 4px;
  }
  .fp-time-range input {
    padding: 3px 4px;
    border: 1px solid #ccc;
    border-radius: 3px;
    font-size: 12px;
  }
  .fp-follow {
    display: inline-flex;
    align-items: center;
    gap: 3px;
    cursor: pointer;
    user-select: none;
    white-space: nowrap;
  }
  .fp-toolbar button {
    padding: 4px 10px;
    border: 1px solid #ccc;
    border-radius: 3px;
    background: #fff;
    cursor: pointer;
    font-size: 12px;
  }
  .fp-toolbar button:hover {
    background: #f0f0f0;
  }
  .fp-count {
    margin-left: auto;
    color: rgba(0, 0, 0, 0.54);
  }
  .fp-table-holder {
    overflow: auto;
    border: 1px solid rgba(0, 0, 0, 0.125);
  }
  table {
    width: 100%;
    border-collapse: collapse;
    table-layout: fixed;
  }
  th, td {
    border: 1px solid rgba(0, 0, 0, 0.125);
    padding: 0 8px;
    text-align: left;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
  }
  tbody tr {
    height: ${ROW_HEIGHT}px;
  }
  tr.fp-spacer, tr.fp-spacer td {
    border: none;
    padding: 0;
  }
  thead th {
    position: sticky;
    top: 0;
    background: #eee;
    z-index: 1;
    height: ${ROW_HEIGHT}px;
  }
  col.fp-col-time { width: 190px; }
  col.fp-col-id { width: 90px; }
  col.fp-col-name { width: 28%; }
  col.fp-col-severity { width: 100px; }
`;

/** Normalizes a Yamcs event into display fields */
function normalize(event) {
  const extra = event.extra || {};
  const severity =
    extra["fprime_severity"] ||
    YAMCS_SEVERITY_FALLBACK[event.severity] ||
    "ACTIVITY_LO";
  const idNumber = parseInt(extra["fprime_event_id"], 10);
  const id = isNaN(idNumber) ? "" : "0x" + idNumber.toString(16).toUpperCase().padStart(4, "0");
  const name = extra["fprime_event_name"] || event.type || "";
  // Strip the redundant "[EventName] " prefix that the processor prepends
  let message = event.message || "";
  const match = message.match(/^\[[^\]]*\] /);
  if (match && name) {
    message = message.substring(match[0].length);
  }
  return {
    key: `${event.generationTime}-${event.seqNumber}`,
    time: event.generationTime,
    id,
    idNumber: isNaN(idNumber) ? null : idNumber,
    name,
    severity,
    message,
  };
}

/** True if the event was published by the F Prime event processor */
function isFprimeEvent(event) {
  return event.source === EVENT_SOURCE || !!(event.extra || {})["fprime_event_id"];
}

class FprimeEventsElement extends HTMLElement {
  constructor() {
    super();
    this._events = [];
    this._keys = new Set();
    this._visible = [];
    this._filterText = "";
    this._enabledSeverities = new Set(SEVERITIES);
    this._subscription = null;
    this._service = null;
    this._follow = true;
    this._newestFirst = false;
    this._timeStart = null;
    this._timeStop = null;
    this._loadingOlder = false;
    this._archiveExhausted = false;
    this._renderedRange = [-1, -1];
  }

  set extensionService(service) {
    this._service = service;
    this.render();
    this.connect();
  }

  connectedCallback() {
    this._onResize = () => this.sizeHolder();
    window.addEventListener("resize", this._onResize);
    this.sizeHolder();
    if (this._service && !this._subscription) {
      this.connect();
    }
  }

  disconnectedCallback() {
    window.removeEventListener("resize", this._onResize);
    this.disconnect();
  }

  /** Pins the table to the viewport height so it scrolls internally */
  sizeHolder() {
    const holder = this._tableHolder;
    if (!holder) {
      return;
    }
    const top = holder.getBoundingClientRect().top;
    holder.style.height = `${Math.max(3 * ROW_HEIGHT, window.innerHeight - top - 16)}px`;
    this._renderedRange = [-1, -1];
    this.renderWindow();
    this.scrollIfFollowing();
  }

  disconnect() {
    if (this._subscription) {
      this._subscription.cancel();
      this._subscription = null;
    }
  }

  render() {
    const root = this.shadowRoot || this.attachShadow({ mode: "open" });
    root.innerHTML = "";

    const style = document.createElement("style");
    style.textContent = PAGE_STYLE;
    root.appendChild(style);

    const page = document.createElement("div");
    page.className = "fp-events";

    const toolbar = document.createElement("div");
    toolbar.className = "fp-toolbar";

    this._filterInput = document.createElement("input");
    this._filterInput.type = "text";
    this._filterInput.placeholder = "Filter by event ID, name, or message";
    this._filterInput.addEventListener("input", () => {
      this._filterText = this._filterInput.value.trim().toLowerCase();
      this.redraw();
    });
    toolbar.appendChild(this._filterInput);

    const makeTimeInput = (title, onChange) => {
      const input = document.createElement("input");
      input.type = "datetime-local";
      input.step = "1";
      input.title = title;
      input.addEventListener("change", () => {
        onChange(input.value ? new Date(input.value).toISOString() : null);
        this.redraw();
      });
      return input;
    };
    const timeRange = document.createElement("span");
    timeRange.className = "fp-time-range";
    timeRange.appendChild(document.createTextNode("From"));
    timeRange.appendChild(makeTimeInput("Show events at or after this time", (v) => (this._timeStart = v)));
    timeRange.appendChild(document.createTextNode("To"));
    timeRange.appendChild(makeTimeInput("Show events before this time", (v) => (this._timeStop = v)));
    toolbar.appendChild(timeRange);

    const severities = document.createElement("div");
    severities.className = "fp-severities";
    for (const severity of SEVERITIES) {
      const label = document.createElement("label");
      label.style.background = SEVERITY_COLORS[severity];
      const check = document.createElement("input");
      check.type = "checkbox";
      check.checked = true;
      check.addEventListener("change", () => {
        if (check.checked) {
          this._enabledSeverities.add(severity);
        } else {
          this._enabledSeverities.delete(severity);
        }
        this.redraw();
      });
      label.appendChild(check);
      label.appendChild(document.createTextNode(severity));
      severities.appendChild(label);
    }
    toolbar.appendChild(severities);

    const orderLabel = document.createElement("label");
    orderLabel.className = "fp-follow";
    const orderCheck = document.createElement("input");
    orderCheck.type = "checkbox";
    orderCheck.checked = this._newestFirst;
    orderCheck.addEventListener("change", () => {
      this._newestFirst = orderCheck.checked;
      this.redraw();
    });
    orderLabel.appendChild(orderCheck);
    orderLabel.appendChild(document.createTextNode("Newest at top"));
    toolbar.appendChild(orderLabel);

    const followLabel = document.createElement("label");
    followLabel.className = "fp-follow";
    this._followCheck = document.createElement("input");
    this._followCheck.type = "checkbox";
    this._followCheck.checked = this._follow;
    this._followCheck.addEventListener("change", () => {
      this._follow = this._followCheck.checked;
      this.scrollIfFollowing();
    });
    followLabel.appendChild(this._followCheck);
    followLabel.appendChild(document.createTextNode("Follow latest"));
    toolbar.appendChild(followLabel);

    const clearButton = document.createElement("button");
    clearButton.textContent = "Clear";
    clearButton.addEventListener("click", () => {
      this._events = [];
      this._keys.clear();
      this.redraw();
    });
    toolbar.appendChild(clearButton);

    this._countLabel = document.createElement("span");
    this._countLabel.className = "fp-count";
    toolbar.appendChild(this._countLabel);

    page.appendChild(toolbar);

    this._tableHolder = document.createElement("div");
    this._tableHolder.className = "fp-table-holder";
    // Scrolling away from the bottom releases follow; scrolling back
    // engages it. Nearing the top pages older events in from the archive.
    this._tableHolder.addEventListener("scroll", () => {
      const holder = this._tableHolder;
      const atNewestEdge = this._newestFirst
        ? holder.scrollTop <= 5
        : holder.scrollTop + holder.clientHeight >= holder.scrollHeight - 5;
      this._follow = atNewestEdge;
      this._followCheck.checked = this._follow;
      this.renderWindow();
      const nearOldestEdge = this._newestFirst
        ? holder.scrollTop + holder.clientHeight >= holder.scrollHeight - ROW_HEIGHT * OVERSCAN
        : holder.scrollTop < ROW_HEIGHT * OVERSCAN;
      if (nearOldestEdge) {
        this.loadOlder();
      }
    });

    const table = document.createElement("table");
    const colgroup = document.createElement("colgroup");
    for (const cls of ["time", "id", "name", "severity", "message"]) {
      const col = document.createElement("col");
      col.className = `fp-col-${cls}`;
      colgroup.appendChild(col);
    }
    table.appendChild(colgroup);
    const thead = document.createElement("thead");
    const headRow = document.createElement("tr");
    for (const column of ["Time", "ID", "Name", "Severity", "Message"]) {
      const th = document.createElement("th");
      th.textContent = column;
      headRow.appendChild(th);
    }
    thead.appendChild(headRow);
    table.appendChild(thead);
    this._tbody = document.createElement("tbody");
    table.appendChild(this._tbody);
    this._tableHolder.appendChild(table);
    page.appendChild(this._tableHolder);

    root.appendChild(page);
    this.sizeHolder();
    this.redraw();
  }

  async connect() {
    this.disconnect();
    const yamcs = this._service.yamcs;
    this._instance = yamcs.instance;
    this._client = yamcs.yamcsClient;

    this._subscription = this._client.createEventSubscription(
      { instance: this._instance },
      (event) => this.addEvents([event]),
    );

    await this.loadOlder();
  }

  /** Pages the next-older chunk of archived events in (infinite scroll-back) */
  async loadOlder() {
    if (this._loadingOlder || this._archiveExhausted || !this._client) {
      return;
    }
    this._loadingOlder = true;
    try {
      const options = {
        source: [EVENT_SOURCE],
        limit: PAGE_LIMIT,
        order: "desc",
      };
      if (this._events.length) {
        options.stop = this._events[0].time;
      }
      const page = await this._client.getEvents(this._instance, options);
      const before = this._events.length;
      this.addEvents(page);
      if (this._events.length === before) {
        this._archiveExhausted = true;
      }
    } catch (err) {
      console.error("fprime-events: archive load failed", err);
    } finally {
      this._loadingOlder = false;
    }
  }

  addEvents(events) {
    const holder = this._tableHolder;
    const oldVisibleCount = this._visible.length;
    const oldScrollTop = holder ? holder.scrollTop : 0;

    let added = false;
    let prepended = false;
    for (const event of events) {
      if (!isFprimeEvent(event)) {
        continue;
      }
      const normalized = normalize(event);
      if (this._keys.has(normalized.key)) {
        continue;
      }
      const last = this._events[this._events.length - 1];
      if (last && normalized.time.localeCompare(last.time) < 0) {
        prepended = true;
      }
      this._keys.add(normalized.key);
      this._events.push(normalized);
      added = true;
    }
    if (!added) {
      return;
    }
    if (prepended) {
      this._events.sort((a, b) => a.time.localeCompare(b.time));
    }
    // Cap the buffer; evict oldest only while following so scroll-back
    // reading is never yanked away
    if (this._follow && this._events.length > MAX_EVENTS) {
      const removed = this._events.splice(0, this._events.length - MAX_EVENTS);
      for (const item of removed) {
        this._keys.delete(item.key);
      }
    }
    this.redraw();
    // Keep the viewport anchored when older rows are inserted above it
    // (newest-at-top inserts older rows below, which needs no adjustment)
    if (prepended && holder && !this._follow && !this._newestFirst) {
      const addedAbove = this._visible.length - oldVisibleCount;
      holder.scrollTop = oldScrollTop + addedAbove * ROW_HEIGHT;
      this.renderWindow();
    }
  }

  matches(item) {
    // Unknown severities are never filtered out (they have no toggle)
    if (SEVERITIES.includes(item.severity) && !this._enabledSeverities.has(item.severity)) {
      return false;
    }
    if (this._timeStart && item.time < this._timeStart) {
      return false;
    }
    if (this._timeStop && item.time >= this._timeStop) {
      return false;
    }
    if (!this._filterText) {
      return true;
    }
    const haystack = [
      item.id,
      item.idNumber !== null ? String(item.idNumber) : "",
      item.name,
      item.message,
      item.severity,
    ]
      .join(" ")
      .toLowerCase();
    return haystack.includes(this._filterText);
  }

  buildRow(item) {
    const row = document.createElement("tr");
    row.style.background = SEVERITY_COLORS[item.severity] || "transparent";
    for (const value of [item.time, item.id, item.name, item.severity, item.message]) {
      const td = document.createElement("td");
      td.textContent = value;
      td.title = value;
      row.appendChild(td);
    }
    return row;
  }

  scrollIfFollowing() {
    if (this._follow && this._tableHolder) {
      this._tableHolder.scrollTop = this._newestFirst
        ? 0
        : this._tableHolder.scrollHeight;
      this.renderWindow();
    }
  }

  /** Recomputes the filtered list, then renders the visible window */
  redraw() {
    if (!this._tbody) {
      return;
    }
    this._visible = this._events.filter((item) => this.matches(item));
    if (this._newestFirst) {
      this._visible.reverse();
    }
    this._countLabel.textContent = `${this._visible.length} of ${this._events.length} events`;
    this._renderedRange = [-1, -1];
    this.renderWindow();
    this.scrollIfFollowing();
  }

  /** Places only the on-screen slice of rows in the DOM, between spacers */
  renderWindow() {
    const holder = this._tableHolder;
    const total = this._visible.length;
    const viewportRows = Math.ceil(holder.clientHeight / ROW_HEIGHT);
    let first = Math.floor(holder.scrollTop / ROW_HEIGHT) - OVERSCAN;
    first = Math.max(0, Math.min(first, total));
    let last = Math.min(total, first + viewportRows + 2 * OVERSCAN);
    if (first === this._renderedRange[0] && last === this._renderedRange[1]) {
      return;
    }
    this._renderedRange = [first, last];

    this._tbody.innerHTML = "";
    if (first > 0) {
      this._tbody.appendChild(this.buildSpacer(first));
    }
    for (let i = first; i < last; i++) {
      this._tbody.appendChild(this.buildRow(this._visible[i]));
    }
    if (last < total) {
      this._tbody.appendChild(this.buildSpacer(total - last));
    }
  }

  buildSpacer(rowCount) {
    const row = document.createElement("tr");
    row.className = "fp-spacer";
    const td = document.createElement("td");
    td.colSpan = 5;
    row.style.height = `${rowCount * ROW_HEIGHT}px`;
    row.appendChild(td);
    return row;
  }
}

class FprimeYamcsInitializer extends HTMLElement {
  set extensionService(service) {
    service.addNavItem("archive", {
      path: "ext/fprime-events",
      label: "F´ Events",
      icon: "event_note",
    });
    // Mount the GDS-style data-flow orb (defined in fprime-dataflow.js);
    // whenDefined guards against script load-order differences
    customElements.whenDefined("fprime-dataflow-orb").then(() => {
      if (!document.querySelector("fprime-dataflow-orb")) {
        const orb = document.createElement("fprime-dataflow-orb");
        orb.extensionService = service;
        document.body.appendChild(orb);
      }
    });
  }
}

// Guarded: a stale bundle double-load must not throw on re-registration
if (!customElements.get("fprime-events")) {
  customElements.define("fprime-events", FprimeEventsElement);
}
if (!customElements.get("fprime-yamcs")) {
  customElements.define("fprime-yamcs", FprimeYamcsInitializer);
}
