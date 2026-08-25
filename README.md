# fprime-yamcs: A YAMCS to F Prime Bridge Package

fprime-yamcs is designed to run YAMCS as the ground system when working with fprime. It operates similar to fprime-gds where it launches YAMCS in-lieu of the fprime-gds data pipelines.

## Requirements

`fprime-yamcs` requires the users to have `mvn` installed. See: [https://maven.apache.org/](https://maven.apache.org/).

> [!CAUTION]
> `mvn` requires JDK to be installed

## Usage

Install this package and run `fprime-yamcs` on a compatible F Prime deployment.

## fprime-yamcs-events: Event Processor

`fprime-yamcs-events` runs the F Prime event processor standalone: it reads the F Prime JSON topology dictionary and publishes F Prime events into YAMCS. It is launched automatically by `fprime-yamcs`; run it directly when operating YAMCS without the full `fprime-yamcs` launcher.

### F´ Events Web Display

The YAMCS web interface gains an **F´ Events** page (sidebar item, served at `/ext/fprime-events`) providing the event display F Prime developers know from `fprime-gds`:

- Whole-row colors by F Prime severity (FATAL, WARNING_HI, WARNING_LO, ACTIVITY_HI, ACTIVITY_LO, COMMAND, DIAGNOSTIC), using a color-blind-safe palette derived from Okabe-Ito.
- Filtering by event ID (hex or decimal), event name, message text, severity (per-severity toggles), and time range.
- A virtualized table (only on-screen rows are rendered) with infinite scroll-back into the YAMCS event archive, plus a "Follow latest" toggle that keeps the view pinned to the newest event.

This works because the event processor publishes each event with structured `extra` fields (`fprime_severity`, `fprime_event_id`, `fprime_event_name`) preserving the full 7-level F Prime severity set, which YAMCS's native 5-level severity model cannot represent. The page is registered by the `FprimeEventsWebExtension` YAMCS plugin bundled with the YAMCS project that `fprime-yamcs` builds; no additional configuration is required. Events published by older versions of the event processor (without the `extra` fields) are shown with a best-effort severity derived from the YAMCS severity.

### Data-Flow Orb

Every YAMCS web page also gains the data-flow indicator F Prime developers know from the `fprime-gds` main screen: a floating orb (bottom-right corner) that glows green while telemetry or events are flowing and turns into a red X once neither has been seen for 5 seconds (the same timeout `fprime-gds` uses). Hovering the orb shows a per-source breakdown (telemetry vs. events); the orb is grey while no YAMCS instance is selected.

Telemetry flow is detected from the selected processor's TM statistics stream (received-packet count deltas), and event flow from the instance's event stream, so the orb reflects live downlink activity regardless of which page is open.

## fprime-yamcs-tlmchan: Telemetry Channel Splitter

`Svc.TlmChan` packs multiple (id, time, value) telemetry channel records into a single downlinked packet, but the generated XTCE models one channel per packet keyed on the first channel id — so YAMCS alone only decodes the first record of each packet.

`fprime-yamcs-tlmchan` closes this gap: it reads the F Prime JSON topology dictionary, subscribes to the aggregate `FPrimeTelemetryChannel` container, walks every record in each packet, and re-injects each record beyond the first as a standalone single-record space packet on a dedicated UDP telemetry data link (`UDP_TM_SPLIT_IN`, default port 50002, configurable via `--udp-tm-inject-port` / `FPRIME_YAMCS_TM_INJECT_PORT`). YAMCS then decodes each injected packet through its normal XTCE path, publishing every channel as a parameter. It is launched automatically by `fprime-yamcs`; run it directly when operating YAMCS without the full `fprime-yamcs` launcher.

## fprime-yamcs-comm: Communication Bridge

`fprime-yamcs-comm` bridges bidirectional communication between an F Prime endpoint and the YAMCS UDP intake/outlet:

- The endpoint side is reached through an F Prime GDS **communication adapter plugin** (`--communication-selection`: `uart`, `ip`, or any installed adapter plugin).
- The YAMCS side pushes deframed packets as UDP datagrams to the telemetry intake (`--tm-host`/`--tm-port`, default `127.0.0.1:50000`) and receives command datagrams on a local UDP port (`--tc-host`/`--tc-port`, default `127.0.0.1:50001`). Command datagrams are only accepted from the TM host, loopback (`127.0.0.1`), and any hosts supplied via `--tc-allowed-source`; hostnames are resolved to IPv4 addresses once at startup and compared against the datagram source IP.
- One stage of framing/deframing sits in between, provided by an F Prime GDS **framing plugin** (`--framing-selection`). The default is the packaged `no-op` framer/deframer, which passes data through unchanged since YAMCS nominally performs framing/deframing itself. Select `fprime` to apply the standard F Prime framing (start word, length, data, checksum) on the endpoint side.

> [!NOTE]
> The UDP-transport requirement described under [Caveats](#caveats) applies to connecting F Prime directly to YAMCS; `fprime-yamcs-comm` lifts it by bridging non-UDP endpoints (e.g. UART) to the YAMCS UDP links.

> [!WARNING]
> With `no-op` framing over a stream-oriented adapter (`uart`, `ip`), packet boundaries depend on read timing: packets may be split or merged across UDP datagrams. Use a boundary-recovering framing plugin (e.g. `--framing-selection fprime`) unless the endpoint stream carries self-delimiting data that YAMCS deframes. The bridge warns on startup for the built-in stream adapters only; third-party stream adapters are not detected.

Operational notes: the bridge exits with a non-zero code if either data pump fails abnormally, so supervisors can detect and restart it; buffered downlink data that the framing plugin cannot deframe is discarded (with a warning) once it exceeds ten maximum-size datagrams (~640 KB).

Example, bridging a UART device to YAMCS with F Prime framing recovering packet boundaries (all UDP flags shown use their default values):

```
fprime-yamcs-comm --communication-selection uart --uart-device /dev/ttyUSB0 --uart-baud 115200 \
    --framing-selection fprime --tm-host 127.0.0.1 --tm-port 50000 --tc-port 50001
```

```mermaid
flowchart LR
    subgraph COMM["fprime-yamcs-comm"]
        ADPT["Comm Adapter Plugin<br/>(--communication-selection)"]
        FRAME["Framing Plugin<br/>(--framing-selection, default no-op)"]
        UDP["YAMCS UDP Endpoints<br/>(TM out / TC in)"]
        ADPT <--> FRAME
        FRAME <--> UDP
    end
    EP["F´ Endpoint<br/>(UART, IP, ...)"] <--> ADPT
    UDP <--> YAMCS["YAMCS UDP intake/outlet"]
```

### Testing

The bridge's integration tests (`tests/test_comm_bridge.py`) require `socat` to emulate a UART endpoint; without it only the unit tests run (the integration tests are skipped). CI environments running these tests should install `socat`.

## Configuration 

YAMCS is powerful and has many configuration properties. `fprime-yamcs` requires one instance of YAMCS defined in the configuration to have the following MDB:

```
mdb:
   - type: xtce
     args:
        file: .../fprime.xtce.xml
```

This is to allow for automatic dictionary generation. Users declining this service must specify: `--no-convert-dictionary`.

## Web Extensions

Projects may extend the YAMCS web interface with their own JavaScript:

```sh
fprime-yamcs --yamcs-web-extension-dirs path/to/extension-dir ...
```

Every top-level `.js` file in each directory is loaded as a module script by
the YAMCS web interface, and the directory's files are served alongside the
webapp's static files. Paths must not contain commas or whitespace.

## Caveats

Currently, the default configuration of YAMCS requires F Prime to connect a CCSDS TC/TM framer/deframer to the Drv.Udp component ensuring that UDP is the transport mechanism.

```mermaid id="th4eai"
flowchart LR
    subgraph FPRIME["F´"]
        FPD["F´ Dictionary<br/>(JSON topology dictionary)"]
    end

    subgraph OUTER["fprime-yamcs CLI"]
        subgraph FY["fprime-yamcs"]
            XTCEC["XTCE Converter<br/>(fprime-xtce)"]
            EVENTS["F Prime Event Processor"]
            BASECFG["Standard Config<br/>(yamcs.yml, processors, links, etc.)"]
        end

        XTCE["XTCE Dictionary<br/>(YAMCS dialect XML)"]

        subgraph YSYS["YAMCS"]
            YAMCS["Mission Control / Ground System"]
        end
    end

    FPD --> XTCEC
    FPD --> EVENTS

    XTCEC --> XTCE
    XTCE --> YAMCS
    EVENTS --> YAMCS
    BASECFG --> YAMCS

    %% Make the outer box dotted with no background
    style OUTER stroke-dasharray: 5 5, fill:none
```
