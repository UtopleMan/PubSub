# Management-API broker-scraping admin (Pulse decoupling)

**Date:** 2026-08-01
**Status:** Approved design, ready for implementation plan

## Problem

`PubSub.Pulse` renders its console from an `IPubSubAdmin`. The only implementation,
`RabbitMqPubSubAdmin`, derives its data from two in-process sources:

1. `PubSubRegistrationSnapshot` — the consumers/publishers registered *in this binary*.
   This is the topology source (which queues, error queues, exchange, routing key, prefetch).
2. `PubSubMetricsSampler` — a `MeterListener` on the in-process `PubSub.RabbitMQ` meter, which
   produces publish/consume/DLQ rates, handler p95, and in-flight age.

Because both are in-process, Pulse only ever sees the slice of the system running in the same
process where consumers were registered. In a distributed landscape (consumers spread across many
binaries) this means one Pulse per binary, each blind to the rest. A plain AMQP connection is not
enough today because half the console's data (handler p95, in-flight/wedge, per-key rates) is
computed from in-process meters the broker never sees, and the "what to inspect" list comes from
local registration, not the broker.

## Goal

Replace the co-located admin with a **broker-scraping** `IPubSubAdmin` that reads the RabbitMQ
**Management HTTP API**. One Pulse instance, pointed at a broker, shows the whole vhost with no
consumer registration required. Pulse becomes deployable as a standalone ops binary.

### Explicit consequence

Handler p95 and in-flight age are **no longer computed anywhere**. The broker cannot report them.
The `IPubSubAdmin` model keeps `HandlerP95Ms`, `InFlightAgeMs`, and `OverallHandlerP95Ms` for
wire compatibility, but they are always `0`. Queue health keys off **depth + error%** only.

The hot-path meter *emission* in the consumer/publisher code (`pubsub.consumer.handler_ms`,
`pubsub.consumer.in_flight_age_ms`, `pubsub.*.count`) **stays** — it remains consumable via
OpenTelemetry. Only the in-process listener/aggregator (`PubSubMetricsSampler`) is deleted.

## Decisions (from brainstorming)

| Question | Decision |
|---|---|
| Coexistence with co-located admin | **Replace entirely** — remove the sampler + registration-snapshot admin |
| Handler p95 / in-flight gap | **Keep model fields, return `0`/null** (least churn; health = depth + error%) |
| Queue discovery | **All queues in the vhost**, DLQ paired by `.error` suffix |
| Failed-message peek/replay/delete | **Hybrid** — Management HTTP for stats; AMQP for error-queue ops |
| Management endpoint + credentials | **Fully separate options** — explicit, never inferred from the AMQP URI |
| Code placement | **Same project** (`src/PubSub.RabbitMQ/Admin/`) |

## Components (new)

### 1. `PubSubAdminOptions` (reworked, same name)

Explicit configuration, no AMQP inference:

- `ManagementBaseUrl` (`Uri`) — required, e.g. `http://rabbit:15672`
- `ManagementUser`, `ManagementPassword` — required
- `VHost` (`string`) — required, e.g. `/`
- `ConnectionString` (AMQP) — required; used only for error-queue peek/replay/delete
- `ServiceName` — kept; shown as the console "endpoint" column
- `FailedPeekPerQueue` — kept; bounds per-`.error`-queue peek cost
- `SeriesLength` — kept; how many historical samples to request from the Management API for sparklines
- `WedgeThresholdMs` — kept, but now **UI-only** (passed to `PulseClientOptions`; no longer affects health because in-flight is always `0`)
- **Removed:** `SampleInterval` (no local sampler)

### 2. `RabbitMqManagementClient`

Typed `HttpClient` wrapper with HTTP basic auth. Methods:

- `GetOverviewAsync(ct)` → `/api/overview`
- `GetQueuesAsync(vhost, ct)` → `/api/queues/{vhost}` (with `?msg_rates_age`/`?msg_rates_incr`/`?lengths_age`/`?lengths_incr` for rate + sample history)
- `GetBindingsAsync(vhost, ct)` → `/api/bindings/{vhost}`
- `GetExchangesAsync(vhost, ct)` → `/api/exchanges/{vhost}`
- `GetConsumersAsync(vhost, ct)` → `/api/consumers/{vhost}` (for per-queue `prefetch_count`)

Deserializes management JSON DTOs. This is the isolated, unit-testable unit (stub
`HttpMessageHandler`). On HTTP failure it surfaces a typed failure the admin can degrade on
(returns empty/zeroed rather than throwing to the endpoint).

### 3. Management DTOs + `ManagementJsonContext`

Source-generated JSON (`JsonSerializerContext`), mirroring the existing `PubSubAdminJsonContext`
pattern. DTOs cover only the fields consumed:

- `ManagementQueue` — `name`, `vhost`, `messages_ready`, `messages_unacknowledged`, `consumers`,
  `message_stats` { `publish_details`, `ack_details`, `deliver_get_details`, `redeliver_details` }
  where each `*_details` has `rate` and `samples`
- `ManagementBinding` — `source` (exchange), `routing_key`, `destination` (queue), `destination_type`
- `ManagementExchange` — `name`, `type`
- `ManagementConsumer` — `queue` { `name` }, `prefetch_count`
- `ManagementOverview` — `message_stats` (cluster-wide, with `_details.rate` + `samples`)

### 4. `ErrorQueueOperations`

Extracted from the current `RabbitMqPubSubAdmin`, salvaged verbatim (the logic is solid): AMQP peek
(`BasicGet` + requeue), replay (republish with publisher confirms, preserving body and
`x-original-routing-key`), delete (`DrainAsync`). Owns a lightweight AMQP connection built from
`PubSubAdminOptions.ConnectionString` — independent of `PubSubConnectionProvider`, so it works in a
standalone Pulse binary. Also retains `BuildFailedMessage` header parsing and `ShovelCommand`.

### 5. `ManagementRabbitMqPubSubAdmin : IPubSubAdmin`

Maps Management DTOs to the `IPubSubAdmin` models; delegates failed-message operations to
`ErrorQueueOperations`. Stateless per request (no local ring buffers; sparkline history comes from
the Management API samples).

### 6. `AddPubSubRabbitMqAdmin(Action<PubSubAdminOptions>)` (reworked)

Registers `PubSubAdminOptions`, the management `HttpClient` (via `AddHttpClient`), and
`IPubSubAdmin` → `ManagementRabbitMqPubSubAdmin`. No hosted service, no sampler, no dependency on
`PubSubRegistrationSnapshot` or `PubSubConnectionProvider`. Standalone-capable.

## Removals

- `src/PubSub.RabbitMQ/Admin/PubSubMetricsSampler.cs` (includes `MetricsSnapshot`)
- `src/PubSub.RabbitMQ/Admin/RabbitMqPubSubAdmin.cs` (error-queue logic salvaged into `ErrorQueueOperations`)
- Sampler / hosted-service / registration-snapshot wiring inside the old `AddPubSubRabbitMqAdmin`

The meter instrumentation in the consumer/publisher hot path is **not** removed.

## Data flow + model mapping

Pulse WASM polls `{mount}/api/*` → admin method → live Management HTTP GET (per request, no local
state) → map to models. AMQP is used only for the failed-message endpoints.

| Model field | Management API source |
|---|---|
| `QueueStat.Depth` | queue `messages_ready` |
| `QueueStat.Consumers` | queue `consumers` |
| `QueueStat.Endpoint` | `PubSubAdminOptions.ServiceName` (also used for `FailedMessage.Endpoint`) |
| `QueueStat.Prefetch` | `/api/consumers` `prefetch_count` for the queue, or `0` if unavailable |
| `QueueStat.PublishRate` | queue `message_stats.publish_details.rate` (ingress into the queue) |
| `QueueStat.ConsumeRate` | queue `message_stats.ack_details.rate` |
| `QueueStat.DlqRate` | `{queue}.error` queue `message_stats.publish_details.rate` |
| `QueueStat.ConsumeSeries` | queue `ack_details.samples` (broker-provided history) |
| `QueueStat.Exchange` / `RoutingKey` | `/api/bindings` for the queue (authoritative; not name-parsed) |
| `QueueStat.HandlerP95Ms` / `InFlightAgeMs` | `0` (not available from broker) |
| `QueueStat.Status` | derived from depth + error% (in-flight branch never trips) |
| `TopologySnapshot` exchanges/topics | `/api/exchanges` + `/api/bindings`; failed count from `{q}.error` depth |
| `PubSubKpis` totals + series | `/api/overview` `message_stats` (`publish`, `deliver_get`/`ack`, DLQ) + `samples`; handler p95 KPI = `0` |
| `MessagingInstallation.Connected` | Management API reachable (`/api/overview` succeeds) |

**Discovery:** enumerate all queues in `VHost`. A queue is treated as a consumer queue; its DLQ is
`{name}.error` when that sibling exists. Error queues (suffix `.error`) are not listed as consumer
rows themselves — they contribute failed counts/rates to their base queue.

**Error % / health:** `errPct = consumeRate > 0 ? dlqRate/consumeRate*100 : (dlqRate > 0 ? 100 : 0)`
(unchanged formula). `Health(depth, errPct)`: Critical if `errPct > 5`; Warning if `errPct > 1 ||
depth > 5000`; else Healthy.

## Error handling

- **Management API unreachable** → `GetInstallationAsync` returns `Connected = false`; stat
  endpoints log a warning and return empty/zeroed results so the console renders "disconnected"
  rather than a 500.
- **Rates disabled on broker** (`management_rates_mode = none`) → `*_details.rate` absent → treated
  as `0`; sparkline series empty.
- **`.error` queue absent** → failed depth 0, peek skipped (already handled in salvaged logic).
- **AMQP unavailable for ops** → `ReplayAsync`/`DeleteAsync` return `0` with a logged warning;
  `GetFailedMessagesAsync` returns empty.

## Placement + testing

**Placement:** all new files under `src/PubSub.RabbitMQ/Admin/`. A standalone Pulse ops binary
references `PubSub.RabbitMQ` and simply does not call `AddPubSubRabbitMq`. (Alternative considered
and deferred: a slim `PubSub.RabbitMQ.Management` project — cleaner standalone package, more
scaffolding; YAGNI for now.)

**Unit tests** (`PubSub.Pulse.Tests`, or a new `PubSub.RabbitMQ.Tests` if cleaner):

- `RabbitMqManagementClient` against a stub `HttpMessageHandler` returning canned management JSON —
  assert DTO parsing (rates, samples, bindings).
- `ManagementRabbitMqPubSubAdmin` mapping — feed a fake client; assert `QueueStat`/`TopologySnapshot`/
  `PubSubKpis` mapping, DLQ pairing by `.error`, health from depth + error%, and p95/in-flight = 0.

**Integration** (`PubSub.RabbitMQ.IntegrationTests`): rework `AdminApiTests` to register the new
options from the fixture's `ManagementUri` / `ManagementUser` / `ManagementPassword` +
`ConnectionString`. Existing assertions for depth, consumers, failed-message listing, replay, and
delete stay valid under the hybrid design. The fixture already runs `rabbitmq:3.13-management`, so
the Management API is available; it already exposes the management properties.

## Out of scope

- OpenTelemetry export wiring for handler p95 / in-flight (option C from the discussion) — separate effort.
- Changes to the Pulse WASM client beyond what the always-`0` fields already tolerate (the gauges
  render "n/a"/empty; no client rewrite).
- Multi-vhost aggregation (single configured `VHost` per Pulse instance).
