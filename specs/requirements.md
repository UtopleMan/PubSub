# Phase 29 — PubSub.RabbitMQ Library

## North star

**Fastest .NET pub/sub on RabbitMQ.** SMB and MassTransit prioritize feature breadth; Wolverine prioritizes integration. There is an unclaimed lane for a focused, benchmark-published library whose README starts with a head-to-head throughput chart against those three. Every design decision in this phase is judged against that north star: if a feature can't be added without slowing the hot path beyond the budget below, it doesn't ship in v1.

## Context

The platform currently routes every cross-process message through `SlimMessageBus.Host.RabbitMQ` plus a vendored fork (`vendor/SlimMessageBus`, branch `phoenix-patches`) carrying three local patches:

- `UseNativeRecoveryOnly` — neutralizes SMB's own auto-recovery layer, which competes with `RabbitMQ.Client` v7's native recovery and leaks `IConnection`s.
- `PrefetchCount(ushort)` — adds the missing `basic.qos` knob to the consumer builder.
- `name` parameter on `UseDeadLetterExchangeDefaults` — lets multiple modules share one DLX with distinct routing.

Across this build we have also hit issues that are not fork-fixable. The motivating incident from 2026-05-22: an in-flight `AnalystEstimateForSymbolDue` handler on pod `analytics-worker-7999d694bc-rk9vz` wedged silently — no exception, no log line, no CH activity — and held its channel for 30+ minutes until RabbitMQ's `consumer_timeout` finally dropped it. Two consumers shared that channel, so the `factors.engine` consumer went silent simultaneously. Same shape recurred earlier the same day after the `factor_values` `DROP + CREATE` migration. DLQ messages from earlier failures carry **empty** `x-exception` / `x-stacktrace` headers, so root-cause analysis means grep'ing pod logs by timestamp instead of reading the DLQ entry.

This phase delivers a tightly-integrated replacement library that surfaces every operational gotcha as a first-class API. **No production migrations happen in this phase** — those land in follow-up engagements once the library proves itself in tests.

## Scope

### In scope

- A new project **`PubSub`** carrying the cross-transport surface — interfaces (`IPublish<T>`, `ISubscribeTo<T>`), configuration attributes (`[PubSubTopic]`, `[PublishTimeout]`, `[ConsumerPrefetch]`, plus any further tech-agnostic ones we identify), a `PubSubException` hierarchy that every broker library throws so application catch-sites stay portable, and the small amount of pure-CLR support code that every transport would otherwise duplicate (attribute-discovery / cache helpers, serializer abstractions). Anything requiring a transport SDK (RMQ, Kafka) stays out — the package compiles with zero transport dependencies so it can be shared verbatim between `PubSub.RabbitMQ` and a future `PubSub.Kafka`.
- A new project **`PubSub.RabbitMQ`** that implements the contract on top of `RabbitMQ.Client` ≥ 7.0 directly, with channel-per-consumer isolation, mandatory publish-confirm timeouts, single shared `IConnection`, and conventional topology declaration.
- **Auto-attached exception details on DLQ** — when an `ISubscribeTo<T>.OnHandle` throws, the library writes `x-exception-type`, `x-exception`, `x-stacktrace`, `x-consumer-handler`, `x-handler-elapsed-ms`, `x-original-routing-key`, and `x-pod-name` to the DLQ message via direct re-publish to the DLX (broker-native dead-letter routing preserves but does not append headers).
- **Stuck-handler heartbeat metric** — `pubsub.consumer.in_flight_age_ms` gauge, observable callback, faceted by `consumer_tag` + `queue` + `handler_type`. Grafana panel + alert (>60 s for ≥ 90 s).
- **OpenTelemetry tracing** — `pubsub.publish {routing-key}` and `pubsub.consume {queue}` spans linked via `traceparent` propagation through `BasicProperties.Headers`.
- **Source-generated dispatch** — required, not stretch. A Roslyn generator emits per-`IPublish<T>` / `ISubscribeTo<T>` deserializer + dispatch glue at compile-time, removing reflection from the hot path entirely. There is no reflection-based fallback: code that references `PubSub.RabbitMQ` without the generator package fails the build with a clear analyzer diagnostic.
- Integration tests built on `Testcontainers.RabbitMq` proving channel isolation, publish-timeout fail-fast, DLQ exception-header propagation, traceparent continuity, and the stuck-handler metric.

### Out of scope (deferred to follow-up phases)

- Migration of any existing producer/consumer (Scheduler, Ingestion, Factors) off `SlimMessageBus` — these are sequenced per-module in a separate engagement.
- Removal of `SlimMessageBus.*` NuGet packages and the `vendor/SlimMessageBus` submodule.
- Deletion of the three forked SMB patches.
- A `PubSub.Kafka` sibling library. Only the design constraint that `PubSub` must remain Kafka-implementable falls inside this phase.

## Decisions

### Performance bar (non-negotiable acceptance criteria)

These are commitments the library makes publicly in its README and against which every PR is regression-tested. A merge that misses any of them is blocked.

| Metric | Target | Measured how |
|---|---|---|
| Publish throughput (1 KB JSON payload, in-process to broker, single publisher, confirms on) | **≥ 100 000 msg/sec** sustained over 30 s | `vendor/PubSub/bench/PubSub.RabbitMQ.Bench` against a `Testcontainers.RabbitMq` broker on a 4-core runner |
| Publish-confirm p99 | **< 5 ms** at the throughput above | Same harness |
| Publish-confirm p99.9 | **< 25 ms** | Same harness |
| Consume + ack roundtrip p99 | **< 10 ms** (publish → broker → consumer → ack visible at broker) | Same harness, one producer + one consumer |
| Allocations per published message (post-warmup) | **0 bytes** on the hot path | BenchmarkDotNet `[MemoryDiagnoser]` — assert `Gen0 == 0`, `Gen1 == 0`, `Allocated == 0 B` |
| Allocations per consumed message (post-warmup) | **0 bytes** excluding the deserialized `T` itself | Same |
| Head-to-head publish throughput vs SMB / MassTransit / Wolverine on identical hardware | **≥ 1.5× the fastest competitor** | Same harness, separate scenarios with each library configured against the same broker |

These targets are baked into a benchmark CI gate (`plan.md` §0 + §10). The CI gate compares each PR's numbers against a committed baseline JSON and fails the build if any metric regresses by more than 5 %.

### Performance design constraints

The above targets force specific design decisions that must hold across every implementation task:

- **Zero-allocation hot path.** Per-message code paths use `ValueTask` (not `Task`), `ReadOnlyMemory<byte>` (not `byte[]`), pooled `BasicProperties` + pooled `Activity` + pooled serializer buffers. `System.IO.Pipelines` (or a pinned `ArrayPool<byte>` buffer) for serialization. Heap-free `System.Threading.Channels.Channel<T>` with `SingleReader=true, SingleWriter=true` for per-consumer hand-off where applicable.
- **Source-generated dispatch is required, not stretch.** The reflection-based fallback path is removed; the only dispatcher is the generated one. The dispatcher emits `JsonSerializer.Serialize<T>(T, JsonTypeInfo<T>)` against a per-message-type `JsonTypeInfo` so deserialization is allocation-free post-warmup. Code that references `PubSub.RabbitMQ` without the source generator fails the build with a clear analyzer diagnostic.
- **AOT-friendly.** No reflection, no `Expression.Compile()`, no `Activator.CreateInstance` on the hot path. The library compiles cleanly under `PublishAot=true`.
- **Avoid abstractions that prevent inlining.** Internal types stay `sealed` by default. `IPublish<T>` and `ISubscribeTo<T>` resolve to concrete generated types so dispatch is a direct call, not an interface dispatch.
- **No feature lands without a benchmark.** Every PR that touches hot-path code re-runs the bench harness and posts the delta against baseline in the PR body. The reviewer's first scroll target is the numbers, not the diff.

### Architecture

- **Two-project split.** `PubSub` is the shared, transport-agnostic package that both `PubSub.RabbitMQ` and any future broker library depend on. It is broader than a marker-interface project — it owns interfaces, configuration attributes, the exception hierarchy, and any pure-CLR helpers (attribute discovery, serializer abstractions) that every transport would otherwise duplicate — but stays free of transport SDK references so it can ship as one NuGet package alongside per-broker companion packages. Application code references `PubSub` for `IPublish<T>` / `ISubscribeTo<T>` and never on `PubSub.RabbitMQ` directly, so swapping transports per module is a DI-registration change.
- **Tech-agnostic config lives as attributes in `PubSub`.** `[PubSubTopic("routing.key")]` on the message contract is the canonical name. Additional cross-cutting attributes are scoped to concepts every broker has: `[PublishTimeout(Seconds = 10)]` (per-publisher), `[ConsumerPrefetch(50)]` (per-consumer batch hint — Kafka calls it `max.poll.records`, RMQ calls it `basic.qos`, but the concept maps cleanly), `[DeadLetterTopic("phoenix.dlq")]`. Truly broker-specific knobs live as `PubSub.RabbitMQ`-local attributes (e.g. `[RabbitMqQuorumQueue]`) so the contract surface stays portable.
- **Channel-per-consumer.** Every `ISubscribeTo<T>` registration receives its own AMQP `IChannel` from the shared `IConnection`. This is the design fix for the rk9vz multi-consumer-on-one-channel head-of-line block.
- **Single shared `IConnection`.** Owned by the library, exposed via DI as a singleton so health-check probes (`AspNetCore.HealthChecks.RabbitMQ`) reuse it instead of opening per-probe sockets (today's `AddRabbitMqHealthCheckConnection` band-aid).
- **Mandatory publish timeout.** `IPublish<T>.PublishAsync` always runs under a linked CTS. Default 10 s; overridable per-publisher via `[PublishTimeout]`. Hung confirms throw `TimeoutException` immediately.
- **Direct DLX re-publish for exception headers.** The library catches `OnHandle` exceptions, builds enriched `BasicProperties`, publishes the original body to the DLX with the original routing-key, then `BasicNackAsync(requeue: false)` on the source queue. Native `x-dead-letter-exchange` routing preserves but does not append headers; we own the routing to own the metadata.

### Target frameworks

- `PubSub` → `net10.0`. The project is part of the in-tree solution; consistency with the rest of the monolith outweighs cross-target portability. A future open-source split can multi-target if needed.
- `PubSub.RabbitMQ` → `net10.0`.
- `PubSub.RabbitMQ.SourceGenerators` → `netstandard2.0` (Roslyn analyzer requirement).

### Wire serialization

- `System.Text.Json` with the existing `JsonSerializerOptions` shape used by SMB today (PascalCase preserved, `DateTime` as ISO-8601 UTC). The source generator emits `JsonSerializer.Serialize<T>(T, JsonTypeInfo<T>)` calls against a per-message-type `JsonTypeInfo` so the hot path is allocation-free post-warmup.

### Behaviour the library is responsible for

| Concern | Behaviour |
|---|---|
| Connection | One `IConnection` per process. Native auto-recovery enabled. Exposed as DI singleton. |
| Publishing | Confirms required. Per-publish deadline via linked CTS. Throws `TimeoutException` on confirm hang. |
| Consuming | One `IChannel` per registration. `basic.qos = ConsumerPrefetch`. Handler resolved per-message from a DI scope. |
| Acks | Success ⇒ `BasicAckAsync(deliveryTag, multiple: false)`. Failure ⇒ enriched DLX publish + `BasicNackAsync(deliveryTag, multiple: false, requeue: false)`. |
| Topology | Declared once at `IHostedService.StartAsync` in deterministic order: exchanges → DLX/DLQ → DLQ binding → consumer queues + bindings. Idempotent. |
| Metrics | `pubsub.consumer.in_flight_age_ms` gauge (observable). `pubsub.publish.count`, `pubsub.consume.count`, `pubsub.dlq.count` counters. |
| Tracing | Inbound: extract `traceparent` from headers. Outbound: inject into headers. Span names per the OTel messaging semantic-conventions spec. |

### Non-goals

- Request/reply, RPC, sagas, in-memory transport. The library does pub/sub only.
- Multi-broker routing (publish-to-one-broker-consume-from-another). One bus instance maps to one broker.
- Topic-pattern subscriptions for one consumer (e.g. `consume("market.*")`). One `ISubscribeTo<T>` ↔ one routing-key.

## Reference material

- `vendor/SlimMessageBus/src/SlimMessageBus.Host.RabbitMQ/` — the upstream-plus-patches we are replacing; reading order: `RabbitMqChannelManager`, `AbstractRabbitMqConsumer`, `RabbitMqMessageBusSettings`.
- `src/Shared/Common/SmbRabbitMqExtensions.cs` — `AddRabbitMqHealthCheckConnection`, `UseRabbitMqWithNativeRecovery`, `UseDeadLetterExchangeDefaults` extensions; their callers and concerns map directly onto the new library's API.
- `src/Factors/Services/MessageBusEngineJobPublisher.cs` — the per-publish 10 s timeout pattern (added 2026-05-22 commit `883d53c`); the new `IPublish<T>` adopts the same shape as a built-in default.
- `src/Phoenix/Worker/EngineJobConsumer.cs` — canonical consumer shape to mirror as `ISubscribeTo<EngineJob>`.
- `specs/mission.md`, `specs/tech-stack.md` — platform conventions and module layout.
- `specs/roadmap.md` Phase 29 — the source spec; this directory expands it into requirements / plan / validation.
