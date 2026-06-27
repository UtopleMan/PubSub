# Phase 29 — Plan

Numbered tasks. Each names the files touched and the executable proof point that finishes it. Order is build-order — later tasks can compile and test against earlier ones.

**Directory layout**: every new project in this phase lives under `vendor/PubSub/` rather than `src/`. The library is designed to be lifted out as its own repository and published as an independent NuGet package once it has proved itself in-tree; keeping it under `vendor/` from day one means the extraction will be a `git filter-repo` of one directory and no path rewrites across the consuming code. Within `vendor/PubSub/` the layout mirrors what a standalone repo would look like: `vendor/PubSub/PubSub.sln`, `vendor/PubSub/src/PubSub/`, `vendor/PubSub/src/PubSub.RabbitMQ/`, `vendor/PubSub/src/PubSub.RabbitMQ.SourceGenerators/`, `vendor/PubSub/tests/PubSub.Tests/`, `vendor/PubSub/tests/PubSub.RabbitMQ.IntegrationTests/`, `vendor/PubSub/bench/PubSub.RabbitMQ.Bench/`, plus a future `vendor/PubSub/README.md` written as the published-NuGet-page front matter. Reference the `.csproj`s from `Phoenix.slnx` via `ProjectReference` so the modular monolith still consumes them in-tree without an intermediate NuGet step.

## 0. Benchmark contract first

Before any library code is written, define the performance targets the library is committing to and stand up the harness that will measure them. This anchors every later task in a number, not an intuition.

Create `vendor/PubSub/bench/PubSub.RabbitMQ.Bench/` (BenchmarkDotNet, `[MemoryDiagnoser]`, targeting `net10.0`). Scenarios — each ran against a `Testcontainers.RabbitMq` broker so numbers are reproducible from a clean checkout:

- **`PublishThroughput`** — single publisher, 1 KB JSON payload, confirms on, sustained 30 s. Reports msg/sec, publish-confirm p50 / p95 / p99 / p99.9 latency, allocations per message.
- **`ConsumeRoundtrip`** — one producer + one consumer, publish → broker → consumer → ack visible at broker. Reports roundtrip p50 / p95 / p99 latency, allocations per consumed message.
- **`ChannelIsolation`** — five consumers of distinct message types, one of which blocks 5 s per message. Reports throughput on the *other four* during the block (must stay within 1 % of unblocked throughput).
- **`HeadToHead`** — same `PublishThroughput` + `ConsumeRoundtrip` scenarios run separately against SMB, MassTransit, Wolverine, and `PubSub.RabbitMQ` (each library configured against the same broker, same payload, same hardware). Reports a four-row comparison table.

Commit baseline numbers as `vendor/PubSub/bench/baseline.json` after running the harness against a placeholder no-op implementation of `IPublish<T>` (so the first real implementation is measured against zero). The benchmark CI gate (§10) reads this file.

Done when: `dotnet run -c Release --project vendor/PubSub/bench/PubSub.RabbitMQ.Bench -- --filter '*'` runs all four scenarios end-to-end against a fresh `Testcontainers.RabbitMq` broker on a clean checkout; `baseline.json` is committed; the README in `vendor/PubSub/bench/README.md` documents how to reproduce the numbers locally (single `dotnet run` command, no env-var prerequisites). The performance targets from `requirements.md` are written into the scenario assertions so a missed target is a benchmark failure, not just a slow number.

## 1. `PubSub` project (shared, transport-agnostic)

The shared package is named **`PubSub`** (not `PubSub.Interfaces`). It is the only assembly both the RabbitMQ implementation and a future `PubSub.Kafka` sibling depend on, and it carries **both the contract surface (interfaces + attributes) and any transport-agnostic supporting code** that has a sensible meaning under every broker — for example serializer abstractions, attribute-discovery helpers that walk `T` for `[PubSubTopic]` once at startup and cache the result, a small `PubSubException` hierarchy with `PubSubPublishTimeoutException` / `PubSubHandlerFailedException` that broker libraries throw uniformly so application catch sites stay portable, and similar pure-CLR utilities. Anything that requires `RabbitMQ.Client` or `Confluent.Kafka` stays out — the package must compile and test without either reference.

Create `vendor/PubSub/src/PubSub/PubSub.csproj` (`net10.0`, no transport-layer dependencies). Add to both `vendor/PubSub/PubSub.sln` (the standalone solution that will move when the directory is extracted) and `Phoenix.slnx` (so the monolith picks it up).

Required types (the published surface; more transport-agnostic helpers can land alongside as needed):

- `interface IPublish<T> { Task PublishAsync(T message, CancellationToken cancellationToken = default); }`
- `interface ISubscribeTo<T> { Task Handle(T message, CancellationToken cancellationToken); }`
- `[AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct)] sealed class PubSubTopicAttribute(string routingKey) : Attribute { public string RoutingKey { get; } = routingKey; public string Exchange { get; init; } = "phoenix.events"; }` — placed on the message contract record.
- `[AttributeUsage(AttributeTargets.Class)] sealed class PublishTimeoutAttribute(int seconds) : Attribute { public int Seconds { get; } = seconds; }` — placed on the message contract record or on the publisher implementation; default 10 s when absent.
- `[AttributeUsage(AttributeTargets.Class)] sealed class ConsumerPrefetchAttribute(int count) : Attribute { public int Count { get; } = count; }` — placed on the `ISubscribeTo<T>` implementation; default 50 when absent.
- `abstract class PubSubException(string message, Exception? inner = null) : Exception(message, inner)` plus the two concrete subclasses `PubSubPublishTimeoutException` and `PubSubHandlerFailedException`, each in its own file. Broker libraries throw these so callers can write transport-agnostic `catch` blocks.
- `static class PubSubTopicResolver { public static PubSubTopicAttribute Resolve<T>(); }` — discovers and caches `[PubSubTopic]` per `T` once via `ConcurrentDictionary<Type, PubSubTopicAttribute>`. Throws `InvalidOperationException` with a precise message if the attribute is missing — every transport calls this rather than re-implementing the lookup.

Done when: `dotnet build vendor/PubSub/src/PubSub/PubSub.csproj` is clean; `PubSub.dll` has zero external NuGet dependencies (verified by `dotnet list package --include-transitive`); xUnit suite in `vendor/PubSub/tests/PubSub.Tests/` covers (a) the public surface listed above is present and public, (b) `PubSubTopicResolver.Resolve<T>` returns the attribute for an annotated `T` and throws a clear message for an unannotated one, (c) every exception type round-trips its message + inner exception. The test target is referenced by `vendor/PubSub/PubSub.sln` so future standalone CI on the extracted repo runs it unchanged.

## 2. `PubSub.RabbitMQ` core — connection, channel-per-consumer, publishers, consumers

Create `vendor/PubSub/src/PubSub.RabbitMQ/PubSub.RabbitMQ.csproj` (`net10.0`). Dependencies: `RabbitMQ.Client` ≥ 7.0, `PubSub` (project-reference inside `vendor/PubSub/`), `Microsoft.Extensions.Hosting.Abstractions`, `Microsoft.Extensions.Logging.Abstractions`. Add to both `vendor/PubSub/PubSub.sln` and `Phoenix.slnx`.

Public surface:

- `PubSubRabbitMqOptions` — connection string, default publish timeout, default DLX name (`phoenix.dlx`), default DLQ name (`phoenix.dlq`).
- `services.AddPubSubRabbitMq(Action<PubSubRabbitMqBuilder> configure)` — DI extension method. The builder exposes `Publish<T>()` and `Subscribe<T, TConsumer>()` where `TConsumer : ISubscribeTo<T>`. Registrations land in a `PubSubRabbitMqOptions` snapshot.

Internal:

- `PubSubConnectionProvider` — singleton that owns one `IConnection`; uses native auto-recovery; exposes it via DI as `IConnection` so external health-checks can reuse.
- `RabbitMqPublisher<T>` — implements `IPublish<T>`. Reads `[PubSubTopic]` for routing-key + exchange and `[PublishTimeout]` for the per-publish deadline. Wraps `BasicPublishAsync` + `WaitForConfirmsOrDieAsync` in a linked CTS firing after the configured timeout. Throws `TimeoutException` on confirm hang.
- `RabbitMqConsumerHost<T>` — one `IHostedService` per `Subscribe<T, TConsumer>()` registration. Opens a dedicated `IChannel` against the shared `IConnection`. Sets `basic.qos` from `[ConsumerPrefetch]`. Registers a `BasicConsumeAsync` callback that resolves the `ISubscribeTo<T>` from a per-message `IServiceScope`, invokes `Handle`, then `BasicAckAsync` on success. Exception handling routes to task 3.

Done when: integration test using `Testcontainers.RabbitMq` declares one publisher + one consumer, publishes 100 messages of distinct payload shapes, asserts all 100 arrive in order; a second test asserts each consumer's `IChannel.ChannelNumber` is distinct from every other consumer's and from the publishers'; a third test asserts a forced `await Task.Delay(20_000)` inside one consumer's `Handle` does not delay deliveries on a second consumer of a different message type beyond its own prefetch buffer.

## 3. Topology declaration + DLX with auto-attached exception headers

Add `PubSubRabbitMqHostedService` (singleton `IHostedService`) that runs once at startup before any consumer host starts. Declaration order is deterministic:

1. Declare every distinct exchange referenced by any `[PubSubTopic]` (default `phoenix.events`, type `topic`, durable, no auto-delete).
2. Declare the DLX (`phoenix.dlx`, type `topic`, durable).
3. Declare the DLQ (`phoenix.dlq`, quorum queue, durable).
4. Bind DLQ to DLX with routing-key `#`.
5. For each `Subscribe<T, TConsumer>()` registration, declare the queue named `{exchange}.{routingKey}` (override via `PubSubTopic.Queue`) and bind it to its exchange with the contract's routing-key.

Add exception handling inside `RabbitMqConsumerHost<T>`'s message callback. When `Handle` throws:

1. Catch and log the exception with the consumer's tag + handler type.
2. Build a fresh `BasicProperties` carrying every original header plus:
   - `x-exception-type` = exception's full type name
   - `x-exception` = exception's `Message`
   - `x-stacktrace` = exception's full `ToString()`
   - `x-consumer-handler` = `typeof(TConsumer).FullName`
   - `x-handler-elapsed-ms` = wall-clock elapsed from `BasicConsumeAsync` callback entry
   - `x-original-routing-key` = the inbound delivery's routing-key
   - `x-pod-name` = `Environment.GetEnvironmentVariable("HOSTNAME") ?? "unknown"`
3. `BasicPublishAsync` the original body to `phoenix.dlx` with the original routing-key and the enriched properties.
4. `BasicNackAsync(deliveryTag, multiple: false, requeue: false)` on the source queue.

Done when: integration test declares a consumer whose `Handle` throws `InvalidOperationException("KMF lookup failed")`; publishes one message; asserts (a) the message lands in `phoenix.dlq`, (b) `x-exception-type` is `"System.InvalidOperationException"`, (c) `x-exception` is `"KMF lookup failed"`, (d) `x-stacktrace` contains the consumer's `FullName`, (e) `x-pod-name` matches the test process's `HOSTNAME`. A separate test publishes a message that succeeds and asserts the message acks normally with no DLQ entry.

## 4. Stuck-handler heartbeat metric + Grafana alert

Add an `IMeterFactory`-backed meter `PubSub.RabbitMQ` with one observable gauge `pubsub.consumer.in_flight_age_ms` faceted by `consumer_tag`, `queue`, `handler_type`.

Each `RabbitMqConsumerHost<T>` maintains a `ConcurrentDictionary<ulong, long>` of `(deliveryTag, startedAtTickCount64)` populated on consume-callback entry and removed on ack/nack. The observable callback enumerates every live consumer host's dictionary and emits `Environment.TickCount64 - startedAtTickCount64` for each in-flight delivery.

Add counters: `pubsub.publish.count` (faceted by `routing_key`), `pubsub.consume.count` (faceted by `queue` + `outcome` where outcome ∈ `acked` / `nacked-dlq`), `pubsub.dlq.publish.count`.

Add Grafana dashboard JSON at `infrastructure/clusters/utopeman/observability/grafana/dashboards/pubsub.json` with three panels: handler-age p95/max per queue, publish/consume rate per routing-key, DLQ entry rate. Add alert rule "PubSub stuck consumer" firing when `max(pubsub_consumer_in_flight_age_ms) > 60_000` for any queue for ≥ 90 s.

Done when: integration test inserts a `Task.Delay(120_000)` inside one consumer's `Handle`, publishes one message, scrapes `/metrics` 30 s after publish, asserts the gauge for that consumer's `consumer_tag` is `≥ 30_000`. A second scrape after the handler completes asserts the gauge no longer reports that `consumer_tag`. The today-rk9vz scenario reproduced under this assertion fires the alert at the 90-second mark.

## 5. OpenTelemetry tracing — publish + consume span propagation

Add the `PubSub.RabbitMQ` `ActivitySource` named `PubSub.RabbitMQ`. Publishers start a span `pubsub.publish {routing-key}` with attributes `messaging.system="rabbitmq"`, `messaging.destination.name={exchange}`, `messaging.rabbitmq.destination.routing_key={routingKey}`, `messaging.message.body.size`, `messaging.operation="publish"`. Inject `traceparent` + `tracestate` into outbound `BasicProperties.Headers` before `BasicPublishAsync`.

Consumer hosts extract `traceparent` + `tracestate` from inbound `BasicProperties.Headers` and start a span `pubsub.consume {queue}` with the extracted context as parent, attributes `messaging.operation="receive"`, `messaging.destination.name={queue}`, `messaging.message.body.size`, `messaging.consumer.id={consumerTag}`. The span scope wraps the entire `Handle` invocation.

Register the activity source via `services.AddOpenTelemetry().WithTracing(b => b.AddSource("PubSub.RabbitMQ"))` in callers — the library does not auto-register OTel to keep DI explicit.

Done when: integration test using the `InMemorySpanExporter` publishes one message via an `IPublish<T>` whose consumer's `Handle` immediately re-publishes a downstream message. The test asserts the three resulting spans (`pubsub.publish A`, `pubsub.consume A`, `pubsub.publish B`) form a single parent chain via `traceparent`.

## 6. Source-generated dispatch — required

This is the load-bearing task for the throughput target. There is no reflection-based fallback; the only legitimate dispatch path is the generated one.

Create `vendor/PubSub/src/PubSub.RabbitMQ.SourceGenerators/PubSub.RabbitMQ.SourceGenerators.csproj` (`netstandard2.0`, `IsRoslynComponent=true`, references `Microsoft.CodeAnalysis.CSharp` ≥ 4.10). Add to `vendor/PubSub/PubSub.sln` + `Phoenix.slnx`.

The generator scans the compilation for every type implementing `ISubscribeTo<T>` and every `PubSubRabbitMqBuilder.Publish<T>()` / `Subscribe<T, …>()` call site. For each unique `T` it emits:

- A `[JsonSerializable(typeof(T))]` partial `JsonSerializerContext` so `JsonSerializer.Serialize<T>(T, JsonTypeInfo<T>)` is used (no reflection).
- A `static class PubSubDispatcher_{T_safe_name}` with `Serialize(T, IBufferWriter<byte>)`, `Deserialize(ReadOnlySpan<byte>)`, and an `Invoke(ISubscribeTo<T>, T, CancellationToken)` direct call (no interface dispatch — the consumer's concrete type is statically known from the registration site).
- A `static class PubSubGeneratedRegistry` whose static constructor populates a `FrozenDictionary<Type, IPubSubDispatcher>` keyed by `typeof(T)`. `RabbitMqPublisher<T>` and `RabbitMqConsumerHost<T>` read from the frozen dictionary once at startup and cache the dispatcher in a static field.

A companion analyzer (in the same project) fires `PUBSUB001: Missing source-generated dispatcher for {T}` as a build error whenever `RabbitMqPublisher<T>` or `RabbitMqConsumerHost<T>` are instantiated for a `T` that the generator didn't pick up — usually because the consuming project forgot to reference `PubSub.RabbitMQ.SourceGenerators`. The diagnostic message names the missing type and the offending registration line, so the fix is one project-reference away.

Hot-path discipline:

- `Serialize` writes to an `IBufferWriter<byte>` (typically `PipeWriter`) — no intermediate `byte[]`, no `MemoryStream`.
- `Deserialize` reads from `ReadOnlySpan<byte>` — no copy from the broker's frame.
- `Invoke` is a non-virtual direct call to the registered concrete consumer type. No reflection, no `Expression.Compile`, no `Activator.CreateInstance`.
- The dispatcher exposes `ValueTask` (not `Task`) on every member so a synchronously-completing handler allocates zero state-machine objects.

Done when:

- The benchmark `PublishThroughput` scenario from §0 reports **0 B allocated per message** post-warmup (`[MemoryDiagnoser]` `Allocated == 0 B`).
- `ConsumeRoundtrip` reports **0 B allocated per consumed message** excluding the deserialized `T` itself.
- The `HeadToHead` scenario shows `PubSub.RabbitMQ` outpacing SMB / MassTransit / Wolverine by **≥ 1.5×** in publish throughput on the 1 KB payload.
- A negative test in `vendor/PubSub/tests/PubSub.RabbitMQ.IntegrationTests/` removes the source-generators package reference from a sample project and asserts the build fails with `PUBSUB001`.

## 7. Documentation + repo hygiene

Update `specs/tech-stack.md`:

- Replace "Messaging: SlimMessageBus + NATS JetStream" with "Messaging: PubSub.RabbitMQ (in-tree) on RabbitMQ.Client v7" — but keep a one-line note that SlimMessageBus still ships in production until the migration phase removes it.
- Add `PubSub` and `PubSub.RabbitMQ` to the Project Structure tree.

Update `specs/roadmap.md` Phase 29 task list:

- Tick off `29.1`–`29.6` as the library ships.
- Leave `29.7` and `29.8` open (those are the deferred migration + removal phases).

Add `docs/pubsub-rabbitmq.md` covering: registering a publisher, registering a consumer, the four attributes (`[PubSubTopic]`, `[PublishTimeout]`, `[ConsumerPrefetch]`, plus any RMQ-local additions from task 2), how the DLQ headers map back to a failing handler, and the metric / span names exposed for ops.

Done when: `git grep -lE "PubSub\\.(Interfaces|RabbitMQ)"` lists `specs/tech-stack.md`, `specs/roadmap.md`, and `docs/pubsub-rabbitmq.md` among the matches; the `docs/pubsub-rabbitmq.md` example compiles when copy-pasted into a `Program.cs`.

## 8. Benchmark CI gate

Every PR that touches `vendor/PubSub/` re-runs the §0 harness against the committed `vendor/PubSub/bench/baseline.json` and fails the build on regression beyond the budget. This is the mechanism that defends the north-star throughput claim over time — without it the targets erode silently.

New GitHub Actions workflow `.github/workflows/pubsub-bench.yml`:

- Trigger: any PR touching `vendor/PubSub/**` or `vendor/PubSub/bench/baseline.json`.
- Runner: `ubuntu-latest`, `dotnet 10.0` setup, Docker available (for `Testcontainers.RabbitMq`).
- Steps: `dotnet build -c Release vendor/PubSub/PubSub.sln`, then `dotnet run -c Release --project vendor/PubSub/bench/PubSub.RabbitMQ.Bench -- --filter '*' --exporters json --artifacts bench-results/`.
- Comparison step: a small `vendor/PubSub/bench/CompareBaseline/` console app reads the run's JSON output, joins against `baseline.json` by scenario name, and asserts every measured metric (throughput, p99 latency, allocations) is within the regression budget:
  - **Throughput**: allowed drop ≤ 5 % vs baseline.
  - **Latency p99 / p99.9**: allowed increase ≤ 10 % vs baseline.
  - **Allocations**: must stay at `0 B` post-warmup for any scenario that was at zero in baseline. No regression to non-zero is allowed.
- On failure: post a step summary table showing scenario, metric, baseline, current, delta, threshold; mark the workflow red so branch protection blocks merge.
- On success on `main` after a deliberate baseline-shift PR: the same workflow regenerates and commits `baseline.json` so the new floor is published. (The workflow detects "baseline-shift" PRs via a `pubsub-baseline` label so accidental shifts don't auto-rebaseline.)

Add a Grafana panel "PubSub bench history" that plots throughput + p99 over time, sourced from the run's JSON output uploaded to S3 (`s3://utopleman-bench/pubsub/{commit_sha}.json`). The panel makes the long-run trend visible to anyone watching the dashboard, not just the PR author.

Done when:

- A deliberately-introduced regression PR (e.g. `await Task.Delay(10)` inserted into `RabbitMqPublisher<T>.PublishAsync`) is blocked by the CI gate with a clear step-summary table.
- A neutral PR (docs-only change) passes the gate within 5 minutes wall-clock from queue to green.
- `baseline.json` is regenerated on a baseline-shift PR via the labelled-PR path; the diff in `baseline.json` is reviewed in the PR like any other change.
- A Grafana screenshot of the bench-history panel goes into the PR description for the original library merge.
