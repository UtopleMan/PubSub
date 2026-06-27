# Phase 29 — Validation

The library is mergeable to `main` when every checkbox below is green. No production migration ships in this phase — the success bar is "a follow-up engagement can pick the library up and replace one SlimMessageBus producer / consumer pair with no library-side fixes."

## Build + structural

- [ ] `dotnet build Phoenix.slnx` is clean on a fresh checkout (no submodule init needed for the new projects).
- [ ] `PubSub.dll` exports the published surface listed in `plan.md` §1 — `IPublish<T>`, `ISubscribeTo<T>`, `PubSubTopicAttribute`, `PublishTimeoutAttribute`, `ConsumerPrefetchAttribute`, `PubSubException` (+ the two concrete subclasses), `PubSubTopicResolver`. Additional internal helpers are allowed, but every public type in the assembly must be one of those (or its rationale documented in the PR). Verified by a reflection sanity test in `vendor/PubSub/tests/PubSub.Tests/` that enumerates `Assembly.GetExportedTypes()`.
- [ ] `PubSub.dll` has zero transport-layer NuGet dependencies — specifically no `RabbitMQ.Client`, `Confluent.Kafka`, or `SlimMessageBus.*` anywhere in its transitive graph. Verified by `dotnet list package --include-transitive` against `vendor/PubSub/src/PubSub/PubSub.csproj`.
- [ ] `PubSub.RabbitMQ.dll` depends only on `PubSub`, `RabbitMQ.Client` ≥ 7.0, `Microsoft.Extensions.Hosting.Abstractions`, `Microsoft.Extensions.Logging.Abstractions`, and the OTel API packages. No transitive `SlimMessageBus.*`. Verified by `dotnet list package --include-transitive | grep -i slimmessagebus` returning empty.
- [ ] The new projects are included in `Phoenix.slnx` and built by the standard CI pipeline (`docker-publish.yml`).

## Library behaviour — integration tests under `Testcontainers.RabbitMq`

All tests live in `vendor/PubSub/tests/PubSub.RabbitMQ.IntegrationTests/` and use the shared-container fixture from Phase 20. Each scenario stands alone — no scenario depends on side effects from another.

- [ ] **Publish/consume happy path**. One publisher + one consumer; publishes 100 messages with distinct payloads; asserts all 100 arrive at the consumer in send order; asserts each delivery is acked (the queue reports `messages_unacknowledged = 0` within 1 s of the last publish).
- [ ] **Channel isolation**. Two consumers of different message types; one consumer's `OnHandle` blocks on `Task.Delay(20_000)`; the other consumer receives + acks 10 messages within 2 s of their publish. Replicates the rk9vz wedge and proves the new library would not have wedged.
- [ ] **Publisher confirm timeout**. Forces a confirm hang by closing the channel between `BasicPublishAsync` and `WaitForConfirmsOrDieAsync` (simulated via a hooked test channel); asserts `PublishAsync` throws `TimeoutException` within `PublishTimeout + 1 s`; asserts no message is later acked into the queue.
- [ ] **Single shared `IConnection`**. Boots a host with three publishers + five consumers; asserts `IServiceProvider.GetServices<IConnection>().Single().IsOpen == true` and that no second `IConnection` is opened against the broker (verified via management API `/api/connections` count delta).
- [ ] **Topology declaration idempotent**. Starts the bus twice in sequence against the same broker; asserts no exception, asserts queue / exchange / binding state matches between the two boots.

## DLQ contract — auto-attached exception headers

- [ ] **DLQ headers populated on handler throw**. Consumer's `OnHandle` throws `InvalidOperationException("KMF lookup failed")`; asserts the message lands in `phoenix.dlq` and carries:
  - `x-exception-type = "System.InvalidOperationException"`
  - `x-exception = "KMF lookup failed"`
  - `x-stacktrace` contains the consumer type's `FullName`
  - `x-consumer-handler` equals the consumer type's `FullName`
  - `x-handler-elapsed-ms` is a parseable integer > 0
  - `x-original-routing-key` equals the inbound delivery's routing-key
  - `x-pod-name` equals the test process's `HOSTNAME` env (or `"unknown"` if unset)
- [ ] **No DLQ entry on success**. Same setup but `OnHandle` returns normally; asserts `phoenix.dlq.messages_ready` is unchanged 1 s after publish.
- [ ] **Contrast with SMB** (manual one-off, captured in the PR description). Same scenario published through the *current* SMB stack; screenshot of the empty `x-exception` header in the RMQ mgmt UI included in the PR body as a before/after.

## Observability

### Metrics

- [ ] **Heartbeat gauge fires on stuck handler**. `OnHandle` does `Task.Delay(120_000)`; scrapes `/metrics` 30 s after publish; asserts `pubsub_consumer_in_flight_age_ms{queue="...", consumer_tag="..."} >= 30000`. A second scrape after the handler completes asserts that label set no longer appears in the gauge readout.
- [ ] **Counter increments correctly**. Publishes 5, acks 4, nacks 1 (handler throws); asserts `pubsub_publish_count == 5`, `pubsub_consume_count{outcome="acked"} == 4`, `pubsub_consume_count{outcome="nacked-dlq"} == 1`, `pubsub_dlq_publish_count == 1`.

### Tracing

- [ ] **Traceparent continuity**. Using `InMemorySpanExporter`, publishes `A` whose consumer's `OnHandle` publishes downstream `B`. Asserts the exported spans `pubsub.publish A`, `pubsub.consume A`, `pubsub.publish B` form a single chain — `pubsub.publish B`'s parent is `pubsub.consume A`, whose parent is `pubsub.publish A`. Asserts span attributes match the OTel messaging semantic conventions (`messaging.system`, `messaging.destination.name`, `messaging.operation`).

### Dashboards

- [ ] Grafana dashboard JSON `infrastructure/clusters/utopeman/observability/grafana/dashboards/pubsub.json` checked in; loads cleanly into the dev Grafana (verified by hand: import → preview).
- [ ] Alert rule "PubSub stuck consumer" defined in the same JSON; fires at the 90-s mark when the heartbeat test from above runs against a real Grafana + Prometheus pair (verified by hand: rule list shows it in `firing` state during the deliberate hang, returns to `OK` after the hang ends).

## Performance bar (non-negotiable)

These are the public claims the library is committing to. A merge that misses any of them is blocked. All numbers measured by `vendor/PubSub/bench/PubSub.RabbitMQ.Bench/` against a `Testcontainers.RabbitMq` broker on a 4-core runner, 1 KB JSON payload.

- [ ] **Publish throughput ≥ 100 000 msg/sec** sustained over 30 s.
- [ ] **Publish-confirm p99 < 5 ms** at the throughput above.
- [ ] **Publish-confirm p99.9 < 25 ms.**
- [ ] **Consume + ack roundtrip p99 < 10 ms.**
- [ ] **0 B allocated per published message** post-warmup (`[MemoryDiagnoser]` reports `Gen0 == 0`, `Gen1 == 0`, `Allocated == 0 B`).
- [ ] **0 B allocated per consumed message** post-warmup, excluding the deserialized `T` itself.
- [ ] **Head-to-head**: publish throughput on `PubSub.RabbitMQ` is **≥ 1.5× the fastest of SMB / MassTransit / Wolverine** on identical hardware, identical broker, identical payload (`HeadToHead` scenario). The PR description carries a screenshot or markdown table of the four-row comparison.
- [ ] **Channel-isolation throughput delta ≤ 1 %**: with one of five consumers blocked 5 s per message, the other four sustain throughput within 1 % of their unblocked rate (`ChannelIsolation` scenario).

## Source-generated dispatch

- [ ] BenchmarkDotNet harness `vendor/PubSub/bench/PubSub.RabbitMQ.Bench/` builds and runs `dotnet run -c Release --project vendor/PubSub/bench/PubSub.RabbitMQ.Bench -- --filter '*'` cleanly against a fresh `Testcontainers.RabbitMq` broker on a clean checkout.
- [ ] The source-generator analyzer fires `PUBSUB001` as a build error when a project registers a publisher/consumer for a type the generator didn't pick up — verified by a negative test in `vendor/PubSub/tests/PubSub.RabbitMQ.IntegrationTests/` that removes the source-generators package reference and asserts the build fails with that diagnostic ID.
- [ ] No reflection on the hot path: `dotnet publish -c Release -p:PublishAot=true` on a sample app that uses `PubSub.RabbitMQ` succeeds with zero AOT warnings about reflection or trim-unsafe code.

## Benchmark CI gate

- [ ] `.github/workflows/pubsub-bench.yml` runs on every PR touching `vendor/PubSub/**` and posts a step-summary table of `(scenario, metric, baseline, current, delta, threshold)`.
- [ ] A deliberately-regressing PR (e.g. `await Task.Delay(10)` inserted into `RabbitMqPublisher<T>.PublishAsync`) is blocked by the gate with a clear failure summary.
- [ ] A docs-only PR completes the gate workflow in under 5 minutes wall-clock.
- [ ] `vendor/PubSub/bench/baseline.json` is committed and machine-readable; the regression budget (≤ 5 % throughput drop, ≤ 10 % latency increase, no non-zero allocations regression) is enforced by `vendor/PubSub/bench/CompareBaseline/`.
- [ ] A "baseline-shift" PR (labelled `pubsub-baseline`) regenerates `baseline.json` on green and the diff is reviewed like any other change.

## Documentation

- [ ] `specs/tech-stack.md` lists `PubSub` and `PubSub.RabbitMQ` in the Project Structure tree; the Messaging row mentions the new library while flagging that SMB still ships pending migration.
- [ ] `specs/roadmap.md` Phase 29: 29.1–29.6 ticked off (matching the tasks completed in `plan.md`); 29.7 and 29.8 remain open and clearly marked as deferred to the migration phase.
- [ ] `docs/pubsub-rabbitmq.md` exists and contains: minimal `Program.cs` example registering one publisher + one consumer, the four attribute references with one-line semantics each, a "DLQ playbook" subsection mapping each `x-*` header to its diagnostic value, and the OTel span / metric names the library exposes.
- [ ] The example code in `docs/pubsub-rabbitmq.md` compiles when copy-pasted into a fresh `dotnet new console` and gets `PubSub.RabbitMQ` added (verified by a smoke test in CI: run `dotnet build` on a generated project containing the doc's example code block).

## Repo hygiene

- [ ] No new top-level entry in `.gitmodules`. The new projects live in-tree.
- [ ] `git grep -E "TODO|FIXME|HACK"` inside `vendor/PubSub/src/PubSub/` and `vendor/PubSub/src/PubSub.RabbitMQ/` returns zero hits.
- [ ] `vendor/PubSub/PubSub.sln` builds standalone (`dotnet build vendor/PubSub/PubSub.sln` clean) so the directory can be extracted as a standalone repo with no in-tree dependencies. Verified by a CI job (or a manual one-off pre-merge check) that runs the build from `vendor/PubSub/` as the working directory.
- [ ] `vendor/SlimMessageBus`, `SmbRabbitMqExtensions.cs`, and every existing SMB-using consumer / producer remain untouched. The library coexists; no migration starts here.
- [ ] Branch `feat/phase-29-pubsub-rabbitmq` rebases cleanly on `main` at merge time. No merge commits.

## Sign-off

- [ ] A reviewer (or the author at a different sitting, after the dust settles) reads `requirements.md` and confirms every "In scope" bullet maps to at least one checkbox above and at least one task in `plan.md`.
- [ ] The reviewer reads `plan.md` and confirms every "Done when" clause matches a checkbox above.
- [ ] CI green on the branch.
- [ ] PR description includes: a screenshot of the RMQ mgmt UI showing a DLQ entry with populated `x-exception` headers (the headline new capability), a snippet of the BenchmarkDotNet output if the source-generator path made the bar, and a one-paragraph note on what the *follow-up* migration phase needs to look like.
