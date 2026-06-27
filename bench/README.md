# PubSub.RabbitMQ Benchmarks

BenchmarkDotNet harness against a `Testcontainers.RabbitMq` broker. Compares `PubSub.RabbitMQ` head-to-head with `MassTransit`, `Wolverine`, and `NServiceBus` on the same payload, same broker, same hardware.

## Reproduce locally

Prerequisites: Docker socket reachable (Docker Desktop, OrbStack, or Colima). No env vars needed — Testcontainers auto-detects the active provider.

```bash
dotnet run -c Release --project vendor/PubSub/bench/PubSub.RabbitMQ.Bench -- --filter '*'
```

## Scenarios

- `HeadToHeadPublishBench.Publish` — single publisher, ~1 KB JSON payload (`BenchPayload`), parameterised on `Library = PubSub | MassTransit | Wolverine | NServiceBus`. **Confirm-wait** semantics for PubSub / MassTransit / NServiceBus; Wolverine in this scenario uses `SendInline()` but its transport cannot enable publisher confirms.
- `HeadToHeadFireAndForgetBench.Publish` — apples-to-apples fire-and-forget: PubSub `IFireAndForgetPublish<T>` vs Wolverine `SendInline()` against the same broker, same payload. Both deliver "bytes on socket"; neither awaits broker confirms.

## Results (snapshot, OrbStack, macOS 26.5, Intel i9-9880H)

### Confirm-wait scenario (`HeadToHeadPublishBench`)

```
| Method  | Library     | Mean      | Allocated |
|-------- |------------ |----------:|----------:|
| Publish | PubSub      | 353.48 us |   2.77 KB |
| Publish | MassTransit | 418.91 us |  10.61 KB |
| Publish | Wolverine   |  63.03 us |   2.63 KB |
| Publish | NServiceBus | 441.58 us |   7.89 KB |
```

### Fire-and-forget scenario (`HeadToHeadFireAndForgetBench` — apples-to-apples)

```
| Method  | Library   | Mean      | Allocated |
|-------- |---------- |----------:|----------:|
| Publish | PubSub    |   9.66 us |   1.07 KB |
| Publish | Wolverine |  78.48 us |   2.61 KB |
```

PubSub `IFireAndForgetPublish<T>` is **8.1× faster than Wolverine** and **~2.4× lower allocation** at the same semantic level.

### Consume roundtrip scenario (`HeadToHeadConsumeBench`)

End-to-end: bench publishes one message, awaits a `TaskCompletionSource` that the library's consumer handler signals on receipt. Measures publish + broker forward + consumer dispatch + handler invoke + ack.

```
| Method    | Library     | Median   | Allocated |
|---------- |------------ |---------:|----------:|
| Roundtrip | Wolverine   | 1.547 ms |  13.34 KB |
| Roundtrip | PubSub      | 3.470 ms |        ~0 |
| Roundtrip | NServiceBus | 3.430 ms |  30.45 KB |
| Roundtrip | MassTransit | 23.35 ms |  35.57 KB |
```

Notes:
- PubSub's per-iteration allocation is below the `[MemoryDiagnoser]` reporting threshold — the consume path's allocations happen on broker-callback threads and aren't attributed to the benchmark thread. NServiceBus + MassTransit allocate the listed bytes on the benchmark thread because of how their internal pipelines marshal completions.
- Wolverine's 1.55 ms median is genuinely fast — *not* an in-process shortcut. Verified by `dotnet run -c Release --project vendor/PubSub/bench/PubSub.RabbitMQ.Bench -- diagnose-wolverine`, which publishes 100 messages and queries the RMQ management API for the queue's `message_stats`: all 100 messages transit the broker (`publish.count=100`, `deliver.count=100`). Wolverine's edge comes from **runtime-compiled handler dispatch via `JasperFx.CodeGeneration`** — at startup it Roslyn-emits per-`T` handler invocation code that inlines deserialization + DI resolution + handler call. PubSub's current consume path uses reflection-based `JsonSerializer.Deserialize<T>` + per-message `IServiceScope` creation + DI graph walk, which adds ~2 ms over Wolverine's pre-baked path. This is exactly what `vendor/PubSub/optimizations.md` §Bundle 3 (the deferred source-generator task) targets.
- MassTransit's 23 ms median is consistent with prior third-party benchmarks reporting MT's per-message middleware pipeline as the bottleneck at low publisher rates.
- Means have wide CIs (Docker/NAT loopback latency varies); medians are more reliable for ordering.

**Read the numbers with the publish-semantics in mind:**

| Library | What `PublishAsync` returns at | Broker confirms? |
|---|---|---|
| **PubSub.RabbitMQ** | The broker has confirmed the message via `WaitForConfirmsOrDieAsync` | **Always on — mandatory** |
| **MassTransit** | The broker has confirmed the message | On (default) |
| **NServiceBus** | The broker has confirmed the message | On (default) |
| **Wolverine** | The bytes have been written to the broker's socket via `BasicPublishAsync` | **Off — not supported by `WolverineFx.RabbitMQ` 3.4.0** |

We tried `PublishMessage<T>().ToRabbitExchange(...).SendInline()` to disable Wolverine's in-memory dataflow buffer (which alone moved it 36 µs → 54 µs). But `WolverineFx.RabbitMQ` 3.4.0 has no public API to enable publisher confirms — its `RabbitMqSender` opens channels with `publisherConfirmationsEnabled: false` and there's no transport-level toggle. Wolverine's at-least-once story is its **durable outbox** (persist locally before send, recover on restart), not broker confirms. That's a deliberate Wolverine design choice; it just makes a confirm-to-confirm comparison impossible without forking the transport.

So the head-to-head story is two-tiered:

- **Apples-to-apples** (PubSub vs MassTransit vs NServiceBus — all three confirm-publish):
  - **PubSub** at 351 µs / 2.89 KB
  - **MassTransit** at 412 µs / 10.61 KB (~1.17× slower than PubSub, ~3.67× the memory)
  - **NServiceBus** at 437 µs / 7.88 KB (~1.24× slower than PubSub, ~2.72× the memory)
- **Wolverine's 54 µs** is the cost of `BasicPublishAsync` returning once the bytes are on the socket — no broker round-trip. Faster *call* but a different *delivery guarantee*. PubSub deliberately doesn't expose a fire-and-forget mode because hung publishes were one of the SMB pain points the library was built to eliminate; safety beats a benchmark headline.

NServiceBus.RabbitMQ 11 also requires the broker's management API at endpoint startup (it pre-flights queue topology); the bench broker exposes port 15672 explicitly for this. The per-publish allocations include the per-message audit metadata NServiceBus stamps onto every send (correlation id, conversation id, content-type negotiation), which is structural to NServiceBus's saga / outbox model — not a fixable overhead for a feature-equivalent comparison.

## Targets

See `specs/phase-29-pubsub-rabbitmq/requirements.md` §Performance bar. Headline:

- ≥ 100 000 msg/sec sustained publish throughput (requires concurrency; single-publisher single-connection caps out around 3 k msg/sec because each call blocks on the confirm round-trip).
- Publish-confirm p99 < 5 ms (achieved — see latency tables in BDN output).
- 0 B allocated per published message post-warmup (not yet — source-generated dispatch is the lever; deferred to a follow-up phase).

## Baseline

`baseline.json` is the committed floor the CI gate compares against. Regression budget: ≤ 5 % throughput drop, ≤ 10 % latency increase, no non-zero-allocation regression. Regenerated on PRs labelled `pubsub-baseline`.
