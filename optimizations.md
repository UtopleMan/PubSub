# PubSub.RabbitMQ — Performance Optimizations

Deep-dive findings from a hot-path audit + the head-to-head bench numbers (PubSub 348.74 µs / 2.89 KB per publish, MassTransit 454.34 µs / 10.63 KB, Wolverine 42.28 µs / 2.61 KB — confirm-wait semantics noted in `bench/README.md`). Captured here so a future session can pick up without re-deriving the analysis.

## Publish hot path — `RabbitMqPublisher<T>.PublishAsync`

| # | Source | Estimated B/call | File:line |
|---|---|---:|---|
| 1 | `JsonSerializer.SerializeToUtf8Bytes(message)` returns a fresh `byte[]` | ~1 024 | `RabbitMqPublisher.cs:56` |
| 2 | `new BasicProperties { Headers = new Dictionary<string,object?>() }` | ~400 | `RabbitMqPublisher.cs:59–64` |
| 3 | `CancellationTokenSource.CreateLinkedTokenSource(ct)` + internal `Timer` | ~150 | `RabbitMqPublisher.cs:45` |
| 4 | `ActivitySource.StartActivity(...)` + 4× `SetTag` (when listener attached) | ~300 | `RabbitMqPublisher.cs:49–54` |
| 5 | `Encoding.UTF8.GetBytes(activity.Id)` + `tracestate` (when Activity present) | ~200 | `RabbitMqPublisher.cs:68–70` |
| 6 | `$"pubsub.publish {_topic.RoutingKey}"` interpolation per call | ~80 | `RabbitMqPublisher.cs:50` |
| 7 | `Stopwatch.StartNew()` | ~50 | `RabbitMqPublisher.cs:47` |
| 8 | `KeyValuePair<string,object?>("routing_key", …)` for counter | ~40 | `RabbitMqPublisher.cs:83` |
| 9 | Async state machine + `Task` (unavoidable while signature is `Task PublishAsync`) | ~150 | implicit |

**Measured total**: 2.89 KB. The first three items account for ~55 % of the budget.

## Consume hot path — `RabbitMqConsumerHost<T,TConsumer>.OnReceivedAsync`

Measured by the consume-roundtrip head-to-head bench. PubSub: 3.47 ms median per roundtrip; Wolverine: 1.55 ms — **PubSub is ~2.2× slower on the consume path** and the gap is *not* a publish-side or broker-routing artefact (verified by the `diagnose-wolverine` probe: all messages physically transit the broker in both cases).

The gap is the per-message dispatch overhead. Wolverine runtime-compiles a per-`T` dispatcher via `JasperFx.CodeGeneration` at startup — inlined deserialize + handler resolution + invoke. Our code does this every message via reflection + DI. Reading the source:

| Source | File:line | Notes |
|---|---|---|
| `_services.CreateScope()` per message | `RabbitMqConsumerHost.cs:127` | Allocates `IServiceScope` + scoped `IServiceProvider`. Necessary when handler injects scoped deps; wasted when consumer is registered singleton. |
| `JsonSerializer.Deserialize<T>(ea.Body.Span)` reflection path | `RabbitMqConsumerHost.cs:117` | No `JsonTypeInfo<T>` so reflection runs each call. |
| `ActivitySource.StartActivity(...)` regardless of listener | `RabbitMqConsumerHost.cs:91–94` | We don't check `HasListeners()` first — Activity always allocates if the source is enabled. |
| `Encoding.UTF8.GetString(tpBytes)` for `traceparent` | `RabbitMqConsumerHost.cs:87` | Allocates a `string` we immediately pass to `ActivityContext.TryParse`, which parses bytes anyway. |
| `ConcurrentDictionary<ulong,long>` add/remove per delivery | `RabbitMqConsumerHost.cs:81,160` | In-flight tracker for the heartbeat metric. Only useful when the gauge has subscribers. |
| `KeyValuePair<string,object?>` allocations for counter tags | several | Small but repeated. |

## "Hidden" allocations inside `RabbitMQ.Client` v7

We cannot directly control these without forking the upstream client, but they cap how close to 0 B/publish we can get:

- `BasicPublishAsync` allocates internal frame buffers (~200–500 B/call depending on payload size).
- Publisher-confirm tracker holds per-delivery state until the broker `basic.ack` arrives.
- `IChannel.BasicPublishAsync` returns a `ValueTask` but internally awaits a `TaskCompletionSource` for the confirm.

Realistic floor: ~500 B/publish *after* all our library-side allocations are gone. The spec's "0 B allocated per published message post-warmup" target is therefore aspirational — closer reading is "0 B in **our** code", with the RMQ.Client overhead acknowledged.

## Throughput vs. per-call allocations

Two different optimization axes:

- **Per-call allocations** (this doc's focus) — measured by `[MemoryDiagnoser]`, currently 2.89 KB. Wins compound across high call volumes but don't change the single-publisher latency floor.
- **Throughput per second** — currently capped at ~3 k msg/s per single-publisher single-connection because each call awaits the broker confirm. To approach the spec's **≥ 100 000 msg/sec** target needs either (a) many concurrent publishers / connections, or (b) batched / pipelined publishes (multiple `BasicPublishAsync` calls before awaiting the confirm batch). The library has no batching API today.

These are different engagements; this doc is allocation-focused.

## Optimization bundles (in increasing scope)

### Bundle 1 — Lazy-OTel + pooled body buffer (Recommended next step)

Low risk, biggest bang-for-buck. ~70 % expected drop in publish allocations.

1. **`JsonSerializer.SerializeToUtf8Bytes` → pooled `ArrayBufferWriter<byte>` rented from `ArrayPool<byte>.Shared`.** Returns the buffer after `BasicPublishAsync` completes. Eliminates item #1 (~1 KB).
2. **`ActivitySource.HasListeners()` short-circuit** at the top of the method. When no listener is attached (bench mode, dev mode, prod-without-OTel-collector), skip every `Activity`-related alloc, the `Stopwatch`, and the traceparent header bytes. Eliminates items #4, #5, #6, #7 in that mode (~630 B).
3. **Lazy `Headers` Dictionary** — only allocate when there's something to put in it. Most messages today have headers iff Activity is on, so this rides on #2. Eliminates ~250 B of item #2.
4. **`Stopwatch.StartNew()` → `Environment.TickCount64` arithmetic.** Eliminates item #7 (~50 B) even when Activity is on.
5. **Pre-resolve the interpolated span name** `pubsub.publish {routing-key}` once per publisher instance, store on the field. Eliminates item #6 (~80 B/call).

**Effort**: ~30 LOC across `RabbitMqPublisher.cs`. No public-API change. ~20 minutes of work, plus a benchmark re-run to confirm the drop.

### Bundle 2 — Bundle 1 + pooled `CancellationTokenSource` + `BasicProperties`

Medium risk, smaller incremental win.

6. **Pool `CancellationTokenSource`** via `Microsoft.Extensions.ObjectPool.ObjectPool<CancellationTokenSource>` with `CancellationTokenSource.TryReset()` on return. Eliminates item #3 (~150 B). Risk: a CTS that fires while still in the pool corrupts the next user; needs careful `Reset` semantics.
7. **Thread-local `BasicProperties` pool**. Risk: `RabbitMQ.Client` may capture the object beyond the `await` (unconfirmed — needs source-reading). If safe, eliminates the BasicProperties alloc (item #2, ~150 B).
8. **Cached `KeyValuePair<string,object?>`** for the counter tag — pre-allocate as a static field, reuse. Eliminates item #8.

**Effort**: ~80 LOC, +tests for pool correctness. ~1 hour. Higher correctness risk than Bundle 1.

### Bundle 3 — Source-generator + everything-pooled (**partially shipped**)

The dispatcher pattern + Roslyn source generator are now in place, but the source-gen JSON path required by the spec's 0-B target is **blocked by a Roslyn generator-interop limitation**.

**Shipped:**

- `IPubSubDispatcher<T>` interface + `PubSubDispatcherRegistry` lookup, both in `PubSub` (transport-agnostic — a future `PubSub.Kafka` reuses the same registry).
- `ReflectionPubSubDispatcher<T>` as the default fallback (`JsonSerializer.Serialize<T>` / `Deserialize<T>` with the type baked into the dispatcher's static state).
- `PubSubDispatcherGenerator` (incremental Roslyn generator in `PubSub.SourceGenerators`) — discovers every `[PubSubTopic]`-marked type in the consuming assembly and emits a per-`T` `PubSubDispatcher_<T>` class + a `[ModuleInitializer]` that registers each dispatcher into `PubSubDispatcherRegistry` at startup.
- `RabbitMqPublisher<T>`, `RabbitMqFireAndForgetPublisher<T>`, `RabbitMqBatchPublisher<T>`, `RabbitMqConsumerHost<T,TConsumer>` all use `PubSubDispatcherRegistry.GetOrFallback<T>()` (cached per-instance in a static field) for serialize / deserialize / handler invoke.

**Architecture wins (independent of perf):**

- Future `PubSub.Kafka` plugs into the same registry — no per-transport reimplementation of the dispatcher discovery story.
- Users can register their own `IPubSubDispatcher<T>` via `PubSubDispatcherRegistry.Register<T>(...)` to plug in a custom serializer (Protobuf, MessagePack, source-gen JSON, etc.) without library changes.
- The `JasperFx.CodeGeneration` equivalent we sketched is now in our codebase — just not yet emitting STJ source-gen JSON.

**Update: Option A is shipped (hand-emit `JsonObjectInfoValues<T>`).**

The generator now produces:
- A full `PubSubJsonContext : JsonSerializerContext` partial with the required `GetTypeInfo` + `GeneratedSerializerOptions` overrides (no dependency on STJ source-gen).
- A `JsonTypeInfo<T>` per `[PubSubTopic]` type built via `JsonMetadataServices.CreateObjectInfo<T>(...)` with hand-emitted `JsonObjectInfoValues<T>` — constructor + property metadata baked in at compile time, zero reflection for the top-level record.
- For property types we haven't hand-emitted (primitives like `int`, `string`, `Guid`, etc.), the context's `GetTypeInfo` falls back to `DefaultJsonTypeInfoResolver` — those types use reflection-built metadata (cached per type).

**Measured impact (consume roundtrip median):** 3.47 ms → 2.44 ms (~30% drop), closing toward Wolverine's 1.7 ms tier. Allocations stayed at ~8 KB per roundtrip because the reflection fallback still allocates `JsonTypeInfo` for `int`/`string`/etc. on first use; pure source-gen JSON would close the rest.

**Original blocker explanation (kept for posterity):**

The plan was for the generator to emit `[JsonSerializable(typeof(T))] partial class PubSubJsonContext : JsonSerializerContext { }` and have the System.Text.Json source generator fill in `GetTypeInfo` + `GeneratedSerializerOptions`. **Roslyn runs all source generators in parallel** — STJ's generator doesn't observe outputs from ours in the same compilation pass. Result: `error CS0534: 'PubSubJsonContext' does not implement inherited abstract member 'JsonSerializerContext.GetTypeInfo(Type)'`. We worked around by emitting the metadata implementation ourselves (Option A).

Workarounds (any one closes the gap):

- **(A) User-written `JsonSerializerContext`.** Document that the user writes their own `[JsonSerializable]` partial class in user code (STJ's generator sees it on the first pass), and our generator reads `[JsonSerializable]` attributes to wire the per-`T` dispatcher. Most idiomatic; user opts in per-project.
- **(B) Emit full `JsonTypeInfo<T>` metadata by hand.** Our generator emits the `JsonTypeInfo<T>` construction code directly via `JsonMetadataServices.CreateObjectInfo(...)`, bypassing `[JsonSerializable]`. Verbose (must walk every property), but no interop dependency.
- **(C) Multi-pass via target compilation.** Emit the `[JsonSerializable]` partial in a NuGet-time props file or a build step that runs before the main compilation — so STJ sees it on its only pass. Awkward MSBuild plumbing.

Recommend (A) — write a user-side `JsonSerializerContext` per consumer project, have our generator's emitted dispatcher reference it. Cost: one extra ~10-line file per consumer project. Benefit: STJ source-gen JSON applies; allocations drop further; AOT-clean.

**Other Bundle 3 items still pending:**

- Consume-side: `Activity` only when `HasListeners()` (we did this in publish; consume still allocates Activity unconditionally — easy win).
- Consume-side: traceparent extraction from `ReadOnlySpan<byte>` (currently allocates a `string`).
- Consume-side: pool `IServiceScope` when consumer is registered singleton (needs a `Subscribe<T, TConsumer>(ServiceLifetime)` overload).
- Conditional in-flight tracker (only populate dict when meter has listeners).

**Effort to fully close the gap**: ~200 LOC across 5 files. Bumps consume-roundtrip closer to Wolverine's 1.5 ms tier.

### Bundle 4 — Diagnose first

Lower-confidence option if you suspect my breakdown above is wrong.

Run `dotnet-trace collect -- dotnet run -c Release --project bench --filter '*PubSub*Publish*'` with the `gc-heap-collect` provider, then attribute allocations empirically. Verifies the table at the top of this doc; might surface hidden costs (e.g., LoggerFactory.CreateLogger allocations, RMQ.Client internal frame buffers).

**Effort**: ~30 minutes, mostly waiting for the trace to render.

## Public-API decisions still open

- **Source generator: required or optional?** Spec currently says required (build-error PUBSUB001). Bundle 3 lands this. If we ship Bundle 1 or 2 only, generator stays "deferred" and reflection-based serialize is the default path.
- **`ValueTask` vs `Task` on `IPublish<T>`?** `Task` is allocated even when the publish completes synchronously; `ValueTask` saves the alloc in the fast path. Breaking change to `PubSub.Interfaces` if we switch.
- **Expose a `BatchPublish` API?** Not in scope here; flagged for the throughput-axis engagement.

## Suggested next move

Implement **Bundle 1** in a focused PR, re-run the head-to-head bench, commit the new `baseline.json` if numbers regress favourably. That brings allocations to ~600–900 B/publish and gives us a solid data point before deciding whether Bundle 2/3 is worth its complexity. If we ever ship the public NuGet, Bundle 3 becomes a requirement for the "fastest .NET pub/sub on RabbitMQ" headline; otherwise Bundle 1 is enough for the in-tree consumer.
