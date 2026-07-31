# PubSub.RabbitMQ

Tightly-integrated pub/sub for .NET on top of `RabbitMQ.Client` v7. Focused on one job — moving messages from publishers to subscribers — and built around the operational gotchas that other libraries inflict in silence.

## Why this library exists

Specifically, every silent failure mode that bit us in production:

- **Empty DLQ exception headers + replay friction** → Every consumer gets its own paired error queue (`{queue}` ↔ `{queue}.error`, MassTransit-style), each error entry carries `x-exception-type`, `x-exception`, `x-stacktrace`, `x-consumer-handler`, `x-handler-elapsed-ms`, `x-original-routing-key`, `x-pod-name`. Replay is one shovel: `rabbitmqadmin shovel --from={queue}.error --to={queue}`.
- **Hung publisher confirms pinning a worker forever** → every `IPublish<T>.PublishAsync` runs under a linked CTS firing after the configured timeout. Hung confirms throw `PubSubPublishTimeoutException`, not deadlock.
- **One stuck consumer blocking every other consumer on the same channel** → **channel-per-consumer isolation**. Every `ISubscribeTo<T>` registration owns its own AMQP channel via the shared `IConnection`. A wedged handler cannot block any other consumer.
- **Silent-hang invisibility** → `pubsub.consumer.in_flight_age_ms` observable gauge faceted by `queue` + `consumer_tag` + `handler_type`. A 90-second alert turns a 30-minute "everything looks fine" wedge into a pager event.
- **Reconnect-loop leaks from duplicate auto-recovery layers** → we use `RabbitMQ.Client` v7's native recovery only. No second recovery layer.
- **Verbose producer registration** → routing-key lives once on the contract via `[PubSubTopic]`; producers and consumers both read it. No `.Exchange().RoutingKeyProvider()` boilerplate per type.

## Quickstart

```csharp
using PubSub;
using PubSub.RabbitMQ;
using Microsoft.Extensions.Hosting;

[PubSubTopic("orders.placed", Exchange = "shop.events")]
public sealed record OrderPlaced(Guid OrderId, decimal Total);

public sealed class OrderPlacedConsumer(IOrderProjection projection) : ISubscribeTo<OrderPlaced>
{
    public Task Handle(OrderPlaced message, CancellationToken ct)
        => projection.ApplyAsync(message, ct);
}

var builder = Host.CreateApplicationBuilder();
builder.Services.AddPubSubRabbitMq(new PubSubRabbitMqOptions
{
    ConnectionString = "amqp://user:pass@rabbit.svc:5672/",
}, b =>
{
    b.Publish<OrderPlaced>();
    b.Subscribe<OrderPlaced, OrderPlacedConsumer>();
});
var app = builder.Build();
await app.RunAsync();

// Anywhere in your code:
var pub = app.Services.GetRequiredService<IPublish<OrderPlaced>>();
await pub.PublishAsync(new OrderPlaced(Guid.NewGuid(), 99.50m));
```

## Attributes

All four live in the shared `PubSub` package. A future `PubSub.Kafka` implementation reads the same attributes with Kafka-native semantics — application code stays portable.

| Attribute | Placement | Default | Purpose |
|---|---|---|---|
| `[PubSubTopic("routing.key", Exchange = "...")]` | Message contract | `Exchange = "phoenix.events"` | Routing-key + exchange. Required. |
| `[PublishTimeout(seconds)]` | Message contract or publisher impl | 10 s | Per-publish deadline. Wraps the publisher-confirm round-trip. |
| `[ConsumerPrefetch(count)]` | `ISubscribeTo<T>` impl | 50 | `basic.qos` for the consumer's channel. |

## Configured behaviour

| Concern | Behaviour |
|---|---|
| Connection | One `IConnection` per process. Native auto-recovery on. Exposed as DI singleton. |
| Publishing | Confirms required. Per-publish deadline via linked CTS. Hung confirms throw `PubSubPublishTimeoutException`. |
| Consuming | One `IChannel` per registration. `basic.qos = ConsumerPrefetch`. Handler resolved per-message from a `IServiceScope`. |
| Acks | Success ⇒ `BasicAckAsync(deliveryTag, multiple: false)`. Failure ⇒ enriched DLX publish on a side channel + `BasicNackAsync(deliveryTag, multiple: false, requeue: false)`. |
| Topology | Declared by every consumer host at `StartAsync` in deterministic order. Exchange → DLX → DLQ → DLQ binding → consumer queue + binding. Idempotent. |
| Metrics | `pubsub.consumer.in_flight_age_ms` (observable gauge), `pubsub.publish.count`, `pubsub.consume.count`, `pubsub.dlq.publish.count` (counters). |
| Tracing | Activity source `PubSub.RabbitMQ`. Publish spans inject `traceparent`/`tracestate` into headers; consume spans extract them and continue the trace. |

## Monitoring console (`PubSub.Pulse`)

`PubSub.Pulse` is a self-hosted Blazor WebAssembly console that shows live statistics for a
PubSub installation — publish/consume/DLQ rates, per-queue depth and in-flight age, handler
p95, and the contents of every `.error` queue with one-click replay/delete. The whole WASM app
is embedded in the `PubSub.Pulse` assembly, so there is nothing extra to deploy.

```csharp
builder.Services.AddPubSubRabbitMq(new PubSubRabbitMqOptions { ConnectionString = "amqp://…" }, b =>
{
    b.Publish<OrderPlaced>();
    b.Subscribe<OrderPlaced, OrderPlacedConsumer>();
});

builder.Services.AddPubSubRabbitMqAdmin(o => o.ServiceName = "Shop.Api"); // exposes IPubSubAdmin
builder.Services.AddPubSubPulse();                                        // console options + JSON

var app = builder.Build();
app.UsePubSubPulse("/pulse");                                            // mount the console
app.Run();
```

Browse to `/pulse`. Mount it behind your own auth if the stats should not be public.

- **No management plugin required.** Depth and consumer counts come from AMQP passive declares,
  failed messages from peeking the `.error` queues, and rates/p95/in-flight from the library's
  own in-process meters (so the rates reflect *this* process).
- **Vendor-neutral.** The console talks to `IPubSubAdmin` (in the core `PubSub` package). The
  RabbitMQ implementation ships in `PubSub.RabbitMQ`; a future backend can drive the same UI.
- **Runnable sample.** `samples/PubSub.Pulse.Sample` wires the above with demo traffic (incl. a
  deliberately flaky consumer). Start RabbitMQ with its `docker-compose.yml`, then
  `dotnet run --project samples/PubSub.Pulse.Sample` and open `http://localhost:5080/pulse`.

## Error queue + replay playbook

Each consumer registration auto-declares a paired error queue: main queue `{exchange}.{routing-key}` and error queue `{exchange}.{routing-key}.error`. When `ISubscribeTo<T>.Handle` throws, the library publishes the original body to `phoenix.dlx` with the original routing-key (so the broker routes it to *that consumer's* error queue, not a shared bucket), and attaches these headers:

| Header | Meaning |
|---|---|
| `x-exception-type` | Full type name of the thrown exception (UTF-8 bytes) |
| `x-exception` | `ex.Message` (UTF-8 bytes) |
| `x-stacktrace` | `ex.ToString()` (UTF-8 bytes) |
| `x-consumer-handler` | `typeof(TConsumer).FullName` (UTF-8 bytes) |
| `x-handler-elapsed-ms` | Wall-clock ms from message-receipt to throw (int64) |
| `x-original-routing-key` | Original delivery's routing-key (UTF-8 bytes) |
| `x-pod-name` | `HOSTNAME` env at process start (UTF-8 bytes) |

**Peek a specific error queue:**
```bash
curl -u admin:admin http://rabbit:15672/api/queues/%2F/shop.events.orders.placed.error/get \
  -d '{"count":5,"ackmode":"ack_requeue_true","encoding":"auto","truncate":50000}'
```

**Replay all errors back to the main queue** (the operational headline — no filtering, no custom tool):
```bash
rabbitmqadmin --uri=amqp://admin:admin@rabbit:5672 \
  shovel \
  --src-queue=shop.events.orders.placed.error \
  --dest-exchange=shop.events \
  --dest-exchange-key=orders.placed
```

The exception headers map back to the failing handler's source line — no log-grep by timestamp.

## Standalone build

```bash
dotnet build vendor/PubSub/PubSub.sln
dotnet test  vendor/PubSub/tests/PubSub.Tests
dotnet test  vendor/PubSub/tests/PubSub.RabbitMQ.IntegrationTests   # requires Docker (OrbStack works)
dotnet run -c Release --project vendor/PubSub/bench/PubSub.RabbitMQ.Bench -- --filter '*'
```

## Status

In-tree at `vendor/PubSub/` so it can be lifted out into a standalone repo + NuGet package once it has proved itself. Scope of this phase: library + tests + own-bench harness. The full performance bar (publish ≥100k msg/s, 0 B/msg allocations, head-to-head vs SMB / MassTransit / Wolverine, CI gate on `baseline.json`) lands in a follow-up — see `specs/phase-29-pubsub-rabbitmq/`.
