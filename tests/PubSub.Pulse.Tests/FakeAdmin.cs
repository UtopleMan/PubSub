using PubSub.Admin;

namespace PubSub.Pulse.Tests;

/// <summary>Canned <see cref="IPubSubAdmin"/> for host/endpoint tests — no broker involved.</summary>
internal sealed class FakeAdmin : IPubSubAdmin
{
    public ReplayRequest? LastReplay { get; private set; }
    public DeleteRequest? LastDelete { get; private set; }

    public Task<MessagingInstallation> GetInstallationAsync(CancellationToken cancellationToken = default)
        => Task.FromResult(new MessagingInstallation("RabbitMQ", "amqp://localhost:5672", "/", "Testing", true));

    public Task<TopologySnapshot> GetTopologyAsync(CancellationToken cancellationToken = default)
        => Task.FromResult(new TopologySnapshot(
            [new ExchangeInfo("shop.events", 1, 12.5)],
            [new TopicBinding("shop.events", "orders.placed", 1, 12.5, 0)]));

    public Task<IReadOnlyList<QueueStat>> GetQueueStatsAsync(CancellationToken cancellationToken = default)
        => Task.FromResult<IReadOnlyList<QueueStat>>(
        [
            new QueueStat(
                Queue: "shop.events.orders.placed.OrderConsumer",
                Exchange: "shop.events", RoutingKey: "orders.placed",
                Endpoint: "Shop.Api", Consumer: "OrderConsumer", Prefetch: 50,
                PublishRate: 10, ConsumeRate: 12.5, Depth: 6000, Consumers: 2,
                DlqRate: 0.2, ErrorPercent: 1.6, HandlerP95Ms: 42, InFlightAgeMs: 1200,
                ConsumeSeries: [10, 11, 12.5], Status: QueueHealth.Warning),
        ]);

    public Task<PubSubKpis> GetKpisAsync(CancellationToken cancellationToken = default)
        => Task.FromResult(new PubSubKpis(
        [
            new PubSubKpi("Publish rate", "pubsub.publish.count", 10, "msg/s", 4.2, false, [8, 9, 10]),
        ]));

    public Task<IReadOnlyList<FailedMessage>> GetFailedMessagesAsync(FailedQuery query, CancellationToken cancellationToken = default)
        => Task.FromResult<IReadOnlyList<FailedMessage>>(
        [
            new FailedMessage(
                Id: "abc123", ErrorQueue: "shop.events.orders.placed.OrderConsumer.error",
                Exchange: "shop.events", RoutingKey: "orders.placed",
                Consumer: "OrderConsumer", Endpoint: "Shop.Api",
                ExceptionType: "System.InvalidOperationException", ExceptionMessage: "boom",
                HandlerElapsedMs: 30, Pod: "pod-1", LastSeen: DateTimeOffset.UnixEpoch, Count: 1,
                Headers: [new FailedMessageHeader("x-exception-type", "System.InvalidOperationException")],
                StackTrace: "at OrderConsumer.Handle", ShovelCommand: "rabbitmqadmin ... shovel ..."),
        ]);

    public Task<ReplayResult> ReplayAsync(ReplayRequest request, CancellationToken cancellationToken = default)
    {
        LastReplay = request;
        return Task.FromResult(new ReplayResult(request.MessageIds.Count == 0 ? 3 : request.MessageIds.Count, 0));
    }

    public Task<DeleteResult> DeleteAsync(DeleteRequest request, CancellationToken cancellationToken = default)
    {
        LastDelete = request;
        return Task.FromResult(new DeleteResult(request.MessageIds.Count == 0 ? 3 : request.MessageIds.Count, 0));
    }
}
