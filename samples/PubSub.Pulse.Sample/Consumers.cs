using PubSub;

namespace PubSub.Pulse.Sample;

public sealed class OrderProjector : ISubscribeTo<OrderPlaced>
{
    public Task Handle(OrderPlaced message, CancellationToken cancellationToken) => Task.CompletedTask;
}

public sealed class PaymentPoster : ISubscribeTo<PaymentCaptured>
{
    public async Task Handle(PaymentCaptured message, CancellationToken cancellationToken)
        => await Task.Delay(Random.Shared.Next(5, 40), cancellationToken); // a little handler latency for p95
}

// Deliberately flaky, so the console's Failed tab and DLQ metrics have something to show.
public sealed class FlakyReserver : ISubscribeTo<InventoryReserved>
{
    public Task Handle(InventoryReserved message, CancellationToken cancellationToken)
    {
        if (Random.Shared.NextDouble() < 0.35)
            throw new InvalidOperationException($"Out of stock for SKU {message.Sku} (order {message.OrderId:N})");
        return Task.CompletedTask;
    }
}
