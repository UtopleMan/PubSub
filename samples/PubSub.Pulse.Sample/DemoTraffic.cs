using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using PubSub;

namespace PubSub.Pulse.Sample;

/// <summary>Publishes a steady trickle of demo messages so the console has live data.</summary>
public sealed class DemoTraffic(IServiceProvider services) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var orders = services.GetRequiredService<IPublish<OrderPlaced>>();
        var payments = services.GetRequiredService<IPublish<PaymentCaptured>>();
        var inventory = services.GetRequiredService<IPublish<InventoryReserved>>();

        // Give the broker/topology a moment to settle.
        try { await Task.Delay(2000, stoppingToken); } catch (OperationCanceledException) { return; }

        var skus = new[] { "SKU-1001", "SKU-2002", "SKU-3003", "SKU-4004" };
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var orderId = Guid.NewGuid();
                await orders.PublishAsync(new OrderPlaced(orderId, Random.Shared.Next(10, 500)), stoppingToken);
                await payments.PublishAsync(new PaymentCaptured(orderId, Random.Shared.Next(10, 500)), stoppingToken);
                await inventory.PublishAsync(
                    new InventoryReserved(orderId, skus[Random.Shared.Next(skus.Length)], Random.Shared.Next(1, 5)),
                    stoppingToken);
                await Task.Delay(TimeSpan.FromMilliseconds(400), stoppingToken);
            }
            catch (OperationCanceledException) { break; }
            catch { await Task.Delay(1000, stoppingToken); }
        }
    }
}
