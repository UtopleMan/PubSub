using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Shouldly;
using Xunit;

namespace PubSub.RabbitMQ.IntegrationTests;

[Collection(RabbitMqCollection.Name)]
public class FireAndForgetPublishTests(RabbitMqFixture rmq)
{
    [Fact]
    public async Task FireAndForget_publish_arrives_at_consumer()
    {
        using var host = HostBuilder.Build(rmq.ConnectionString, b =>
        {
            b.Publish<FnfHeartbeat>();
            b.Subscribe<FnfHeartbeat, FnfConsumer>();
        }, svc => svc.AddSingleton<FnfRecorder>());
        await host.StartAsync();

        var pub = host.Services.GetRequiredService<PubSub.IFireAndForgetPublish<FnfHeartbeat>>();
        var rec = host.Services.GetRequiredService<FnfRecorder>();

        for (int i = 0; i < 25; i++)
            await pub.PublishAsync(new FnfHeartbeat(i));

        await WaitFor(() => rec.Bag.Count >= 25, TimeSpan.FromSeconds(15));
        rec.Bag.Count.ShouldBe(25);

        await host.StopAsync();
    }

    [Fact]
    public void Wrong_interface_throws_at_resolution()
    {
        using var host = HostBuilder.Build(rmq.ConnectionString, b => b.Publish<FnfHeartbeat>(),
            svc => svc.AddSingleton<FnfRecorder>());
        Should.Throw<InvalidOperationException>(
            () => host.Services.GetRequiredService<PubSub.IPublish<FnfHeartbeat>>());
    }

    private static async Task WaitFor(Func<bool> condition, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (condition()) return;
            await Task.Delay(50);
        }
        throw new TimeoutException($"Condition not satisfied within {timeout}");
    }
}

[PubSub.PubSubTopic("test.fnf", Exchange = "pubsub.itests")]
[PubSub.FireAndForget]
public sealed record FnfHeartbeat(int Tick);

public sealed class FnfRecorder
{
    public ConcurrentBag<FnfHeartbeat> Bag { get; } = new();
}

public sealed class FnfConsumer(FnfRecorder rec) : PubSub.ISubscribeTo<FnfHeartbeat>
{
    public Task Handle(FnfHeartbeat message, CancellationToken cancellationToken)
    {
        rec.Bag.Add(message);
        return Task.CompletedTask;
    }
}
