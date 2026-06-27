using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Shouldly;
using Xunit;

namespace PubSub.RabbitMQ.IntegrationTests;

[Collection(RabbitMqCollection.Name)]
public class BatchedPublishTests(RabbitMqFixture rmq)
{
    [Fact]
    public async Task Batched_publish_100_messages_all_consumed()
    {
        using var host = HostBuilder.Build(rmq.ConnectionString, b =>
        {
            b.Publish<BatchedTickEvent>();
            b.Subscribe<BatchedTickEvent, BatchedTickConsumer>();
        }, svc => svc.AddSingleton<TickRecorder>());
        await host.StartAsync();

        var pub = host.Services.GetRequiredService<PubSub.IBatchPublish<BatchedTickEvent>>();
        var rec = host.Services.GetRequiredService<TickRecorder>();

        var tasks = new List<Task>(100);
        for (int i = 0; i < 100; i++)
            tasks.Add(pub.PublishAsync(new BatchedTickEvent(i, $"sym-{i}")));
        await Task.WhenAll(tasks);

        await WaitFor(() => rec.Bag.Count >= 100, TimeSpan.FromSeconds(15));
        rec.Bag.Count.ShouldBe(100);
        rec.Bag.Select(m => m.Id).OrderBy(i => i).ShouldBe(Enumerable.Range(0, 100));

        await host.StopAsync();
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

[PubSub.PubSubTopic("test.batched", Exchange = "pubsub.itests")]
[PubSub.BatchedPublish(20, FlushIntervalMs = 100)]
public sealed record BatchedTickEvent(int Id, string Symbol);

public sealed class TickRecorder
{
    public ConcurrentBag<BatchedTickEvent> Bag { get; } = new();
}

public sealed class BatchedTickConsumer(TickRecorder rec) : PubSub.ISubscribeTo<BatchedTickEvent>
{
    public Task Handle(BatchedTickEvent message, CancellationToken cancellationToken)
    {
        rec.Bag.Add(message);
        return Task.CompletedTask;
    }
}
