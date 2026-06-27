using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Shouldly;
using Xunit;

namespace PubSub.RabbitMQ.IntegrationTests;

[Collection(RabbitMqCollection.Name)]
public class DynamicRoutingKeyTests(RabbitMqFixture rmq)
{
    [Fact]
    public async Task IRoutingKeyProvider_routes_per_message_payload()
    {
        using var host = HostBuilder.Build(rmq.ConnectionString, b =>
        {
            b.Publish<TimedJob>();
            b.Subscribe<TimedJob, TimedJobRecorderConsumer>();
        }, svc => svc.AddSingleton<TimedJobRecorder>());
        await host.StartAsync();

        var publisher = host.Services.GetRequiredService<PubSub.IPublish<TimedJob>>();
        var recorder = host.Services.GetRequiredService<TimedJobRecorder>();

        await publisher.PublishAsync(new TimedJob("AAPL", "eod"));
        await publisher.PublishAsync(new TimedJob("MSFT", "intraday-min"));
        await publisher.PublishAsync(new TimedJob("VOD.L", "eod"));

        await WaitFor(() => recorder.Bag.Count >= 3, TimeSpan.FromSeconds(10));
        recorder.Bag.Count.ShouldBe(3);
        recorder.Bag.Select(j => j.Symbol).OrderBy(s => s).ShouldBe(new[] { "AAPL", "MSFT", "VOD.L" });

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

[PubSub.PubSubTopic("test.timed.*", Exchange = "pubsub.itests")]
public sealed record TimedJob(string Symbol, string Timeframe) : PubSub.IRoutingKeyProvider
{
    public string GetRoutingKey() => $"test.timed.{Timeframe}";
}

public sealed class TimedJobRecorder
{
    public ConcurrentBag<TimedJob> Bag { get; } = new();
}

public sealed class TimedJobRecorderConsumer(TimedJobRecorder rec) : PubSub.ISubscribeTo<TimedJob>
{
    public Task Handle(TimedJob message, CancellationToken cancellationToken)
    {
        rec.Bag.Add(message);
        return Task.CompletedTask;
    }
}
