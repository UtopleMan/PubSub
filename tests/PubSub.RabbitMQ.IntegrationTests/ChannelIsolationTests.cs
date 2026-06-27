using Microsoft.Extensions.DependencyInjection;
using Shouldly;
using Xunit;

namespace PubSub.RabbitMQ.IntegrationTests;

[Collection(RabbitMqCollection.Name)]
public class ChannelIsolationTests(RabbitMqFixture rmq)
{
    [Fact]
    public async Task Slow_consumer_does_not_block_a_different_consumer()
    {
        using var host = HostBuilder.Build(rmq.ConnectionString, b =>
        {
            b.Publish<SlowMessage>();
            b.Publish<GammaMessage>();
            b.Subscribe<SlowMessage, SlowConsumer>();
            b.Subscribe<GammaMessage, GammaConsumer>();
        });
        await host.StartAsync();

        var slowPub = host.Services.GetRequiredService<PubSub.IPublish<SlowMessage>>();
        var gammaPub = host.Services.GetRequiredService<PubSub.IPublish<GammaMessage>>();
        var recorder = host.Services.GetRequiredService<MessageRecorder>();

        await slowPub.PublishAsync(new SlowMessage(1));
        await recorder.SlowStarted.Task.WaitAsync(TimeSpan.FromSeconds(10));

        for (int i = 0; i < 10; i++)
            await gammaPub.PublishAsync(new GammaMessage($"g-{i}"));

        await WaitFor(() => recorder.Gamma.Count >= 10, TimeSpan.FromSeconds(10));
        recorder.Gamma.Count.ShouldBe(10);

        recorder.ReleaseSlow.TrySetResult();
        await WaitFor(() => recorder.Slow.Count >= 1, TimeSpan.FromSeconds(5));
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
