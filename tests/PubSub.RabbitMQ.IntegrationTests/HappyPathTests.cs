using Microsoft.Extensions.DependencyInjection;
using Shouldly;
using Xunit;

namespace PubSub.RabbitMQ.IntegrationTests;

[Collection(RabbitMqCollection.Name)]
public class HappyPathTests(RabbitMqFixture rmq)
{
    [Fact]
    public async Task Publish_100_messages_consumed_in_order()
    {
        using var host = HostBuilder.Build(rmq.ConnectionString, b =>
        {
            b.Publish<AlphaMessage>();
            b.Subscribe<AlphaMessage, AlphaConsumer>();
        });
        await host.StartAsync();

        var publisher = host.Services.GetRequiredService<PubSub.IPublish<AlphaMessage>>();
        var recorder = host.Services.GetRequiredService<MessageRecorder>();

        for (int i = 0; i < 100; i++)
            await publisher.PublishAsync(new AlphaMessage(i, $"payload-{i}"));

        await WaitFor(() => recorder.Alpha.Count >= 100, TimeSpan.FromSeconds(15));
        recorder.Alpha.Count.ShouldBe(100);
        recorder.Alpha.Select(m => m.Id).OrderBy(i => i).ShouldBe(Enumerable.Range(0, 100));

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
