using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using PubSub;
using PubSub.Admin;
using PubSub.RabbitMQ.Admin;
using Shouldly;
using Xunit;

namespace PubSub.RabbitMQ.IntegrationTests;

// A consumer whose failure is switchable, so a replayed message can succeed the second time
// (a replayed *failing* message would just loop straight back to its error queue).
[PubSubTopic("admin.toggle", Exchange = "pubsub.itests")]
public sealed record ToggleMessage(string Id);

public sealed class ToggleState
{
    public volatile bool Fail = true;
    public ConcurrentBag<string> Ok { get; } = new();
    public ConcurrentBag<string> Failed { get; } = new();
}

public sealed class ToggleConsumer(ToggleState state) : ISubscribeTo<ToggleMessage>
{
    public Task Handle(ToggleMessage message, CancellationToken cancellationToken)
    {
        if (state.Fail)
        {
            state.Failed.Add(message.Id);
            throw new InvalidOperationException($"toggle fail {message.Id}");
        }
        state.Ok.Add(message.Id);
        return Task.CompletedTask;
    }
}

[Collection(RabbitMqCollection.Name)]
public class AdminApiTests(RabbitMqFixture rmq)
{
    [Fact]
    public async Task Admin_reports_stats_failed_messages_and_replays_and_deletes()
    {
        var toggle = new ToggleState();
        using var host = HostBuilder.Build(rmq.ConnectionString, b =>
        {
            b.Publish<AlphaMessage>();
            b.Subscribe<AlphaMessage, AlphaConsumer>();
            b.Publish<ToggleMessage>();
            b.Subscribe<ToggleMessage, ToggleConsumer>();
        }, extra: s =>
        {
            s.AddSingleton(toggle);
            s.AddPubSubRabbitMqAdmin(o =>
            {
                o.ServiceName = "itest";
                o.ManagementBaseUrl = new Uri(rmq.ManagementUri);
                o.ManagementUser = rmq.ManagementUser;
                o.ManagementPassword = rmq.ManagementPassword;
                o.VHost = "/";
                o.ConnectionString = rmq.ConnectionString;
            });
        });
        await host.StartAsync();
        var admin = host.Services.GetRequiredService<IPubSubAdmin>();
        var recorder = host.Services.GetRequiredService<MessageRecorder>();

        var install = await admin.GetInstallationAsync();
        install.Provider.ShouldBe("RabbitMQ");
        install.Connected.ShouldBeTrue();
        install.Endpoint.ShouldStartWith("amqp://");

        var pub = host.Services.GetRequiredService<IPublish<AlphaMessage>>();
        for (var i = 0; i < 3; i++) await pub.PublishAsync(new AlphaMessage(i, $"p{i}"));
        await WaitFor(() => recorder.Alpha.Count >= 3, TimeSpan.FromSeconds(10));

        await WaitFor(async () =>
        {
            var s = await StatForRoutingKey(admin, "test.alpha");
            return s is { Depth: 0, Consumers: >= 1 };
        }, TimeSpan.FromSeconds(10));

        var alpha = await StatForRoutingKey(admin, "test.alpha");
        alpha.ShouldNotBeNull();
        alpha!.Endpoint.ShouldBe("itest");
        alpha.ConsumeRate.ShouldBeGreaterThanOrEqualTo(0);
        (await StatForRoutingKey(admin, "admin.toggle")).ShouldNotBeNull();

        toggle.Fail = true;
        var tp = host.Services.GetRequiredService<IPublish<ToggleMessage>>();
        await tp.PublishAsync(new ToggleMessage("m1"));

        FailedMessage? failed = null;
        await WaitFor(async () =>
        {
            var list = await admin.GetFailedMessagesAsync(new FailedQuery(Search: "toggle"));
            failed = list.FirstOrDefault();
            return failed is not null;
        }, TimeSpan.FromSeconds(10));

        failed!.ExceptionType.ShouldContain("InvalidOperationException");
        failed.ExceptionMessage.ShouldContain("toggle fail m1");
        failed.StackTrace.ShouldContain("ToggleConsumer");
        failed.Pod.ShouldBe("pubsub-itest");
        failed.ErrorQueue.ShouldEndWith(".error");
        failed.Headers.ShouldContain(h => h.Key == "x-exception-type");
        failed.ShovelCommand.ShouldContain("--src-queue=" + failed.ErrorQueue);

        toggle.Fail = false;
        var replay = await admin.ReplayAsync(new ReplayRequest(failed.ErrorQueue, [failed.Id]));
        replay.Replayed.ShouldBe(1);
        replay.NotFound.ShouldBe(0);
        await WaitFor(() => toggle.Ok.Contains("m1"), TimeSpan.FromSeconds(10));
        await WaitFor(async () =>
            (await admin.GetFailedMessagesAsync(new FailedQuery(Search: "toggle"))).Count == 0,
            TimeSpan.FromSeconds(10));

        toggle.Fail = true;
        await tp.PublishAsync(new ToggleMessage("m2"));
        string errorQueue = failed.ErrorQueue;
        await WaitFor(async () =>
            (await admin.GetFailedMessagesAsync(new FailedQuery(Search: "toggle"))).Count >= 1,
            TimeSpan.FromSeconds(10));

        var del = await admin.DeleteAsync(new DeleteRequest(errorQueue, []));
        del.Deleted.ShouldBeGreaterThanOrEqualTo(1);
        await WaitFor(async () =>
            (await admin.GetFailedMessagesAsync(new FailedQuery(Search: "toggle"))).Count == 0,
            TimeSpan.FromSeconds(10));

        await host.StopAsync();
    }

    private static async Task<QueueStat?> StatForRoutingKey(IPubSubAdmin admin, string routingKey)
    {
        var stats = await admin.GetQueueStatsAsync();
        return stats.FirstOrDefault(s => s.RoutingKey == routingKey);
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

    private static async Task WaitFor(Func<Task<bool>> condition, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (await condition()) return;
            await Task.Delay(100);
        }
        throw new TimeoutException($"Condition not satisfied within {timeout}");
    }
}
