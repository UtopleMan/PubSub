using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Threading;
using Microsoft.Extensions.DependencyInjection;
using Shouldly;
using Xunit;

namespace PubSub.RabbitMQ.IntegrationTests;

[Collection(RabbitMqCollection.Name)]
public class ShovelReplayTests(RabbitMqFixture rmq)
{
    [Fact]
    public async Task Failed_message_can_be_replayed_from_error_queue()
    {
        using var host = HostBuilder.Build(rmq.ConnectionString, b =>
        {
            b.Publish<FailOnceMessage>();
            b.Subscribe<FailOnceMessage, FailOnceConsumer>();
        }, svc =>
        {
            svc.AddSingleton<FailOnceState>();
        });
        await host.StartAsync();

        var publisher = host.Services.GetRequiredService<PubSub.IPublish<FailOnceMessage>>();
        var state = host.Services.GetRequiredService<FailOnceState>();

        await publisher.PublishAsync(new FailOnceMessage("KMF"));
        await WaitForAsync(async () => (await GetQueueDepthAsync("pubsub.itests.test.fail-once.error")) >= 1, TimeSpan.FromSeconds(15));
        state.Failures.ShouldBe(1);

        var moved = await ShovelAsync(from: "pubsub.itests.test.fail-once.error", toRoutingKey: "test.fail-once", toExchange: "pubsub.itests");
        moved.ShouldBe(1);

        await WaitFor(() => state.Successes >= 1, TimeSpan.FromSeconds(10));
        state.Successes.ShouldBe(1);
        await WaitForAsync(async () => (await GetQueueDepthAsync("pubsub.itests.test.fail-once.error")) == 0, TimeSpan.FromSeconds(5));

        await host.StopAsync();
    }

    private async Task<int> GetQueueDepthAsync(string queue)
    {
        using var http = MgmtHttp();
        var resp = await http.GetAsync($"/api/queues/%2F/{queue}");
        if (!resp.IsSuccessStatusCode) return 0;
        var json = await resp.Content.ReadAsStringAsync();
        using var doc = JsonDocument.Parse(json);
        return doc.RootElement.TryGetProperty("messages_ready", out var v) ? v.GetInt32() : 0;
    }

    private async Task<int> ShovelAsync(string from, string toRoutingKey, string toExchange)
    {
        using var http = MgmtHttp();
        var body = $$"""{"count":100,"ackmode":"ack_requeue_false","encoding":"auto","truncate":50000}""";
        var resp = await http.PostAsync($"/api/queues/%2F/{from}/get",
            new StringContent(body, Encoding.UTF8, "application/json"));
        resp.EnsureSuccessStatusCode();
        var json = await resp.Content.ReadAsStringAsync();
        using var doc = JsonDocument.Parse(json);
        var n = 0;
        foreach (var item in doc.RootElement.EnumerateArray())
        {
            var payload = item.GetProperty("payload").GetString() ?? string.Empty;
            var publishBody = $$"""
            {
              "properties": {},
              "routing_key": "{{toRoutingKey}}",
              "payload": {{JsonSerializer.Serialize(payload)}},
              "payload_encoding": "string"
            }
            """;
            var pubResp = await http.PostAsync($"/api/exchanges/%2F/{toExchange}/publish",
                new StringContent(publishBody, Encoding.UTF8, "application/json"));
            pubResp.EnsureSuccessStatusCode();
            n++;
        }
        return n;
    }

    private HttpClient MgmtHttp()
    {
        var http = new HttpClient { BaseAddress = new Uri(rmq.ManagementUri) };
        var basic = Convert.ToBase64String(Encoding.UTF8.GetBytes($"{rmq.ManagementUser}:{rmq.ManagementPassword}"));
        http.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic", basic);
        return http;
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

    private static async Task WaitForAsync(Func<Task<bool>> condition, TimeSpan timeout)
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

public sealed class FailOnceState
{
    private int _failures;
    private int _successes;
    public int Failures => _failures;
    public int Successes => _successes;
    public void RecordFailure() => Interlocked.Increment(ref _failures);
    public void RecordSuccess() => Interlocked.Increment(ref _successes);
}

public sealed class FailOnceConsumer(FailOnceState state) : PubSub.ISubscribeTo<FailOnceMessage>
{
    public Task Handle(FailOnceMessage message, CancellationToken cancellationToken)
    {
        if (state.Failures == 0)
        {
            state.RecordFailure();
            throw new InvalidOperationException($"Simulated first-attempt failure for {message.Symbol}");
        }
        state.RecordSuccess();
        return Task.CompletedTask;
    }
}
