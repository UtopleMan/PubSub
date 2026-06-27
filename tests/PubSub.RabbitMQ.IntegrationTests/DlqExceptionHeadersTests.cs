using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Shouldly;
using Xunit;

namespace PubSub.RabbitMQ.IntegrationTests;

[Collection(RabbitMqCollection.Name)]
public class DlqExceptionHeadersTests(RabbitMqFixture rmq)
{
    [Fact]
    public async Task Handler_throws_DLQ_message_carries_exception_headers()
    {
        using var host = HostBuilder.Build(rmq.ConnectionString, b =>
        {
            b.Publish<FailingMessage>();
            b.Subscribe<FailingMessage, FailingConsumer>();
        });
        await host.StartAsync();

        var publisher = host.Services.GetRequiredService<PubSub.IPublish<FailingMessage>>();
        var recorder = host.Services.GetRequiredService<MessageRecorder>();

        await publisher.PublishAsync(new FailingMessage("KMF"));
        await WaitFor(() => recorder.Fail.Count >= 1, TimeSpan.FromSeconds(10));

        await WaitFor(async () => (await GetDlqDepthAsync()) >= 1, TimeSpan.FromSeconds(10));
        var msgs = await PeekDlqAsync();
        msgs.Count.ShouldBeGreaterThanOrEqualTo(1);
        var msg = msgs[0];
        var headers = msg.Headers;

        DecodeHeader(headers, "x-exception-type").ShouldContain("InvalidOperationException");
        DecodeHeader(headers, "x-exception").ShouldContain("KMF lookup failed for KMF");
        DecodeHeader(headers, "x-stacktrace").ShouldContain("FailingConsumer");
        DecodeHeader(headers, "x-consumer-handler").ShouldContain("FailingConsumer");
        DecodeHeader(headers, "x-original-routing-key").ShouldBe("test.fail");
        DecodeHeader(headers, "x-pod-name").ShouldBe("pubsub-itest");
        msg.RoutingKey.ShouldBe("test.fail");

        await host.StopAsync();
    }

    private static string DecodeHeader(IDictionary<string, JsonElement> headers, string name)
    {
        headers.ShouldContainKey(name);
        var v = headers[name];
        return v.ValueKind switch
        {
            JsonValueKind.String => v.GetString() ?? string.Empty,
            JsonValueKind.Number => v.GetInt64().ToString(),
            JsonValueKind.Array => Encoding.UTF8.GetString(v.EnumerateArray().Select(e => (byte)e.GetInt32()).ToArray()),
            _ => v.ToString(),
        };
    }

    private async Task<int> GetDlqDepthAsync()
    {
        using var http = MgmtHttp();
        var resp = await http.GetAsync("/api/queues/%2F/pubsub.itests.test.fail.error");
        if (!resp.IsSuccessStatusCode) return 0;
        var json = await resp.Content.ReadAsStringAsync();
        using var doc = JsonDocument.Parse(json);
        return doc.RootElement.TryGetProperty("messages_ready", out var v) ? v.GetInt32() : 0;
    }

    private async Task<List<DlqEntry>> PeekDlqAsync()
    {
        using var http = MgmtHttp();
        var body = """{"count":5,"ackmode":"ack_requeue_true","encoding":"auto","truncate":50000}""";
        var resp = await http.PostAsync("/api/queues/%2F/pubsub.itests.test.fail.error/get",
            new StringContent(body, Encoding.UTF8, "application/json"));
        resp.EnsureSuccessStatusCode();
        var json = await resp.Content.ReadAsStringAsync();
        using var doc = JsonDocument.Parse(json);
        var list = new List<DlqEntry>();
        foreach (var item in doc.RootElement.EnumerateArray())
        {
            var headers = new Dictionary<string, JsonElement>();
            if (item.TryGetProperty("properties", out var props)
                && props.TryGetProperty("headers", out var hdrs)
                && hdrs.ValueKind == JsonValueKind.Object)
            {
                foreach (var h in hdrs.EnumerateObject())
                    headers[h.Name] = h.Value.Clone();
            }
            list.Add(new DlqEntry(
                item.TryGetProperty("routing_key", out var rk) ? rk.GetString() ?? string.Empty : string.Empty,
                headers));
        }
        return list;
    }

    private HttpClient MgmtHttp()
    {
        var http = new HttpClient { BaseAddress = new Uri(rmq.ManagementUri) };
        var basic = Convert.ToBase64String(Encoding.UTF8.GetBytes($"{rmq.ManagementUser}:{rmq.ManagementPassword}"));
        http.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic", basic);
        return http;
    }

    private sealed record DlqEntry(string RoutingKey, Dictionary<string, JsonElement> Headers);

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
