using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Testcontainers.RabbitMq;
using Wolverine;
using Wolverine.RabbitMQ;
using Host = Microsoft.Extensions.Hosting.Host;

namespace PubSub.RabbitMQ.Bench;

internal static class WolverineRoutingDiagnostic
{
    public static async Task RunAsync()
    {
        await using var broker = new RabbitMqBuilder()
            .WithImage("rabbitmq:3.13-management")
            .WithUsername("admin")
            .WithPassword("admin")
            .WithPortBinding(15672, assignRandomHostPort: true)
            .Build();
        await broker.StartAsync();
        var amqp = broker.GetConnectionString();
        var mgmtUri = $"http://{broker.Hostname}:{broker.GetMappedPublicPort(15672)}";

        const string Queue = "wolverine-routing-probe";

        var b = Host.CreateApplicationBuilder();
        b.Logging.SetMinimumLevel(LogLevel.Warning);
        b.UseWolverine(opts =>
        {
            opts.UseRabbitMq(new Uri(amqp)).AutoProvision();
            opts.PublishMessage<DiagnosticPayload>()
                .ToRabbitQueue(Queue)
                .SendInline();
            opts.ListenToRabbitQueue(Queue);
            opts.Discovery.DisableConventionalDiscovery();
            opts.Discovery.IncludeType<DiagnosticHandler>();
        });
        var host = b.Build();
        await host.StartAsync();

        var bus = host.Services.GetRequiredService<IMessageBus>();
        const int N = 100;
        DiagnosticHandler.Reset(N);
        for (int i = 0; i < N; i++)
            await bus.PublishAsync(new DiagnosticPayload(i));
        var consumed = await DiagnosticHandler.WaitAllAsync(TimeSpan.FromSeconds(30));
        Console.WriteLine($"Handler invocations: {consumed}/{N}");

        await Task.Delay(500);
        using (var http = MgmtHttp(mgmtUri))
        {
            var resp = await http.GetAsync($"/api/queues/%2F/{Queue}");
            if (!resp.IsSuccessStatusCode)
            {
                Console.WriteLine($"Queue '{Queue}' not found via mgmt API (status {(int)resp.StatusCode}).");
                Console.WriteLine("=> Wolverine did NOT declare a RabbitMQ queue for this route — local in-process delivery.");
            }
            else
            {
                var json = await resp.Content.ReadAsStringAsync();
                using var doc = JsonDocument.Parse(json);
                var ready = doc.RootElement.GetProperty("messages_ready").GetInt32();
                var deliveredAck = doc.RootElement.TryGetProperty("message_stats", out var stats)
                    && stats.TryGetProperty("deliver", out var d) ? d.GetInt32() : 0;
                var published = stats.ValueKind == JsonValueKind.Object && stats.TryGetProperty("publish", out var p) ? p.GetInt32() : 0;
                Console.WriteLine($"Queue '{Queue}': ready={ready}, broker-side publish.count={published}, deliver.count={deliveredAck}");
                if (published == 0 && consumed == N)
                    Console.WriteLine("=> CONFIRMED: Wolverine bypassed the broker entirely (in-process shortcut).");
                else if (published == N)
                    Console.WriteLine("=> Messages transited the broker normally — Wolverine is just fast.");
                else
                    Console.WriteLine("=> Partial broker transit — mixed path.");
            }

            var qList = await http.GetAsync("/api/queues");
            qList.EnsureSuccessStatusCode();
            using var qDoc = JsonDocument.Parse(await qList.Content.ReadAsStringAsync());
            Console.WriteLine("All queues seen by the broker:");
            foreach (var q in qDoc.RootElement.EnumerateArray())
                Console.WriteLine($"  - {q.GetProperty("name").GetString()}: ready={q.GetProperty("messages_ready").GetInt32()}");
        }

        await host.StopAsync();
        host.Dispose();
    }

    private static HttpClient MgmtHttp(string baseUri)
    {
        var http = new HttpClient { BaseAddress = new Uri(baseUri) };
        var basic = Convert.ToBase64String(Encoding.UTF8.GetBytes("admin:admin"));
        http.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic", basic);
        return http;
    }
}

public sealed record DiagnosticPayload(int Id);

public sealed class DiagnosticHandler
{
    private static int _expected;
    private static int _seen;
    private static TaskCompletionSource? _done;

    public static void Reset(int expected)
    {
        _expected = expected;
        _seen = 0;
        _done = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    public static async Task<int> WaitAllAsync(TimeSpan timeout)
    {
        try { await _done!.Task.WaitAsync(timeout); }
        catch (TimeoutException) { }
        return _seen;
    }

    public void Handle(DiagnosticPayload msg)
    {
        var s = Interlocked.Increment(ref _seen);
        if (s >= _expected) _done?.TrySetResult();
    }
}
