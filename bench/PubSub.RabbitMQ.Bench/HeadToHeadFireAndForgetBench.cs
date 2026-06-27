using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Engines;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Reports;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using PubSub.RabbitMQ;
using Testcontainers.RabbitMq;
using Wolverine;
using Wolverine.RabbitMQ;
using IHost = Microsoft.Extensions.Hosting.IHost;
using Host = Microsoft.Extensions.Hosting.Host;

namespace PubSub.RabbitMQ.Bench;

[Config(typeof(BenchConfig))]
[MemoryDiagnoser]
[CategoriesColumn]
public class HeadToHeadFireAndForgetBench
{
    public enum Bus { PubSub, Wolverine }

    private sealed class BenchConfig : ManualConfig
    {
        public BenchConfig()
        {
            AddJob(Job.Default
                .WithStrategy(RunStrategy.Throughput)
                .WithLaunchCount(1)
                .WithWarmupCount(2)
                .WithIterationCount(3)
                .WithInvocationCount(2048)
                .WithUnrollFactor(16));
            SummaryStyle = SummaryStyle.Default
                .WithRatioStyle(BenchmarkDotNet.Columns.RatioStyle.Trend);
        }
    }

    [Params(Bus.PubSub, Bus.Wolverine)]
    public Bus Library { get; set; }

    private RabbitMqContainer? _broker;
    private IHost? _host;
    private Func<Task>? _publishOne;
    private FnfBenchPayload? _payload;

    [GlobalSetup]
    public async Task Setup()
    {
        _broker = new RabbitMqBuilder().WithImage("rabbitmq:3.13-management").Build();
        await _broker.StartAsync();
        var amqp = _broker.GetConnectionString();

        _payload = new FnfBenchPayload(
            Id: Guid.NewGuid(),
            Symbol: "ASX.BHP",
            TimestampUtc: DateTime.UtcNow,
            Price: 42.0,
            Volume: 1_000,
            Note: new string('x', 800));

        _host = Library switch
        {
            Bus.PubSub => await BuildPubSubAsync(amqp),
            Bus.Wolverine => await BuildWolverineAsync(amqp),
            _ => throw new NotSupportedException(),
        };

        _publishOne = Library switch
        {
            Bus.PubSub => () => _host.Services.GetRequiredService<IFireAndForgetPublish<FnfBenchPayload>>().PublishAsync(_payload!),
            Bus.Wolverine => () => _host.Services.GetRequiredService<IMessageBus>().PublishAsync(_payload!).AsTask(),
            _ => throw new NotSupportedException(),
        };

        await _publishOne();
    }

    [Benchmark]
    public Task Publish() => _publishOne!();

    [GlobalCleanup]
    public async Task Cleanup()
    {
        if (_host is not null) { await _host.StopAsync(); _host.Dispose(); }
        if (_broker is not null) await _broker.DisposeAsync();
    }

    private static async Task<IHost> BuildPubSubAsync(string amqp)
    {
        var b = Host.CreateApplicationBuilder();
        b.Logging.SetMinimumLevel(LogLevel.Warning);
        b.Services.AddPubSubRabbitMq(new PubSubRabbitMqOptions
        {
            ConnectionString = amqp,
        }, x => x.Publish<FnfBenchPayload>());
        var host = b.Build();
        await host.StartAsync();
        return host;
    }

    private static async Task<IHost> BuildWolverineAsync(string amqp)
    {
        var b = Host.CreateApplicationBuilder();
        b.Logging.SetMinimumLevel(LogLevel.Warning);
        b.UseWolverine(opts =>
        {
            opts.UseRabbitMq(new Uri(amqp))
                .AutoProvision()
                .DeclareExchange("wolverine.fnfbench", ex => ex.ExchangeType = ExchangeType.Topic);
            opts.PublishMessage<FnfBenchPayload>()
                .ToRabbitExchange("wolverine.fnfbench")
                .SendInline();
        });
        var host = b.Build();
        await host.StartAsync();
        return host;
    }
}

[PubSub.PubSubTopic("bench.fnf", Exchange = "pubsub.fnfbench")]
[PubSub.FireAndForget]
public sealed record FnfBenchPayload(
    Guid Id,
    string Symbol,
    DateTime TimestampUtc,
    double Price,
    int Volume,
    string Note);
