using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Engines;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Reports;
using MassTransit;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NServiceBus;
using NServiceBus.Transport.RabbitMQ;
using PubSub.RabbitMQ;
using SlimMessageBus;
using SlimMessageBus.Host;
using SlimMessageBus.Host.RabbitMQ;
using SlimMessageBus.Host.Serialization.SystemTextJson;
using Testcontainers.RabbitMq;
using Wolverine;
using Wolverine.RabbitMQ;
using IHost = Microsoft.Extensions.Hosting.IHost;
using Host = Microsoft.Extensions.Hosting.Host;
using WolverineMessageBus = Wolverine.IMessageBus;
using SmbExchangeType = SlimMessageBus.Host.RabbitMQ.ExchangeType;

namespace PubSub.RabbitMQ.Bench;

[Config(typeof(BenchConfig))]
[MemoryDiagnoser]
[CategoriesColumn]
public class HeadToHeadConsumeBench
{
    public enum Bus { PubSub, MassTransit, Wolverine, NServiceBus, SlimMessageBus }

    private sealed class BenchConfig : ManualConfig
    {
        public BenchConfig()
        {
            AddJob(Job.Default
                .WithStrategy(RunStrategy.Throughput)
                .WithLaunchCount(1)
                .WithWarmupCount(2)
                .WithIterationCount(3)
                .WithInvocationCount(1)
                .WithUnrollFactor(1));
            SummaryStyle = SummaryStyle.Default
                .WithRatioStyle(BenchmarkDotNet.Columns.RatioStyle.Trend);
        }
    }

    [Params(Bus.PubSub, Bus.MassTransit, Bus.Wolverine, Bus.NServiceBus, Bus.SlimMessageBus)]
    public Bus Library { get; set; }

    private RabbitMqContainer? _broker;
    private IHost? _host;
    private IEndpointInstance? _nsbEndpoint;
    private Func<Task>? _publishOne;
    private ConsumeRoundtripPayload? _payload;
    private TaskCompletionSource? _tcs;

    [GlobalSetup]
    public async Task Setup()
    {
        _broker = new RabbitMqBuilder()
            .WithImage("rabbitmq:3.13-management")
            .WithUsername("admin")
            .WithPassword("admin")
            .WithPortBinding(15672, assignRandomHostPort: true)
            .Build();
        await _broker.StartAsync();
        var amqp = _broker.GetConnectionString();
        var mgmtUri = $"http://{_broker.Hostname}:{_broker.GetMappedPublicPort(15672)}";

        _payload = new ConsumeRoundtripPayload(
            Id: Guid.NewGuid(),
            Symbol: "ASX.BHP",
            TimestampUtc: DateTime.UtcNow,
            Price: 42.0,
            Volume: 1_000,
            Note: new string('x', 800));

        if (Library == Bus.NServiceBus)
        {
            _nsbEndpoint = await BuildNServiceBusAsync(amqp, mgmtUri);
            _publishOne = () => _nsbEndpoint.Publish(_payload!);
        }
        else
        {
            _host = Library switch
            {
                Bus.PubSub => await BuildPubSubAsync(amqp),
                Bus.MassTransit => await BuildMassTransitAsync(amqp),
                Bus.Wolverine => await BuildWolverineAsync(amqp),
                Bus.SlimMessageBus => await BuildSlimMessageBusAsync(amqp),
                _ => throw new NotSupportedException(),
            };
            _publishOne = Library switch
            {
                Bus.PubSub => () => _host.Services.GetRequiredService<IPublish<ConsumeRoundtripPayload>>().PublishAsync(_payload!),
                Bus.MassTransit => () => _host.Services.GetRequiredService<IPublishEndpoint>().Publish(_payload!),
                Bus.Wolverine => () => _host.Services.GetRequiredService<WolverineMessageBus>().PublishAsync(_payload!).AsTask(),
                Bus.SlimMessageBus => () => _host.Services.GetRequiredService<IMasterMessageBus>().ProducePublish(_payload!),
                _ => throw new NotSupportedException(),
            };
        }

        ConsumeRoundtripSignal.Reset();
        _tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        ConsumeRoundtripSignal.Current = _tcs;
        await _publishOne();
        await _tcs.Task.WaitAsync(TimeSpan.FromSeconds(30));
    }

    [IterationSetup]
    public void IterSetup()
    {
        _tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        ConsumeRoundtripSignal.Current = _tcs;
    }

    [Benchmark]
    public async Task Roundtrip()
    {
        await _publishOne!();
        await _tcs!.Task;
    }

    [GlobalCleanup]
    public async Task Cleanup()
    {
        if (_nsbEndpoint is not null) await _nsbEndpoint.Stop();
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
        }, x =>
        {
            x.Publish<ConsumeRoundtripPayload>();
            x.Subscribe<ConsumeRoundtripPayload, PubSubRoundtripConsumer>();
        });
        var host = b.Build();
        await host.StartAsync();
        return host;
    }

    private static async Task<IHost> BuildMassTransitAsync(string amqp)
    {
        var b = Host.CreateApplicationBuilder();
        b.Logging.SetMinimumLevel(LogLevel.Warning);
        b.Services.AddMassTransit(mt =>
        {
            mt.AddConsumer<MassTransitRoundtripConsumer>();
            mt.UsingRabbitMq((ctx, cfg) =>
            {
                cfg.Host(new Uri(amqp));
                cfg.ReceiveEndpoint("mt-bench-roundtrip", ep =>
                {
                    ep.ConfigureConsumer<MassTransitRoundtripConsumer>(ctx);
                });
            });
        });
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
            opts.UseRabbitMq(new Uri(amqp)).AutoProvision();
            opts.PublishMessage<ConsumeRoundtripPayload>()
                .ToRabbitQueue("wolverine-consumebench")
                .SendInline();
            opts.ListenToRabbitQueue("wolverine-consumebench");
            opts.Discovery.DisableConventionalDiscovery();
            opts.Discovery.IncludeType<WolverineRoundtripHandler>();
        });
        var host = b.Build();
        await host.StartAsync();
        return host;
    }

    private static async Task<IHost> BuildSlimMessageBusAsync(string amqp)
    {
        var b = Host.CreateApplicationBuilder();
        b.Logging.SetMinimumLevel(LogLevel.Warning);
        b.Services.AddSlimMessageBus(mbb =>
        {
            mbb.WithProviderRabbitMQ(rmq =>
            {
                rmq.ConnectionFactory = new global::RabbitMQ.Client.ConnectionFactory { Uri = new Uri(amqp) };
            });
            mbb.AddJsonSerializer();
            mbb.Produce<ConsumeRoundtripPayload>(x => x
                .Exchange("smb.consumebench", SmbExchangeType.Topic, durable: true)
                .RoutingKeyProvider((m, p) => "bench.consume"));
            mbb.Consume<ConsumeRoundtripPayload>(x => x
                .Queue("smb-consumebench")
                .ExchangeBinding("smb.consumebench", "bench.consume")
                .WithConsumer<SmbRoundtripConsumer>());
        });
        var host = b.Build();
        await host.StartAsync();
        return host;
    }

    private static async Task<IEndpointInstance> BuildNServiceBusAsync(string amqp, string mgmtUri)
    {
        var endpointConfig = new EndpointConfiguration("pubsub-bench-nsb-consume");
        var transport = new RabbitMQTransport(
            RoutingTopology.Conventional(QueueType.Quorum),
            amqp)
        {
            ManagementApiConfiguration = new ManagementApiConfiguration(mgmtUri, "admin", "admin"),
        };
        endpointConfig.UseTransport(transport);
        endpointConfig.UseSerialization<SystemJsonSerializer>();
        endpointConfig.EnableInstallers();
        endpointConfig.SendFailedMessagesTo("pubsub-bench-nsb-consume.error");
        endpointConfig.Conventions().DefiningEventsAs(t => t == typeof(ConsumeRoundtripPayload));
        return await Endpoint.Start(endpointConfig);
    }
}

[PubSub.PubSubTopic("bench.consume", Exchange = "pubsub.consumebench")]
public sealed record ConsumeRoundtripPayload(
    Guid Id,
    string Symbol,
    DateTime TimestampUtc,
    double Price,
    int Volume,
    string Note);

public static class ConsumeRoundtripSignal
{
    public static volatile TaskCompletionSource? Current;
    public static void Reset() => Current = null;
}

public sealed class PubSubRoundtripConsumer : PubSub.ISubscribeTo<ConsumeRoundtripPayload>
{
    public Task Handle(ConsumeRoundtripPayload message, CancellationToken cancellationToken)
    {
        ConsumeRoundtripSignal.Current?.TrySetResult();
        return Task.CompletedTask;
    }
}

public sealed class MassTransitRoundtripConsumer : MassTransit.IConsumer<ConsumeRoundtripPayload>
{
    public Task Consume(ConsumeContext<ConsumeRoundtripPayload> context)
    {
        ConsumeRoundtripSignal.Current?.TrySetResult();
        return Task.CompletedTask;
    }
}

public sealed class WolverineRoundtripHandler
{
    public void Handle(ConsumeRoundtripPayload message)
    {
        ConsumeRoundtripSignal.Current?.TrySetResult();
    }
}

public sealed class NServiceBusRoundtripHandler : NServiceBus.IHandleMessages<ConsumeRoundtripPayload>
{
    public Task Handle(ConsumeRoundtripPayload message, NServiceBus.IMessageHandlerContext context)
    {
        ConsumeRoundtripSignal.Current?.TrySetResult();
        return Task.CompletedTask;
    }
}

public sealed class SmbRoundtripConsumer : SlimMessageBus.IConsumer<ConsumeRoundtripPayload>
{
    public Task OnHandle(ConsumeRoundtripPayload message, CancellationToken cancellationToken)
    {
        ConsumeRoundtripSignal.Current?.TrySetResult();
        return Task.CompletedTask;
    }
}
