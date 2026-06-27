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
using WolverineExchangeType = Wolverine.RabbitMQ.ExchangeType;
using SmbMessageBus = SlimMessageBus.IMessageBus;
using SmbExchangeType = SlimMessageBus.Host.RabbitMQ.ExchangeType;

namespace PubSub.RabbitMQ.Bench;

[Config(typeof(BenchConfig))]
[MemoryDiagnoser]
[CategoriesColumn]
public class HeadToHeadPublishBench
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
                .WithInvocationCount(1024)
                .WithUnrollFactor(16));
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
    private BenchPayload? _payload;

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

        _payload = new BenchPayload(
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
                Bus.PubSub => () => _host.Services.GetRequiredService<IPublish<BenchPayload>>().PublishAsync(_payload!),
                Bus.MassTransit => () => _host.Services.GetRequiredService<IPublishEndpoint>().Publish(_payload!),
                Bus.Wolverine => () => _host.Services.GetRequiredService<WolverineMessageBus>().PublishAsync(_payload!).AsTask(),
                Bus.SlimMessageBus => () => _host.Services.GetRequiredService<IMasterMessageBus>().ProducePublish(_payload!),
                _ => throw new NotSupportedException(),
            };
        }

        await _publishOne();
    }

    [Benchmark]
    public Task Publish() => _publishOne!();

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
            DefaultPublishTimeout = TimeSpan.FromSeconds(10),
        }, x => x.Publish<BenchPayload>());
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
            mt.UsingRabbitMq((ctx, cfg) =>
            {
                cfg.Host(new Uri(amqp));
                cfg.Publish<BenchPayload>(p => p.ExchangeType = "topic");
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
            opts.UseRabbitMq(new Uri(amqp))
                .AutoProvision()
                .DeclareExchange("wolverine.bench", ex => ex.ExchangeType = WolverineExchangeType.Topic);
            opts.PublishMessage<BenchPayload>()
                .ToRabbitExchange("wolverine.bench")
                .SendInline();
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
            mbb.Produce<BenchPayload>(x => x
                .Exchange("smb.bench", SmbExchangeType.Topic, durable: true)
                .RoutingKeyProvider((m, p) => "bench.publish"));
        });
        var host = b.Build();
        await host.StartAsync();
        return host;
    }

    private static async Task<IEndpointInstance> BuildNServiceBusAsync(string amqp, string mgmtUri)
    {
        var endpointConfig = new EndpointConfiguration("pubsub-bench-nsb");
        var transport = new RabbitMQTransport(
            RoutingTopology.Conventional(QueueType.Quorum),
            amqp)
        {
            ManagementApiConfiguration = new ManagementApiConfiguration(mgmtUri, "admin", "admin"),
        };
        endpointConfig.UseTransport(transport);
        endpointConfig.UseSerialization<SystemJsonSerializer>();
        endpointConfig.EnableInstallers();
        endpointConfig.SendFailedMessagesTo("pubsub-bench-nsb.error");
        endpointConfig.Conventions().DefiningEventsAs(t => t == typeof(BenchPayload));
        return await Endpoint.Start(endpointConfig);
    }
}
