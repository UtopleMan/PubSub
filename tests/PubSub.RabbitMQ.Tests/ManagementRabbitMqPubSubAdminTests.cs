using Microsoft.Extensions.Logging.Abstractions;
using PubSub.Admin;
using PubSub.RabbitMQ.Admin;
using Shouldly;
using Xunit;

namespace PubSub.RabbitMQ.Tests;

public class ManagementRabbitMqPubSubAdminTests
{
    private static ManagementRabbitMqPubSubAdmin Build(FakeManagementClient fake)
    {
        var options = new PubSubAdminOptions
        {
            ManagementBaseUrl = new Uri("http://rabbit:15672"),
            ManagementUser = "admin",
            ManagementPassword = "secret",
            VHost = "/",
            ConnectionString = "amqp://admin:secret@broker:5672/",
            ServiceName = "svc",
        };
        var errorOps = new ErrorQueueOperations(options, NullLogger<ErrorQueueOperations>.Instance);
        return new ManagementRabbitMqPubSubAdmin(fake, options, errorOps, NullLogger<ManagementRabbitMqPubSubAdmin>.Instance);
    }

    private static ManagementQueue Queue(
        string name, long ready = 0, int consumers = 0,
        double? publishRate = null, double? ackRate = null,
        IReadOnlyList<ManagementSample>? ackSamples = null)
    {
        ManagementMessageStats? stats = null;
        if (publishRate is not null || ackRate is not null || ackSamples is not null)
        {
            stats = new ManagementMessageStats
            {
                PublishDetails = publishRate is null ? null : new ManagementRateDetails { Rate = publishRate.Value },
                AckDetails = (ackRate is null && ackSamples is null)
                    ? null
                    : new ManagementRateDetails { Rate = ackRate ?? 0, Samples = ackSamples },
            };
        }
        return new ManagementQueue
        {
            Name = name,
            VHost = "/",
            MessagesReady = ready,
            Consumers = consumers,
            MessageStats = stats,
        };
    }

    private static ManagementBinding Binding(string source, string queue, string routingKey) =>
        new() { Source = source, Destination = queue, DestinationType = "queue", RoutingKey = routingKey };

    [Fact]
    public async Task GetQueueStats_maps_fields_pairs_dlq_and_excludes_error_rows()
    {
        var fake = new FakeManagementClient
        {
            Queues = ManagementResult<IReadOnlyList<ManagementQueue>>.Success(
            [
                Queue("orders", ready: 42, consumers: 2, publishRate: 10, ackRate: 100),
                Queue("orders.error", ready: 5, publishRate: 0.5),
            ]),
            Bindings = ManagementResult<IReadOnlyList<ManagementBinding>>.Success(
            [
                Binding("pubsub.orders", "orders", "order.placed"),
            ]),
            Consumers = ManagementResult<IReadOnlyList<ManagementConsumer>>.Success(
            [
                new ManagementConsumer { PrefetchCount = 50, Queue = new ManagementConsumerQueue { Name = "orders" } },
            ]),
        };

        var stats = await Build(fake).GetQueueStatsAsync();

        var row = stats.ShouldHaveSingleItem(); // orders.error excluded
        row.Queue.ShouldBe("orders");
        row.Depth.ShouldBe(42);
        row.Consumers.ShouldBe(2);
        row.PublishRate.ShouldBe(10);
        row.ConsumeRate.ShouldBe(100);
        row.DlqRate.ShouldBe(0.5);
        row.Exchange.ShouldBe("pubsub.orders");
        row.RoutingKey.ShouldBe("order.placed");
        row.Prefetch.ShouldBe(50);
        row.Endpoint.ShouldBe("svc");
        row.HandlerP95Ms.ShouldBe(0);
        row.InFlightAgeMs.ShouldBe(0);
        row.ErrorPercent.ShouldBe(0.5); // 0.5/100*100
        row.Status.ShouldBe(QueueHealth.Healthy);
    }

    [Fact]
    public async Task GetQueueStats_derives_consume_series_from_ack_samples()
    {
        var fake = new FakeManagementClient
        {
            Queues = ManagementResult<IReadOnlyList<ManagementQueue>>.Success(
            [
                Queue("orders", ackRate: 0, ackSamples:
                [
                    new ManagementSample { Timestamp = 1000, Sample = 100 },
                    new ManagementSample { Timestamp = 2000, Sample = 200 },
                    new ManagementSample { Timestamp = 3000, Sample = 250 },
                ]),
            ]),
        };

        var stats = await Build(fake).GetQueueStatsAsync();

        // (200-100)/1s = 100 ; (250-200)/1s = 50
        stats[0].ConsumeSeries.ShouldBe([100, 50]);
    }

    [Theory]
    [InlineData(10, 100, 6, QueueHealth.Critical)]  // errPct 6 > 5
    [InlineData(10, 100, 2, QueueHealth.Warning)]   // errPct 2 > 1
    [InlineData(6000, 100, 0, QueueHealth.Warning)] // depth > 5000
    [InlineData(10, 100, 0, QueueHealth.Healthy)]
    public async Task GetQueueStats_health_keys_off_depth_and_error_percent(
        long depth, double ackRate, double dlqRate, QueueHealth expected)
    {
        var fake = new FakeManagementClient
        {
            Queues = ManagementResult<IReadOnlyList<ManagementQueue>>.Success(
            [
                Queue("orders", ready: depth, ackRate: ackRate),
                Queue("orders.error", publishRate: dlqRate),
            ]),
        };

        var stats = await Build(fake).GetQueueStatsAsync();

        stats.ShouldHaveSingleItem().Status.ShouldBe(expected);
    }

    [Fact]
    public async Task GetQueueStats_returns_empty_when_management_unreachable()
    {
        var fake = new FakeManagementClient
        {
            Queues = ManagementResult<IReadOnlyList<ManagementQueue>>.Failure(),
        };

        (await Build(fake).GetQueueStatsAsync()).ShouldBeEmpty();
    }

    [Fact]
    public async Task GetTopology_groups_exchanges_and_topics_with_failed_counts()
    {
        var fake = new FakeManagementClient
        {
            Queues = ManagementResult<IReadOnlyList<ManagementQueue>>.Success(
            [
                Queue("orders", ackRate: 9),
                Queue("orders.error", ready: 3),
            ]),
            Bindings = ManagementResult<IReadOnlyList<ManagementBinding>>.Success(
            [
                Binding("pubsub.orders", "orders", "order.placed"),
            ]),
        };

        var topology = await Build(fake).GetTopologyAsync();

        var exchange = topology.Exchanges.ShouldHaveSingleItem();
        exchange.Name.ShouldBe("pubsub.orders");
        exchange.TopicCount.ShouldBe(1);
        exchange.ConsumeRate.ShouldBe(9);

        var topic = topology.Topics.ShouldHaveSingleItem();
        topic.Exchange.ShouldBe("pubsub.orders");
        topic.RoutingKey.ShouldBe("order.placed");
        topic.ConsumerCount.ShouldBe(1);
        topic.FailedCount.ShouldBe(3);
    }

    [Fact]
    public async Task GetKpis_maps_overview_rates_sums_dlq_and_zeroes_handler_p95()
    {
        var fake = new FakeManagementClient
        {
            Overview = ManagementResult<ManagementOverview>.Success(new ManagementOverview
            {
                MessageStats = new ManagementMessageStats
                {
                    PublishDetails = new ManagementRateDetails { Rate = 12 },
                    AckDetails = new ManagementRateDetails { Rate = 10 },
                },
            }),
            Queues = ManagementResult<IReadOnlyList<ManagementQueue>>.Success(
            [
                Queue("orders", ackRate: 10),
                Queue("orders.error", publishRate: 0.5),
                Queue("payments.error", publishRate: 0.5),
            ]),
        };

        var kpis = (await Build(fake).GetKpisAsync()).Kpis;

        kpis.Count.ShouldBe(4);
        kpis[0].Label.ShouldBe("Publish rate");
        kpis[0].Value.ShouldBe(12);
        kpis[1].Label.ShouldBe("Consume rate");
        kpis[1].Value.ShouldBe(10);
        kpis[2].Label.ShouldBe("DLQ publish rate");
        kpis[2].Value.ShouldBe(1.0); // 0.5 + 0.5
        kpis[3].Label.ShouldBe("Handler p95");
        kpis[3].Value.ShouldBe(0);
        kpis[3].Series.ShouldBeEmpty();
    }

    [Fact]
    public async Task GetInstallation_reports_connected_when_overview_succeeds()
    {
        var fake = new FakeManagementClient
        {
            Overview = ManagementResult<ManagementOverview>.Success(new ManagementOverview()),
        };

        var install = await Build(fake).GetInstallationAsync();

        install.Provider.ShouldBe("RabbitMQ");
        install.Connected.ShouldBeTrue();
        install.Endpoint.ShouldBe("amqp://broker:5672");
        install.VHost.ShouldBe("/");
    }

    [Fact]
    public async Task GetInstallation_reports_disconnected_when_overview_fails()
    {
        var fake = new FakeManagementClient
        {
            Overview = ManagementResult<ManagementOverview>.Failure(),
        };

        (await Build(fake).GetInstallationAsync()).Connected.ShouldBeFalse();
    }
}
