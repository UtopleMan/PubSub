using System.Net;
using Microsoft.Extensions.Logging.Abstractions;
using PubSub.RabbitMQ.Admin;
using Shouldly;
using Xunit;

namespace PubSub.RabbitMQ.Tests;

public class RabbitMqManagementClientTests
{
    private static RabbitMqManagementClient Client(StubHttpMessageHandler handler, int seriesLength = 26)
    {
        var options = new PubSubAdminOptions
        {
            ManagementBaseUrl = new Uri("http://rabbit:15672"),
            ManagementUser = "admin",
            ManagementPassword = "secret",
            VHost = "/",
            ConnectionString = "amqp://admin:secret@rabbit:5672/",
            SeriesLength = seriesLength,
        };
        var http = new HttpClient(handler);
        return new RabbitMqManagementClient(http, options, NullLogger<RabbitMqManagementClient>.Instance);
    }

    [Fact]
    public async Task GetQueues_parses_depth_consumers_rates_and_samples()
    {
        const string json = """
        [
          {
            "name": "orders",
            "vhost": "/",
            "messages_ready": 42,
            "messages_unacknowledged": 3,
            "consumers": 2,
            "message_stats": {
              "publish_details": { "rate": 10.5, "samples": [ { "sample": 100, "timestamp": 1000 }, { "sample": 150, "timestamp": 2000 } ] },
              "ack_details":     { "rate": 9.0,  "samples": [ { "sample": 90,  "timestamp": 1000 }, { "sample": 140, "timestamp": 2000 } ] },
              "deliver_get_details": { "rate": 9.5 },
              "redeliver_details":   { "rate": 0.2 }
            }
          },
          { "name": "orders.error", "vhost": "/", "messages_ready": 5, "messages_unacknowledged": 0, "consumers": 0 }
        ]
        """;
        var client = Client(StubHttpMessageHandler.Json(json));

        var result = await client.GetQueuesAsync("/");

        result.Ok.ShouldBeTrue();
        var queues = result.Value!;
        queues.Count.ShouldBe(2);

        var orders = queues[0];
        orders.Name.ShouldBe("orders");
        orders.MessagesReady.ShouldBe(42);
        orders.MessagesUnacknowledged.ShouldBe(3);
        orders.Consumers.ShouldBe(2);
        orders.MessageStats!.PublishDetails!.Rate.ShouldBe(10.5);
        orders.MessageStats.AckDetails!.Rate.ShouldBe(9.0);
        orders.MessageStats.DeliverGetDetails!.Rate.ShouldBe(9.5);
        orders.MessageStats.RedeliverDetails!.Rate.ShouldBe(0.2);

        var samples = orders.MessageStats.AckDetails.Samples!;
        samples.Count.ShouldBe(2);
        samples[0].Sample.ShouldBe(90);
        samples[0].Timestamp.ShouldBe(1000);
        samples[1].Sample.ShouldBe(140);

        // The .error queue is parsed too (pairing/exclusion is the admin's job, not the client's).
        queues[1].Name.ShouldBe("orders.error");
        queues[1].MessagesReady.ShouldBe(5);
        queues[1].MessageStats.ShouldBeNull();
    }

    [Fact]
    public async Task GetQueues_requests_vhost_as_percent2F_with_rate_and_length_params()
    {
        var handler = StubHttpMessageHandler.Json("[]");
        var client = Client(handler, seriesLength: 26);

        await client.GetQueuesAsync("/");

        var uri = handler.LastRequest!.RequestUri!;
        // Default vhost "/" must be sent as %2F, never decoded to a path separator.
        uri.PathAndQuery.ShouldContain("/api/queues/%2F");
        uri.PathAndQuery.ShouldNotContain("/api/queues//");
        // Sample-history windowing: incr = 5s, age = SeriesLength * incr = 130s.
        uri.Query.ShouldContain("msg_rates_age=130");
        uri.Query.ShouldContain("msg_rates_incr=5");
        uri.Query.ShouldContain("lengths_age=130");
        uri.Query.ShouldContain("lengths_incr=5");
    }

    [Fact]
    public async Task Client_sends_basic_auth_header_from_options()
    {
        var handler = StubHttpMessageHandler.Json("[]");
        var client = Client(handler);

        await client.GetExchangesAsync("/");

        var auth = handler.LastRequest!.Headers.Authorization!;
        auth.Scheme.ShouldBe("Basic");
        var decoded = System.Text.Encoding.UTF8.GetString(Convert.FromBase64String(auth.Parameter!));
        decoded.ShouldBe("admin:secret");
    }

    [Fact]
    public async Task GetBindings_parses_source_routing_key_and_destination()
    {
        const string json = """
        [
          { "source": "pubsub.orders", "vhost": "/", "destination": "orders", "destination_type": "queue", "routing_key": "order.placed" },
          { "source": "", "vhost": "/", "destination": "orders", "destination_type": "queue", "routing_key": "orders" }
        ]
        """;
        var client = Client(StubHttpMessageHandler.Json(json));

        var result = await client.GetBindingsAsync("/");

        result.Ok.ShouldBeTrue();
        var bindings = result.Value!;
        bindings.Count.ShouldBe(2);
        bindings[0].Source.ShouldBe("pubsub.orders");
        bindings[0].RoutingKey.ShouldBe("order.placed");
        bindings[0].Destination.ShouldBe("orders");
        bindings[0].DestinationType.ShouldBe("queue");
        bindings[1].Source.ShouldBe("");
    }

    [Fact]
    public async Task GetExchanges_parses_name_and_type()
    {
        const string json = """
        [
          { "name": "pubsub.orders", "vhost": "/", "type": "topic", "durable": true },
          { "name": "", "vhost": "/", "type": "direct" }
        ]
        """;
        var client = Client(StubHttpMessageHandler.Json(json));

        var result = await client.GetExchangesAsync("/");

        result.Ok.ShouldBeTrue();
        result.Value!.Count.ShouldBe(2);
        result.Value[0].Name.ShouldBe("pubsub.orders");
        result.Value[0].Type.ShouldBe("topic");
    }

    [Fact]
    public async Task GetConsumers_parses_queue_name_and_prefetch()
    {
        const string json = """
        [
          { "prefetch_count": 50, "ack_required": true, "queue": { "name": "orders", "vhost": "/" } }
        ]
        """;
        var client = Client(StubHttpMessageHandler.Json(json));

        var result = await client.GetConsumersAsync("/");

        result.Ok.ShouldBeTrue();
        var consumer = result.Value!.ShouldHaveSingleItem();
        consumer.PrefetchCount.ShouldBe(50);
        consumer.Queue!.Name.ShouldBe("orders");
    }

    [Fact]
    public async Task GetOverview_parses_cluster_message_stats_rate_and_samples()
    {
        const string json = """
        {
          "message_stats": {
            "publish_details": { "rate": 12.0, "samples": [ { "sample": 10, "timestamp": 1 } ] },
            "deliver_get_details": { "rate": 11.0 },
            "ack_details": { "rate": 10.0 }
          },
          "queue_totals": { "messages": 100 }
        }
        """;
        var client = Client(StubHttpMessageHandler.Json(json));

        var result = await client.GetOverviewAsync();

        result.Ok.ShouldBeTrue();
        result.Value!.MessageStats!.PublishDetails!.Rate.ShouldBe(12.0);
        result.Value.MessageStats.PublishDetails.Samples!.ShouldHaveSingleItem().Sample.ShouldBe(10);
        result.Value.MessageStats.AckDetails!.Rate.ShouldBe(10.0);
    }

    [Fact]
    public async Task Non_success_status_degrades_to_failure()
    {
        var client = Client(StubHttpMessageHandler.Status(HttpStatusCode.Unauthorized));

        var result = await client.GetQueuesAsync("/");

        result.Ok.ShouldBeFalse();
        result.Value.ShouldBeNull();
    }

    [Fact]
    public async Task Transport_exception_degrades_to_failure_and_does_not_throw()
    {
        var client = Client(StubHttpMessageHandler.Throws());

        var overview = await client.GetOverviewAsync();
        var queues = await client.GetQueuesAsync("/");

        overview.Ok.ShouldBeFalse();
        overview.Value.ShouldBeNull();
        queues.Ok.ShouldBeFalse();
        queues.Value.ShouldBeNull();
    }

    [Fact]
    public async Task Empty_list_body_is_reachable_but_empty()
    {
        var client = Client(StubHttpMessageHandler.Json("[]"));

        var result = await client.GetQueuesAsync("/");

        result.Ok.ShouldBeTrue();
        result.Value!.ShouldBeEmpty();
    }
}
