using System.Text.Json.Serialization;

namespace PubSub.RabbitMQ.Admin;

/// <summary>
/// Deserialization DTOs for the RabbitMQ Management HTTP API. Each type covers only the fields the
/// broker-scraping admin consumes; unknown fields (the API returns 50+ per queue) are ignored.
/// JSON is snake_case, mapped explicitly via <see cref="JsonPropertyNameAttribute"/>.
/// </summary>
internal sealed record ManagementSample
{
    /// <summary>Absolute counter value at <see cref="Timestamp"/> (used for sparkline series).</summary>
    [JsonPropertyName("sample")] public double Sample { get; init; }

    /// <summary>Unix time in milliseconds for this sample.</summary>
    [JsonPropertyName("timestamp")] public long Timestamp { get; init; }
}

/// <summary>A rate-bearing counter facet (<c>*_details</c>): current per-second rate + history.</summary>
internal sealed record ManagementRateDetails
{
    [JsonPropertyName("rate")] public double Rate { get; init; }
    [JsonPropertyName("samples")] public IReadOnlyList<ManagementSample>? Samples { get; init; }
}

/// <summary>The <c>message_stats</c> object attached to queues and the cluster overview.</summary>
internal sealed record ManagementMessageStats
{
    [JsonPropertyName("publish_details")] public ManagementRateDetails? PublishDetails { get; init; }
    [JsonPropertyName("ack_details")] public ManagementRateDetails? AckDetails { get; init; }
    [JsonPropertyName("deliver_get_details")] public ManagementRateDetails? DeliverGetDetails { get; init; }
    [JsonPropertyName("redeliver_details")] public ManagementRateDetails? RedeliverDetails { get; init; }
}

/// <summary>One queue from <c>/api/queues/{vhost}</c>.</summary>
internal sealed record ManagementQueue
{
    [JsonPropertyName("name")] public string Name { get; init; } = string.Empty;
    [JsonPropertyName("vhost")] public string VHost { get; init; } = string.Empty;
    [JsonPropertyName("messages_ready")] public long MessagesReady { get; init; }
    [JsonPropertyName("messages_unacknowledged")] public long MessagesUnacknowledged { get; init; }
    [JsonPropertyName("consumers")] public int Consumers { get; init; }
    [JsonPropertyName("message_stats")] public ManagementMessageStats? MessageStats { get; init; }
}

/// <summary>One binding from <c>/api/bindings/{vhost}</c> (authoritative exchange↔queue routing).</summary>
internal sealed record ManagementBinding
{
    [JsonPropertyName("source")] public string Source { get; init; } = string.Empty;
    [JsonPropertyName("routing_key")] public string RoutingKey { get; init; } = string.Empty;
    [JsonPropertyName("destination")] public string Destination { get; init; } = string.Empty;
    [JsonPropertyName("destination_type")] public string DestinationType { get; init; } = string.Empty;
}

/// <summary>One exchange from <c>/api/exchanges/{vhost}</c>.</summary>
internal sealed record ManagementExchange
{
    [JsonPropertyName("name")] public string Name { get; init; } = string.Empty;
    [JsonPropertyName("type")] public string Type { get; init; } = string.Empty;
}

/// <summary>The <c>queue</c> reference on a consumer record.</summary>
internal sealed record ManagementConsumerQueue
{
    [JsonPropertyName("name")] public string Name { get; init; } = string.Empty;
}

/// <summary>One consumer from <c>/api/consumers/{vhost}</c> (source of per-queue prefetch).</summary>
internal sealed record ManagementConsumer
{
    [JsonPropertyName("queue")] public ManagementConsumerQueue? Queue { get; init; }
    [JsonPropertyName("prefetch_count")] public int PrefetchCount { get; init; }
}

/// <summary>The cluster-wide <c>/api/overview</c> (source of the headline KPI totals + series).</summary>
internal sealed record ManagementOverview
{
    [JsonPropertyName("message_stats")] public ManagementMessageStats? MessageStats { get; init; }
}
