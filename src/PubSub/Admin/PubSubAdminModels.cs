namespace PubSub.Admin;

/// <summary>
/// Vendor-neutral view models returned by <see cref="IPubSubAdmin"/>. These types carry no
/// RabbitMQ (or any other broker) specifics so a future messaging backend can implement the
/// same admin surface and drive the same console.
/// </summary>

/// <summary>Identity + connectivity of the messaging installation the console is attached to.</summary>
public sealed record MessagingInstallation(
    string Provider,
    string Endpoint,
    string? VHost,
    string Environment,
    bool Connected);

/// <summary>Derived health of a single queue/consumer, mirroring the console's dot colours.</summary>
public enum QueueHealth
{
    Healthy,
    Warning,
    Critical,
}

/// <summary>
/// Live + registered statistics for one consumer endpoint (an <c>ISubscribeTo&lt;T&gt;</c> bound to a
/// queue). One row of the console's "Endpoints &amp; queues" table.
/// </summary>
public sealed record QueueStat(
    string Queue,
    string Exchange,
    string RoutingKey,
    string Endpoint,
    string Consumer,
    int Prefetch,
    double PublishRate,
    double ConsumeRate,
    long Depth,
    int Consumers,
    double DlqRate,
    double ErrorPercent,
    long HandlerP95Ms,
    long InFlightAgeMs,
    IReadOnlyList<double> ConsumeSeries,
    QueueHealth Status);

/// <summary>A single headline metric card (value + delta + sparkline series).</summary>
public sealed record PubSubKpi(
    string Label,
    string Metric,
    double Value,
    string Unit,
    double DeltaPercent,
    bool DeltaIsBad,
    IReadOnlyList<double> Series);

/// <summary>The four headline metric cards shown at the top of the Monitoring tab.</summary>
public sealed record PubSubKpis(IReadOnlyList<PubSubKpi> Kpis);

/// <summary>An exchange and how many routing keys/topics are bound under it.</summary>
public sealed record ExchangeInfo(
    string Name,
    int TopicCount,
    double ConsumeRate);

/// <summary>A routing key on an exchange and its aggregate consumer/failure counts.</summary>
public sealed record TopicBinding(
    string Exchange,
    string RoutingKey,
    int ConsumerCount,
    double ConsumeRate,
    int FailedCount);

/// <summary>Registered topology: the exchanges and routing keys this process participates in.</summary>
public sealed record TopologySnapshot(
    IReadOnlyList<ExchangeInfo> Exchanges,
    IReadOnlyList<TopicBinding> Topics);

/// <summary>One <c>x-*</c> header carried by a dead-lettered message.</summary>
public sealed record FailedMessageHeader(string Key, string Value);

/// <summary>
/// A message currently sitting in a consumer's <c>{queue}.error</c> queue, with the enriched
/// exception headers the library attaches on dead-letter. One row of the "Failed messages" tab.
/// </summary>
public sealed record FailedMessage(
    string Id,
    string ErrorQueue,
    string Exchange,
    string RoutingKey,
    string Consumer,
    string Endpoint,
    string ExceptionType,
    string ExceptionMessage,
    long HandlerElapsedMs,
    string Pod,
    DateTimeOffset LastSeen,
    int Count,
    IReadOnlyList<FailedMessageHeader> Headers,
    string StackTrace,
    string ShovelCommand);

/// <summary>Filter/paging criteria for <see cref="IPubSubAdmin.GetFailedMessagesAsync"/>.</summary>
public sealed record FailedQuery(
    string? ExceptionType = null,
    string? Search = null,
    int Limit = 200);

/// <summary>
/// Request to replay dead-lettered messages back to their origin exchange/routing key.
/// An empty <see cref="MessageIds"/> means "replay every message currently in the error queue".
/// </summary>
public sealed record ReplayRequest(
    string ErrorQueue,
    IReadOnlyList<string> MessageIds);

/// <summary>Outcome of a replay.</summary>
public sealed record ReplayResult(int Replayed, int NotFound);

/// <summary>
/// Request to permanently drop dead-lettered messages from an error queue. An empty
/// <see cref="MessageIds"/> means "delete every message currently in the error queue".
/// </summary>
public sealed record DeleteRequest(
    string ErrorQueue,
    IReadOnlyList<string> MessageIds);

/// <summary>Outcome of a delete.</summary>
public sealed record DeleteResult(int Deleted, int NotFound);
