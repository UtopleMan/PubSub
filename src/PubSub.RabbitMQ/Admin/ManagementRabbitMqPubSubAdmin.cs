using Microsoft.Extensions.Logging;
using PubSub.Admin;

namespace PubSub.RabbitMQ.Admin;

/// <summary>
/// Broker-scraping <see cref="IPubSubAdmin"/>: reads live topology and statistics from the RabbitMQ
/// Management HTTP API and maps them to the vendor-neutral console models. Stateless per request —
/// no local ring buffers; sparkline history comes from the Management API's counter samples.
/// Failed-message operations are delegated to <see cref="ErrorQueueOperations"/> over AMQP.
/// <para>
/// The broker cannot report handler latency, so <see cref="QueueStat.HandlerP95Ms"/> /
/// <see cref="QueueStat.InFlightAgeMs"/> and the handler-p95 KPI are always <c>0</c>; queue health
/// keys off depth + error% only.
/// </para>
/// </summary>
internal sealed class ManagementRabbitMqPubSubAdmin(
    IRabbitMqManagementClient client,
    PubSubAdminOptions options,
    ErrorQueueOperations errorOps,
    ILogger<ManagementRabbitMqPubSubAdmin> logger) : IPubSubAdmin
{
    private const string ErrorSuffix = ".error";

    public async Task<MessagingInstallation> GetInstallationAsync(CancellationToken cancellationToken = default)
    {
        var overview = await client.GetOverviewAsync(cancellationToken).ConfigureAwait(false);
        var env = Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT")
                  ?? Environment.GetEnvironmentVariable("DOTNET_ENVIRONMENT")
                  ?? "Production";
        return new MessagingInstallation(
            Provider: "RabbitMQ",
            Endpoint: AmqpEndpoint(),
            VHost: options.VHost,
            Environment: env,
            Connected: overview.Ok);
    }

    public async Task<IReadOnlyList<QueueStat>> GetQueueStatsAsync(CancellationToken cancellationToken = default)
    {
        var queuesResult = await client.GetQueuesAsync(options.VHost, cancellationToken).ConfigureAwait(false);
        if (!queuesResult.Ok)
        {
            logger.LogWarning("Management API unreachable; returning no queue stats");
            return [];
        }

        var queues = queuesResult.Value!;
        var bindings = (await client.GetBindingsAsync(options.VHost, cancellationToken).ConfigureAwait(false)).Value ?? [];
        var consumers = (await client.GetConsumersAsync(options.VHost, cancellationToken).ConfigureAwait(false)).Value ?? [];
        var byName = queues.ToDictionary(q => q.Name, StringComparer.Ordinal);
        var prefetchByQueue = PrefetchByQueue(consumers);

        var stats = new List<QueueStat>();
        foreach (var q in queues)
        {
            if (IsErrorQueue(q.Name)) continue;

            var sibling = byName.GetValueOrDefault(q.Name + ErrorSuffix);
            var publishRate = q.MessageStats?.PublishDetails?.Rate ?? 0;
            var consumeRate = q.MessageStats?.AckDetails?.Rate ?? 0;
            var dlqRate = sibling?.MessageStats?.PublishDetails?.Rate ?? 0;
            var errPct = ErrorPercent(consumeRate, dlqRate);
            var (exchange, routingKey) = ResolveBinding(bindings, q.Name);

            stats.Add(new QueueStat(
                Queue: q.Name,
                Exchange: exchange,
                RoutingKey: routingKey,
                Endpoint: options.ServiceName,
                Consumer: q.Name,
                Prefetch: prefetchByQueue.GetValueOrDefault(q.Name),
                PublishRate: publishRate,
                ConsumeRate: consumeRate,
                Depth: q.MessagesReady,
                Consumers: q.Consumers,
                DlqRate: dlqRate,
                ErrorPercent: errPct,
                HandlerP95Ms: 0,
                InFlightAgeMs: 0,
                ConsumeSeries: RateSeries(q.MessageStats?.AckDetails),
                Status: Health(q.MessagesReady, errPct)));
        }
        return stats.OrderBy(s => s.Queue, StringComparer.Ordinal).ToArray();
    }

    public async Task<TopologySnapshot> GetTopologyAsync(CancellationToken cancellationToken = default)
    {
        var queuesResult = await client.GetQueuesAsync(options.VHost, cancellationToken).ConfigureAwait(false);
        if (!queuesResult.Ok)
            return new TopologySnapshot([], []);

        var queues = queuesResult.Value!;
        var bindings = (await client.GetBindingsAsync(options.VHost, cancellationToken).ConfigureAwait(false)).Value ?? [];
        var byName = queues.ToDictionary(q => q.Name, StringComparer.Ordinal);

        // One row per consumer queue that is bound to a (non-default) exchange.
        var rows = new List<(string Exchange, string RoutingKey, double ConsumeRate, long FailedCount)>();
        foreach (var q in queues)
        {
            if (IsErrorQueue(q.Name)) continue;
            var (exchange, routingKey) = ResolveBinding(bindings, q.Name);
            if (exchange.Length == 0) continue; // default exchange: not shown in topology
            var consumeRate = q.MessageStats?.AckDetails?.Rate ?? 0;
            var failed = byName.GetValueOrDefault(q.Name + ErrorSuffix)?.MessagesReady ?? 0;
            rows.Add((exchange, routingKey, consumeRate, failed));
        }

        var exchanges = rows
            .GroupBy(r => r.Exchange, StringComparer.Ordinal)
            .Select(g => new ExchangeInfo(
                Name: g.Key,
                TopicCount: g.Select(r => r.RoutingKey).Distinct(StringComparer.Ordinal).Count(),
                ConsumeRate: g.Sum(r => r.ConsumeRate)))
            .OrderBy(e => e.Name, StringComparer.Ordinal)
            .ToArray();

        var topics = rows
            .GroupBy(r => (r.Exchange, r.RoutingKey))
            .Select(g => new TopicBinding(
                Exchange: g.Key.Exchange,
                RoutingKey: g.Key.RoutingKey,
                ConsumerCount: g.Count(),
                ConsumeRate: g.Sum(r => r.ConsumeRate),
                FailedCount: (int)g.Sum(r => r.FailedCount)))
            .OrderBy(t => t.Exchange, StringComparer.Ordinal)
            .ThenBy(t => t.RoutingKey, StringComparer.Ordinal)
            .ToArray();

        return new TopologySnapshot(exchanges, topics);
    }

    public async Task<PubSubKpis> GetKpisAsync(CancellationToken cancellationToken = default)
    {
        var overview = await client.GetOverviewAsync(cancellationToken).ConfigureAwait(false);
        var ms = overview.Ok ? overview.Value!.MessageStats : null;
        var publish = ms?.PublishDetails?.Rate ?? 0;
        var consume = ms?.AckDetails?.Rate ?? 0;
        var publishSeries = RateSeries(ms?.PublishDetails);
        var consumeSeries = RateSeries(ms?.AckDetails);

        // The overview has no DLQ-specific facet; total DLQ ingress = sum of the .error queues'
        // publish rate.
        double dlq = 0;
        var queuesResult = await client.GetQueuesAsync(options.VHost, cancellationToken).ConfigureAwait(false);
        if (queuesResult.Ok)
            foreach (var q in queuesResult.Value!)
                if (IsErrorQueue(q.Name))
                    dlq += q.MessageStats?.PublishDetails?.Rate ?? 0;

        var kpis = new List<PubSubKpi>(4)
        {
            new("Publish rate", "pubsub.publish.count", publish, "msg/s",
                Delta(publishSeries), false, publishSeries),
            new("Consume rate", "pubsub.consume.count", consume, "msg/s",
                Delta(consumeSeries), false, consumeSeries),
            new("DLQ publish rate", "pubsub.dlq.publish.count", dlq, "msg/s",
                0, true, []),
            // Handler p95 is not available from the broker.
            new("Handler p95", "pubsub.consumer.handler_ms", 0, "ms", 0, false, []),
        };
        return new PubSubKpis(kpis);
    }

    public async Task<IReadOnlyList<FailedMessage>> GetFailedMessagesAsync(
        FailedQuery query, CancellationToken cancellationToken = default)
    {
        var descriptors = await BuildDescriptorsAsync(cancellationToken).ConfigureAwait(false);
        return await errorOps.GetFailedMessagesAsync(descriptors, query, cancellationToken).ConfigureAwait(false);
    }

    public async Task<ReplayResult> ReplayAsync(ReplayRequest request, CancellationToken cancellationToken = default)
    {
        var descriptors = await BuildDescriptorsAsync(cancellationToken).ConfigureAwait(false);
        var descriptor = descriptors.FirstOrDefault(d => d.ErrorQueue == request.ErrorQueue);
        if (descriptor is null)
            return new ReplayResult(0, request.MessageIds.Count);
        return await errorOps.ReplayAsync(descriptor, request.MessageIds, cancellationToken).ConfigureAwait(false);
    }

    public async Task<DeleteResult> DeleteAsync(DeleteRequest request, CancellationToken cancellationToken = default)
    {
        var descriptors = await BuildDescriptorsAsync(cancellationToken).ConfigureAwait(false);
        var descriptor = descriptors.FirstOrDefault(d => d.ErrorQueue == request.ErrorQueue);
        if (descriptor is null)
            return new DeleteResult(0, request.MessageIds.Count);
        return await errorOps.DeleteAsync(descriptor, request.MessageIds, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Resolves the consumer queues (base queues that have an <c>{name}.error</c> sibling) and their
    /// origin exchange/routing key from the broker's bindings, for the AMQP error-queue operations.
    /// </summary>
    private async Task<IReadOnlyList<ErrorQueueDescriptor>> BuildDescriptorsAsync(CancellationToken ct)
    {
        var queuesResult = await client.GetQueuesAsync(options.VHost, ct).ConfigureAwait(false);
        if (!queuesResult.Ok) return [];
        var queues = queuesResult.Value!;
        var bindings = (await client.GetBindingsAsync(options.VHost, ct).ConfigureAwait(false)).Value ?? [];
        var names = new HashSet<string>(queues.Select(q => q.Name), StringComparer.Ordinal);

        var descriptors = new List<ErrorQueueDescriptor>();
        foreach (var q in queues)
        {
            if (IsErrorQueue(q.Name)) continue;
            var errorQueue = q.Name + ErrorSuffix;
            if (!names.Contains(errorQueue)) continue;
            var (exchange, routingKey) = ResolveBinding(bindings, q.Name);
            descriptors.Add(new ErrorQueueDescriptor(q.Name, errorQueue, exchange, routingKey, q.Name));
        }
        return descriptors;
    }

    private static bool IsErrorQueue(string name) => name.EndsWith(ErrorSuffix, StringComparison.Ordinal);

    private static double ErrorPercent(double consumeRate, double dlqRate)
        => consumeRate > 0 ? dlqRate / consumeRate * 100 : dlqRate > 0 ? 100 : 0;

    private static QueueHealth Health(long depth, double errPct)
    {
        if (errPct > 5) return QueueHealth.Critical;
        if (errPct > 1 || depth > 5000) return QueueHealth.Warning;
        return QueueHealth.Healthy;
    }

    /// <summary>First non-default-exchange binding for <paramref name="queue"/>, or empty if none.</summary>
    private static (string Exchange, string RoutingKey) ResolveBinding(
        IReadOnlyList<ManagementBinding> bindings, string queue)
    {
        foreach (var b in bindings)
        {
            if (b.DestinationType == "queue" && b.Destination == queue && b.Source.Length > 0)
                return (b.Source, b.RoutingKey);
        }
        return (string.Empty, string.Empty);
    }

    private static Dictionary<string, int> PrefetchByQueue(IReadOnlyList<ManagementConsumer> consumers)
    {
        var map = new Dictionary<string, int>(StringComparer.Ordinal);
        foreach (var c in consumers)
        {
            var name = c.Queue?.Name;
            if (string.IsNullOrEmpty(name)) continue;
            // Highest prefetch wins if a queue has multiple consumers.
            if (!map.TryGetValue(name, out var existing) || c.PrefetchCount > existing)
                map[name] = c.PrefetchCount;
        }
        return map;
    }

    /// <summary>
    /// Turns the Management API's cumulative counter samples into a per-second rate series
    /// (oldest→newest) for a sparkline. Samples are sorted by timestamp; a counter reset or a
    /// zero time delta yields <c>0</c> for that point.
    /// </summary>
    private static IReadOnlyList<double> RateSeries(ManagementRateDetails? details)
    {
        var samples = details?.Samples;
        if (samples is null || samples.Count < 2) return [];

        var ordered = samples.OrderBy(s => s.Timestamp).ToArray();
        var series = new List<double>(ordered.Length - 1);
        for (var i = 1; i < ordered.Length; i++)
        {
            var deltaMs = ordered[i].Timestamp - ordered[i - 1].Timestamp;
            var deltaValue = ordered[i].Sample - ordered[i - 1].Sample;
            series.Add(deltaMs > 0 && deltaValue > 0 ? deltaValue / (deltaMs / 1000.0) : 0);
        }
        return series;
    }

    private static double Delta(IReadOnlyList<double> series)
    {
        if (series.Count < 2) return 0;
        var first = series[0];
        var last = series[^1];
        return first > 0 ? (last - first) / first * 100 : 0;
    }

    private string AmqpEndpoint()
    {
        if (Uri.TryCreate(options.ConnectionString, UriKind.Absolute, out var uri))
            return $"amqp://{uri.Host}:{(uri.Port > 0 ? uri.Port : 5672)}";
        return string.Empty;
    }
}
