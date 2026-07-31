using System.Security.Cryptography;
using System.Text;
using Microsoft.Extensions.Logging;
using PubSub.Admin;
using RabbitMQ.Client;

namespace PubSub.RabbitMQ;

/// <summary>
/// RabbitMQ implementation of the vendor-neutral <see cref="IPubSubAdmin"/>. Reads registered
/// topology from the in-process <see cref="PubSubRegistrationSnapshot"/>, live depth/consumer
/// counts via passive queue declares, rates/p95/in-flight from <see cref="PubSubMetricsSampler"/>,
/// and dead-letter contents by peeking each <c>{queue}.error</c> queue over AMQP.
/// </summary>
internal sealed class RabbitMqPubSubAdmin : IPubSubAdmin
{
    private readonly PubSubConnectionProvider _connections;
    private readonly PubSubRabbitMqOptions _options;
    private readonly PubSubAdminOptions _adminOptions;
    private readonly PubSubMetricsSampler _sampler;
    private readonly ILogger<RabbitMqPubSubAdmin> _logger;
    private readonly IReadOnlyList<ConsumerDescriptor> _consumers;
    private readonly Dictionary<string, ConsumerDescriptor> _byErrorQueue;

    public RabbitMqPubSubAdmin(
        PubSubConnectionProvider connections,
        PubSubRabbitMqOptions options,
        PubSubAdminOptions adminOptions,
        PubSubRegistrationSnapshot registrations,
        PubSubMetricsSampler sampler,
        ILogger<RabbitMqPubSubAdmin> logger)
    {
        _connections = connections;
        _options = options;
        _adminOptions = adminOptions;
        _sampler = sampler;
        _logger = logger;
        _consumers = registrations.Consumers.Select(c =>
        {
            var queue = PubSubQueueNaming.ResolveQueueName(c.Topic, c.ConsumerType);
            return new ConsumerDescriptor(
                Queue: queue,
                ErrorQueue: $"{queue}.error",
                Exchange: c.Topic.Exchange,
                RoutingKey: c.Topic.RoutingKey,
                ConsumerName: c.ConsumerType.Name,
                Prefetch: c.Prefetch);
        }).ToArray();
        _byErrorQueue = _consumers.ToDictionary(c => c.ErrorQueue, StringComparer.Ordinal);
    }

    public Task<MessagingInstallation> GetInstallationAsync(CancellationToken cancellationToken = default)
    {
        string endpoint = "", vhost = "/";
        if (Uri.TryCreate(_options.ConnectionString, UriKind.Absolute, out var uri))
        {
            endpoint = $"amqp://{uri.Host}:{(uri.Port > 0 ? uri.Port : 5672)}";
            var path = Uri.UnescapeDataString(uri.AbsolutePath).Trim('/');
            vhost = path.Length == 0 ? "/" : path;
        }
        var env = Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT")
                  ?? Environment.GetEnvironmentVariable("DOTNET_ENVIRONMENT")
                  ?? "Production";
        return Task.FromResult(new MessagingInstallation(
            Provider: "RabbitMQ",
            Endpoint: endpoint,
            VHost: vhost,
            Environment: env,
            Connected: _connections.IsConnected));
    }

    public async Task<IReadOnlyList<QueueStat>> GetQueueStatsAsync(CancellationToken cancellationToken = default)
    {
        var snap = _sampler.Current;
        var conn = await _connections.GetOrOpenAsync(cancellationToken).ConfigureAwait(false);
        var depths = await GetDepthsAsync(conn, _consumers.Select(c => c.Queue).ToArray(), cancellationToken)
            .ConfigureAwait(false);

        var stats = new List<QueueStat>(_consumers.Count);
        foreach (var c in _consumers)
        {
            var publishRate = snap.PublishRateByRoutingKey.GetValueOrDefault(c.RoutingKey);
            var consumeRate = snap.ConsumeRateByQueue.GetValueOrDefault(c.Queue);
            var dlqRate = snap.DlqRateByQueue.GetValueOrDefault(c.Queue);
            var (depth, consumers) = depths.GetValueOrDefault(c.Queue, (0L, 0));
            var p95 = snap.HandlerP95ByQueue.GetValueOrDefault(c.Queue);
            var inflight = snap.InFlightByQueue.GetValueOrDefault(c.Queue);
            var series = snap.ConsumeSeriesByQueue.GetValueOrDefault(c.Queue) ?? [];
            var errPct = consumeRate > 0 ? dlqRate / consumeRate * 100 : dlqRate > 0 ? 100 : 0;

            stats.Add(new QueueStat(
                Queue: c.Queue,
                Exchange: c.Exchange,
                RoutingKey: c.RoutingKey,
                Endpoint: _adminOptions.ServiceName,
                Consumer: c.ConsumerName,
                Prefetch: c.Prefetch,
                PublishRate: publishRate,
                ConsumeRate: consumeRate,
                Depth: depth,
                Consumers: consumers,
                DlqRate: dlqRate,
                ErrorPercent: errPct,
                HandlerP95Ms: p95,
                InFlightAgeMs: inflight,
                ConsumeSeries: series,
                Status: Health(inflight, errPct, depth)));
        }
        return stats;
    }

    public async Task<TopologySnapshot> GetTopologyAsync(CancellationToken cancellationToken = default)
    {
        var snap = _sampler.Current;
        var conn = await _connections.GetOrOpenAsync(cancellationToken).ConfigureAwait(false);
        var errorDepths = await GetDepthsAsync(conn, _consumers.Select(c => c.ErrorQueue).ToArray(), cancellationToken)
            .ConfigureAwait(false);

        var exchanges = _consumers
            .GroupBy(c => c.Exchange, StringComparer.Ordinal)
            .Select(g => new ExchangeInfo(
                Name: g.Key,
                TopicCount: g.Select(c => c.RoutingKey).Distinct(StringComparer.Ordinal).Count(),
                ConsumeRate: g.Sum(c => snap.ConsumeRateByQueue.GetValueOrDefault(c.Queue))))
            .OrderBy(e => e.Name, StringComparer.Ordinal)
            .ToArray();

        var topics = _consumers
            .GroupBy(c => (c.Exchange, c.RoutingKey))
            .Select(g => new TopicBinding(
                Exchange: g.Key.Exchange,
                RoutingKey: g.Key.RoutingKey,
                ConsumerCount: g.Count(),
                ConsumeRate: g.Sum(c => snap.ConsumeRateByQueue.GetValueOrDefault(c.Queue)),
                FailedCount: (int)g.Sum(c => errorDepths.GetValueOrDefault(c.ErrorQueue, (0L, 0)).Item1)))
            .OrderBy(t => t.Exchange, StringComparer.Ordinal)
            .ThenBy(t => t.RoutingKey, StringComparer.Ordinal)
            .ToArray();

        return new TopologySnapshot(exchanges, topics);
    }

    public Task<PubSubKpis> GetKpisAsync(CancellationToken cancellationToken = default)
    {
        var snap = _sampler.Current;
        var publish = Sum(snap.PublishRateByRoutingKey);
        var consume = Sum(snap.ConsumeRateByQueue);
        var dlq = Sum(snap.DlqRateByQueue);

        var kpis = new List<PubSubKpi>(4)
        {
            new("Publish rate", "pubsub.publish.count", publish, "msg/s",
                Delta(snap.PublishRateSeries), false, snap.PublishRateSeries),
            new("Consume rate", "pubsub.consume.count", consume, "msg/s",
                Delta(snap.ConsumeRateSeries), false, snap.ConsumeRateSeries),
            new("DLQ publish rate", "pubsub.dlq.publish.count", dlq, "msg/s",
                Delta(snap.DlqRateSeries), true, snap.DlqRateSeries),
            new("Handler p95", "pubsub.consumer.handler_ms", snap.OverallHandlerP95Ms,
                $"across {_consumers.Count} queues", Delta(snap.HandlerP95Series), false, snap.HandlerP95Series),
        };
        return Task.FromResult(new PubSubKpis(kpis));
    }

    public async Task<IReadOnlyList<FailedMessage>> GetFailedMessagesAsync(
        FailedQuery query, CancellationToken cancellationToken = default)
    {
        var conn = await _connections.GetOrOpenAsync(cancellationToken).ConfigureAwait(false);
        var found = new List<FailedMessage>();

        await using var channel = await conn.CreateChannelAsync(
            new CreateChannelOptions(publisherConfirmationsEnabled: false, publisherConfirmationTrackingEnabled: false),
            cancellationToken).ConfigureAwait(false);

        foreach (var c in _consumers)
        {
            var held = new List<ulong>();
            for (var i = 0; i < _adminOptions.FailedPeekPerQueue; i++)
            {
                BasicGetResult? res;
                try
                {
                    res = await channel.BasicGetAsync(c.ErrorQueue, autoAck: false, cancellationToken).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    _logger.LogDebug(ex, "Peek of error queue {Queue} failed (may not exist yet)", c.ErrorQueue);
                    break;
                }
                if (res is null) break;
                held.Add(res.DeliveryTag);
                found.Add(BuildFailedMessage(c, res));
            }
            // Requeue everything we peeked so nothing is consumed by the console.
            if (held.Count > 0)
                await channel.BasicNackAsync(held[^1], multiple: true, requeue: true, cancellationToken).ConfigureAwait(false);
        }

        IEnumerable<FailedMessage> filtered = found;
        if (!string.IsNullOrWhiteSpace(query.ExceptionType))
            filtered = filtered.Where(f => f.ExceptionType == query.ExceptionType);
        if (!string.IsNullOrWhiteSpace(query.Search))
        {
            var term = query.Search.Trim();
            filtered = filtered.Where(f =>
                Contains(f.ErrorQueue, term) || Contains(f.ExceptionType, term) ||
                Contains(f.ExceptionMessage, term) || Contains(f.Pod, term) || Contains(f.Id, term));
        }
        return filtered
            .OrderByDescending(f => f.LastSeen)
            .Take(Math.Max(1, query.Limit))
            .ToArray();
    }

    public async Task<ReplayResult> ReplayAsync(ReplayRequest request, CancellationToken cancellationToken = default)
    {
        if (!_byErrorQueue.TryGetValue(request.ErrorQueue, out var descriptor))
            return new ReplayResult(0, request.MessageIds.Count);

        var conn = await _connections.GetOrOpenAsync(cancellationToken).ConfigureAwait(false);
        await using var channel = await conn.CreateChannelAsync(
            new CreateChannelOptions(publisherConfirmationsEnabled: true, publisherConfirmationTrackingEnabled: true),
            cancellationToken).ConfigureAwait(false);

        var wanted = new HashSet<string>(request.MessageIds, StringComparer.Ordinal);
        var replayAll = wanted.Count == 0;
        var matched = new HashSet<string>(StringComparer.Ordinal);
        var replayed = 0;

        await DrainAsync(channel, descriptor.ErrorQueue, cancellationToken, async (res, id) =>
        {
            if (!replayAll && !wanted.Contains(id)) return false; // keep (requeue)

            var routingKey = HeaderString(res.BasicProperties, "x-original-routing-key");
            if (string.IsNullOrEmpty(routingKey)) routingKey = descriptor.RoutingKey;
            var props = new BasicProperties
            {
                ContentType = res.BasicProperties.ContentType ?? "application/json",
                ContentEncoding = res.BasicProperties.ContentEncoding ?? "utf-8",
                DeliveryMode = DeliveryModes.Persistent,
            };
            await channel.BasicPublishAsync(
                exchange: descriptor.Exchange, routingKey: routingKey, mandatory: false,
                basicProperties: props, body: res.Body, cancellationToken).ConfigureAwait(false);
            matched.Add(id);
            replayed++;
            return true; // ack (remove from error queue)
        }).ConfigureAwait(false);

        return new ReplayResult(replayed, replayAll ? 0 : wanted.Count - matched.Count);
    }

    public async Task<DeleteResult> DeleteAsync(DeleteRequest request, CancellationToken cancellationToken = default)
    {
        if (!_byErrorQueue.TryGetValue(request.ErrorQueue, out var descriptor))
            return new DeleteResult(0, request.MessageIds.Count);

        var conn = await _connections.GetOrOpenAsync(cancellationToken).ConfigureAwait(false);
        await using var channel = await conn.CreateChannelAsync(
            new CreateChannelOptions(publisherConfirmationsEnabled: false, publisherConfirmationTrackingEnabled: false),
            cancellationToken).ConfigureAwait(false);

        var wanted = new HashSet<string>(request.MessageIds, StringComparer.Ordinal);
        var deleteAll = wanted.Count == 0;
        var matched = new HashSet<string>(StringComparer.Ordinal);
        var deleted = 0;

        await DrainAsync(channel, descriptor.ErrorQueue, cancellationToken, (res, id) =>
        {
            if (!deleteAll && !wanted.Contains(id)) return new ValueTask<bool>(false); // keep
            matched.Add(id);
            deleted++;
            return new ValueTask<bool>(true); // ack (drop)
        }).ConfigureAwait(false);

        return new DeleteResult(deleted, deleteAll ? 0 : wanted.Count - matched.Count);
    }

    /// <summary>
    /// Single-pass drain of an error queue: fetch each message once (bounded by the current
    /// depth), invoke <paramref name="decide"/>; if it returns true the message is acked
    /// (removed), otherwise it is nacked back onto the queue. Messages fetched but not decided
    /// (loop bound reached) are requeued.
    /// </summary>
    private async Task DrainAsync(
        IChannel channel, string errorQueue, CancellationToken ct,
        Func<BasicGetResult, string, ValueTask<bool>> decide)
    {
        long depth;
        try
        {
            var ok = await channel.QueueDeclarePassiveAsync(errorQueue, ct).ConfigureAwait(false);
            depth = ok.MessageCount;
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Drain: error queue {Queue} not found", errorQueue);
            return;
        }
        if (depth == 0) return;

        var budget = (int)Math.Min(depth, 100_000);
        for (var i = 0; i < budget; i++)
        {
            var res = await channel.BasicGetAsync(errorQueue, autoAck: false, ct).ConfigureAwait(false);
            if (res is null) break;
            var id = MessageId(res);
            bool ack;
            try
            {
                ack = await decide(res, id).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Drain action failed for a message on {Queue}; requeuing", errorQueue);
                await channel.BasicNackAsync(res.DeliveryTag, multiple: false, requeue: true, ct).ConfigureAwait(false);
                continue;
            }
            if (ack)
                await channel.BasicAckAsync(res.DeliveryTag, multiple: false, ct).ConfigureAwait(false);
            else
                await channel.BasicNackAsync(res.DeliveryTag, multiple: false, requeue: true, ct).ConfigureAwait(false);
        }
    }

    private FailedMessage BuildFailedMessage(ConsumerDescriptor c, BasicGetResult res)
    {
        var props = res.BasicProperties;
        var exType = HeaderString(props, "x-exception-type");
        var exMessage = HeaderString(props, "x-exception");
        var stack = HeaderString(props, "x-stacktrace");
        var pod = HeaderString(props, "x-pod-name");
        var handlerElapsed = HeaderLong(props, "x-handler-elapsed-ms");
        var deliveryCount = HeaderLong(props, "x-delivery-count");
        var lastSeen = props.IsTimestampPresent() && props.Timestamp.UnixTime > 0
            ? DateTimeOffset.FromUnixTimeSeconds(props.Timestamp.UnixTime)
            : DateTimeOffset.UtcNow;

        var headers = new List<FailedMessageHeader>();
        AddHeader(headers, props, "x-exception-type");
        AddHeader(headers, props, "x-exception");
        AddHeader(headers, props, "x-consumer-handler");
        AddHeader(headers, props, "x-handler-elapsed-ms");
        AddHeader(headers, props, "x-original-routing-key");
        AddHeader(headers, props, "x-pod-name");

        return new FailedMessage(
            Id: MessageId(res),
            ErrorQueue: c.ErrorQueue,
            Exchange: c.Exchange,
            RoutingKey: c.RoutingKey,
            Consumer: c.ConsumerName,
            Endpoint: _adminOptions.ServiceName,
            ExceptionType: exType.Length > 0 ? exType : "(unknown)",
            ExceptionMessage: exMessage,
            HandlerElapsedMs: handlerElapsed,
            Pod: pod,
            LastSeen: lastSeen,
            Count: deliveryCount > 0 ? (int)deliveryCount : 1,
            Headers: headers,
            StackTrace: stack,
            ShovelCommand: ShovelCommand(c));
    }

    private string ShovelCommand(ConsumerDescriptor c)
    {
        var host = "rabbit:5672";
        if (Uri.TryCreate(_options.ConnectionString, UriKind.Absolute, out var uri))
            host = $"{uri.Host}:{(uri.Port > 0 ? uri.Port : 5672)}";
        return $"rabbitmqadmin --uri=amqp://{host} shovel " +
               $"--src-queue={c.ErrorQueue} --dest-exchange={c.Exchange} --dest-exchange-key={c.RoutingKey}";
    }

    private QueueHealth Health(long inflight, double errPct, long depth)
    {
        var threshold = _adminOptions.WedgeThresholdMs;
        if (inflight > threshold || errPct > 5) return QueueHealth.Critical;
        if (inflight > threshold / 3 || errPct > 1 || depth > 5000) return QueueHealth.Warning;
        return QueueHealth.Healthy;
    }

    private static async Task<Dictionary<string, (long depth, int consumers)>> GetDepthsAsync(
        IConnection conn, IReadOnlyList<string> queues, CancellationToken ct)
    {
        var result = new Dictionary<string, (long, int)>(StringComparer.Ordinal);
        IChannel? ch = null;
        try
        {
            ch = await conn.CreateChannelAsync(
                new CreateChannelOptions(publisherConfirmationsEnabled: false, publisherConfirmationTrackingEnabled: false),
                ct).ConfigureAwait(false);
            foreach (var q in queues)
            {
                try
                {
                    var ok = await ch.QueueDeclarePassiveAsync(q, ct).ConfigureAwait(false);
                    result[q] = ((long)ok.MessageCount, (int)ok.ConsumerCount);
                }
                catch
                {
                    // A failed passive declare closes the channel; reopen for the remaining queues.
                    try { await ch.CloseAsync(ct).ConfigureAwait(false); } catch { }
                    ch.Dispose();
                    ch = await conn.CreateChannelAsync(
                        new CreateChannelOptions(publisherConfirmationsEnabled: false, publisherConfirmationTrackingEnabled: false),
                        ct).ConfigureAwait(false);
                    result[q] = (0, 0);
                }
            }
        }
        finally
        {
            if (ch is not null)
            {
                try { await ch.CloseAsync(ct).ConfigureAwait(false); } catch { }
                ch.Dispose();
            }
        }
        return result;
    }

    private static double Delta(IReadOnlyList<double> series)
    {
        if (series.Count < 2) return 0;
        var first = series[0];
        var last = series[^1];
        return first > 0 ? (last - first) / first * 100 : 0;
    }

    private static double Sum(IReadOnlyDictionary<string, double> d)
    {
        double s = 0;
        foreach (var v in d.Values) s += v;
        return s;
    }

    private static bool Contains(string haystack, string term)
        => haystack.Contains(term, StringComparison.OrdinalIgnoreCase);

    private static string MessageId(BasicGetResult res)
    {
        if (res.BasicProperties.IsMessageIdPresent() && !string.IsNullOrEmpty(res.BasicProperties.MessageId))
            return res.BasicProperties.MessageId!;
        // Stable content hash: body + the two identifying exception headers.
        var seed = HeaderString(res.BasicProperties, "x-exception-type") +
                   "|" + HeaderString(res.BasicProperties, "x-exception");
        Span<byte> hash = stackalloc byte[32];
        var buffer = new byte[res.Body.Length + Encoding.UTF8.GetByteCount(seed)];
        res.Body.Span.CopyTo(buffer);
        Encoding.UTF8.GetBytes(seed, buffer.AsSpan(res.Body.Length));
        SHA256.HashData(buffer, hash);
        return Convert.ToHexStringLower(hash)[..16];
    }

    private static void AddHeader(List<FailedMessageHeader> list, IReadOnlyBasicProperties props, string key)
    {
        var value = HeaderString(props, key);
        if (value.Length > 0) list.Add(new FailedMessageHeader(key, value));
    }

    private static string HeaderString(IReadOnlyBasicProperties props, string key)
    {
        if (props.Headers is null || !props.Headers.TryGetValue(key, out var raw) || raw is null)
            return string.Empty;
        return raw switch
        {
            byte[] bytes => Encoding.UTF8.GetString(bytes),
            string s => s,
            _ => raw.ToString() ?? string.Empty,
        };
    }

    private static long HeaderLong(IReadOnlyBasicProperties props, string key)
    {
        if (props.Headers is null || !props.Headers.TryGetValue(key, out var raw) || raw is null)
            return 0;
        return raw switch
        {
            long l => l,
            int i => i,
            byte[] bytes when long.TryParse(Encoding.UTF8.GetString(bytes), out var v) => v,
            string s when long.TryParse(s, out var v) => v,
            _ => 0,
        };
    }

    private sealed record ConsumerDescriptor(
        string Queue, string ErrorQueue, string Exchange, string RoutingKey, string ConsumerName, int Prefetch);
}
