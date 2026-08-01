using System.Security.Cryptography;
using System.Text;
using Microsoft.Extensions.Logging;
using PubSub.Admin;
using RabbitMQ.Client;

namespace PubSub.RabbitMQ.Admin;

/// <summary>
/// Identifies a consumer queue and its dead-letter sibling for the AMQP error-queue operations.
/// Resolved by the admin from the broker's queue list + bindings (not from local registration).
/// </summary>
internal sealed record ErrorQueueDescriptor(
    string Queue, string ErrorQueue, string Exchange, string RoutingKey, string ConsumerName);

/// <summary>
/// AMQP side of the hybrid admin: peeks, replays, and deletes messages in <c>{queue}.error</c>
/// queues. Owns a lightweight connection built from <see cref="PubSubAdminOptions.ConnectionString"/>,
/// independent of the library's <c>PubSubConnectionProvider</c>, so Pulse can run standalone.
/// <para>
/// Salvaged verbatim from the previous co-located admin (the logic is solid): peek uses
/// <c>BasicGet</c> + requeue; replay republishes with publisher confirms preserving the body and
/// <c>x-original-routing-key</c>; delete drains and acks. On AMQP failure every method degrades
/// (empty list / zero counts + a logged warning) rather than throwing to the endpoint.
/// </para>
/// </summary>
internal sealed class ErrorQueueOperations(
    PubSubAdminOptions options, ILogger<ErrorQueueOperations> logger) : IAsyncDisposable
{
    private readonly SemaphoreSlim _connectionGate = new(1, 1);
    private IConnection? _connection;

    public async Task<IReadOnlyList<FailedMessage>> GetFailedMessagesAsync(
        IReadOnlyList<ErrorQueueDescriptor> descriptors, FailedQuery query, CancellationToken cancellationToken = default)
    {
        IConnection conn;
        try
        {
            conn = await GetConnectionAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "AMQP unavailable; returning no failed messages");
            return [];
        }

        var found = new List<FailedMessage>();
        await using var channel = await conn.CreateChannelAsync(
            new CreateChannelOptions(publisherConfirmationsEnabled: false, publisherConfirmationTrackingEnabled: false),
            cancellationToken).ConfigureAwait(false);

        foreach (var c in descriptors)
        {
            var held = new List<ulong>();
            for (var i = 0; i < options.FailedPeekPerQueue; i++)
            {
                BasicGetResult? res;
                try
                {
                    res = await channel.BasicGetAsync(c.ErrorQueue, autoAck: false, cancellationToken).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    logger.LogDebug(ex, "Peek of error queue {Queue} failed (may not exist yet)", c.ErrorQueue);
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

    public async Task<ReplayResult> ReplayAsync(
        ErrorQueueDescriptor descriptor, IReadOnlyList<string> messageIds, CancellationToken cancellationToken = default)
    {
        IConnection conn;
        try
        {
            conn = await GetConnectionAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "AMQP unavailable; replay of {Queue} skipped", descriptor.ErrorQueue);
            return new ReplayResult(0, messageIds.Count);
        }

        await using var channel = await conn.CreateChannelAsync(
            new CreateChannelOptions(publisherConfirmationsEnabled: true, publisherConfirmationTrackingEnabled: true),
            cancellationToken).ConfigureAwait(false);

        var wanted = new HashSet<string>(messageIds, StringComparer.Ordinal);
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

    public async Task<DeleteResult> DeleteAsync(
        ErrorQueueDescriptor descriptor, IReadOnlyList<string> messageIds, CancellationToken cancellationToken = default)
    {
        IConnection conn;
        try
        {
            conn = await GetConnectionAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "AMQP unavailable; delete of {Queue} skipped", descriptor.ErrorQueue);
            return new DeleteResult(0, messageIds.Count);
        }

        await using var channel = await conn.CreateChannelAsync(
            new CreateChannelOptions(publisherConfirmationsEnabled: false, publisherConfirmationTrackingEnabled: false),
            cancellationToken).ConfigureAwait(false);

        var wanted = new HashSet<string>(messageIds, StringComparer.Ordinal);
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

    private async Task<IConnection> GetConnectionAsync(CancellationToken ct)
    {
        if (_connection is { IsOpen: true }) return _connection;
        await _connectionGate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            if (_connection is { IsOpen: true }) return _connection;
            if (_connection is not null)
            {
                await _connection.DisposeAsync().ConfigureAwait(false);
                _connection = null;
            }
            var factory = new ConnectionFactory
            {
                Uri = new Uri(options.ConnectionString),
                AutomaticRecoveryEnabled = true,
            };
            _connection = await factory.CreateConnectionAsync(ct).ConfigureAwait(false);
            return _connection;
        }
        finally
        {
            _connectionGate.Release();
        }
    }

    /// <summary>
    /// Single-pass drain of an error queue: fetch each message once (bounded by the current
    /// depth), invoke <paramref name="decide"/>; if it returns true the message is acked
    /// (removed), otherwise it is nacked back onto the queue.
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
            logger.LogDebug(ex, "Drain: error queue {Queue} not found", errorQueue);
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
                logger.LogWarning(ex, "Drain action failed for a message on {Queue}; requeuing", errorQueue);
                await channel.BasicNackAsync(res.DeliveryTag, multiple: false, requeue: true, ct).ConfigureAwait(false);
                continue;
            }
            if (ack)
                await channel.BasicAckAsync(res.DeliveryTag, multiple: false, ct).ConfigureAwait(false);
            else
                await channel.BasicNackAsync(res.DeliveryTag, multiple: false, requeue: true, ct).ConfigureAwait(false);
        }
    }

    private FailedMessage BuildFailedMessage(ErrorQueueDescriptor c, BasicGetResult res)
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
            Endpoint: options.ServiceName,
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

    private string ShovelCommand(ErrorQueueDescriptor c)
    {
        var host = "rabbit:5672";
        if (Uri.TryCreate(options.ConnectionString, UriKind.Absolute, out var uri))
            host = $"{uri.Host}:{(uri.Port > 0 ? uri.Port : 5672)}";
        return $"rabbitmqadmin --uri=amqp://{host} shovel " +
               $"--src-queue={c.ErrorQueue} --dest-exchange={c.Exchange} --dest-exchange-key={c.RoutingKey}";
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

    public async ValueTask DisposeAsync()
    {
        if (_connection is not null)
        {
            try { await _connection.CloseAsync().ConfigureAwait(false); } catch { /* best effort */ }
            await _connection.DisposeAsync().ConfigureAwait(false);
            _connection = null;
        }
        _connectionGate.Dispose();
    }
}
