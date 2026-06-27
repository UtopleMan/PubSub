using Microsoft.Extensions.Logging;
using RabbitMQ.Client;

namespace PubSub.RabbitMQ;

internal sealed class RabbitMqBatchPublisher<T> : IBatchPublish<T>, IAsyncDisposable
    where T : class
{
    private static readonly IPubSubDispatcher<T> Dispatcher = PubSubDispatcherRegistry.GetOrFallback<T>();

    private readonly PubSubConnectionProvider _connectionProvider;
    private readonly PubSubTopicAttribute _topic;
    private readonly int _batchSize;
    private readonly TimeSpan _flushInterval;
    private readonly ILogger<RabbitMqBatchPublisher<T>> _logger;
    private readonly SemaphoreSlim _channelGate = new(1, 1);
    private readonly Lock _batchLock = new();
    private readonly Timer _flushTimer;
    private readonly KeyValuePair<string, object?> _routingKeyTag;
    private IChannel? _channel;
    private Batch _current = new();
    private bool _disposed;

    public RabbitMqBatchPublisher(
        PubSubConnectionProvider connectionProvider,
        PubSubTopicAttribute topic,
        BatchedPublishAttribute batchedSettings,
        ILogger<RabbitMqBatchPublisher<T>> logger)
    {
        _connectionProvider = connectionProvider;
        _topic = topic;
        _batchSize = batchedSettings.BatchSize;
        _flushInterval = TimeSpan.FromMilliseconds(batchedSettings.FlushIntervalMs);
        _logger = logger;
        _routingKeyTag = new KeyValuePair<string, object?>("routing_key", topic.RoutingKey);
        _flushTimer = new Timer(static state => ((RabbitMqBatchPublisher<T>)state!).TimerTick(), this,
            _flushInterval, _flushInterval);
    }

    public Task PublishAsync(T message, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(message);
        if (_disposed) throw new ObjectDisposedException(nameof(RabbitMqBatchPublisher<T>));

        Batch batchToFlush;
        Task tcsTask;
        var routingKey = message is IRoutingKeyProvider rkp ? rkp.GetRoutingKey() : _topic.RoutingKey;
        lock (_batchLock)
        {
            _current.Entries.Add((routingKey, Dispatcher.Serialize(message)));
            tcsTask = _current.Tcs.Task;
            if (_current.Entries.Count < _batchSize) return tcsTask;
            batchToFlush = _current;
            _current = new Batch();
        }
        _ = Task.Run(() => FlushAsync(batchToFlush, cancellationToken));
        return tcsTask;
    }

    private void TimerTick()
    {
        Batch? batchToFlush;
        lock (_batchLock)
        {
            if (_current.Entries.Count == 0) return;
            batchToFlush = _current;
            _current = new Batch();
        }
        _ = Task.Run(() => FlushAsync(batchToFlush, CancellationToken.None));
    }

    private async Task FlushAsync(Batch batch, CancellationToken cancellationToken)
    {
        try
        {
            var channel = await GetOrOpenChannelAsync(cancellationToken).ConfigureAwait(false);
            var props = new BasicProperties
            {
                ContentType = "application/json",
                ContentEncoding = "utf-8",
                DeliveryMode = DeliveryModes.Persistent,
            };
            foreach (var entry in batch.Entries)
            {
                await channel.BasicPublishAsync(
                    exchange: _topic.Exchange,
                    routingKey: entry.RoutingKey,
                    mandatory: false,
                    basicProperties: props,
                    body: entry.Body,
                    cancellationToken: cancellationToken).ConfigureAwait(false);
            }
            PubSubDiagnostics.PublishCount.Add(batch.Entries.Count, _routingKeyTag);
            batch.Tcs.TrySetResult();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Batch flush failed for {RoutingKey} ({Count} messages)", _topic.RoutingKey, batch.Entries.Count);
            batch.Tcs.TrySetException(ex);
        }
    }

    private async ValueTask<IChannel> GetOrOpenChannelAsync(CancellationToken cancellationToken)
    {
        if (_channel is { IsOpen: true }) return _channel;
        await _channelGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_channel is { IsOpen: true }) return _channel;
            var conn = await _connectionProvider.GetOrOpenAsync(cancellationToken).ConfigureAwait(false);
            _channel = await conn.CreateChannelAsync(
                new CreateChannelOptions(publisherConfirmationsEnabled: true, publisherConfirmationTrackingEnabled: true),
                cancellationToken).ConfigureAwait(false);
            await _channel.ExchangeDeclareAsync(
                exchange: _topic.Exchange, type: "topic", durable: true, autoDelete: false,
                cancellationToken: cancellationToken).ConfigureAwait(false);
            return _channel;
        }
        finally
        {
            _channelGate.Release();
        }
    }

    public async ValueTask DisposeAsync()
    {
        _disposed = true;
        await _flushTimer.DisposeAsync().ConfigureAwait(false);
        Batch? finalBatch;
        lock (_batchLock)
        {
            finalBatch = _current.Entries.Count > 0 ? _current : null;
            _current = new Batch();
        }
        if (finalBatch is not null)
        {
            try { await FlushAsync(finalBatch, CancellationToken.None).ConfigureAwait(false); }
            catch { }
        }
        if (_channel is not null)
        {
            try { await _channel.CloseAsync().ConfigureAwait(false); } catch { }
            _channel.Dispose();
        }
        _channelGate.Dispose();
    }

    private sealed class Batch
    {
        public List<(string RoutingKey, ReadOnlyMemory<byte> Body)> Entries { get; } = new();
        public TaskCompletionSource Tcs { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    }
}
