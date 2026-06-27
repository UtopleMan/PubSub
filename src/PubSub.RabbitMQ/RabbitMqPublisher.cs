using System.Diagnostics;
using System.Text;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;

namespace PubSub.RabbitMQ;

internal sealed class RabbitMqPublisher<T> : IPublish<T>, IAsyncDisposable
    where T : class
{
    private static readonly IPubSubDispatcher<T> Dispatcher = PubSubDispatcherRegistry.GetOrFallback<T>();

    private readonly PubSubConnectionProvider _connectionProvider;
    private readonly PubSubTopicAttribute _topic;
    private readonly TimeSpan _timeout;
    private readonly ILogger<RabbitMqPublisher<T>> _logger;
    private readonly SemaphoreSlim _gate = new(1, 1);
    private readonly string _spanName;
    private readonly KeyValuePair<string, object?> _routingKeyTag;
    private IChannel? _channel;

    public RabbitMqPublisher(
        PubSubConnectionProvider connectionProvider,
        PubSubTopicAttribute topic,
        TimeSpan timeout,
        ILogger<RabbitMqPublisher<T>> logger)
    {
        _connectionProvider = connectionProvider;
        _topic = topic;
        _timeout = timeout;
        _logger = logger;
        _spanName = $"pubsub.publish {topic.RoutingKey}";
        _routingKeyTag = new KeyValuePair<string, object?>("routing_key", topic.RoutingKey);
    }

    public async Task PublishAsync(T message, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(message);
        var channel = await GetOrOpenChannelAsync(cancellationToken).ConfigureAwait(false);

        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        linkedCts.CancelAfter(_timeout);
        var startedTicks = Environment.TickCount64;

        var hasListeners = PubSubDiagnostics.ActivitySource.HasListeners();
        Activity? activity = null;
        if (hasListeners)
        {
            activity = PubSubDiagnostics.ActivitySource.StartActivity(_spanName, ActivityKind.Producer);
            activity?.SetTag("messaging.system", "rabbitmq");
            activity?.SetTag("messaging.operation", "publish");
            activity?.SetTag("messaging.destination.name", _topic.Exchange);
            activity?.SetTag("messaging.rabbitmq.destination.routing_key", _topic.RoutingKey);
        }

        var body = Dispatcher.Serialize(message);
        activity?.SetTag("messaging.message.body.size", body.Length);

        BasicProperties props;
        if (activity is null)
        {
            props = new BasicProperties
            {
                ContentType = "application/json",
                ContentEncoding = "utf-8",
                DeliveryMode = DeliveryModes.Persistent,
            };
        }
        else
        {
            var headers = new Dictionary<string, object?>(2)
            {
                ["traceparent"] = Encoding.UTF8.GetBytes(activity.Id ?? string.Empty),
            };
            if (!string.IsNullOrEmpty(activity.TraceStateString))
                headers["tracestate"] = Encoding.UTF8.GetBytes(activity.TraceStateString);
            props = new BasicProperties
            {
                ContentType = "application/json",
                ContentEncoding = "utf-8",
                DeliveryMode = DeliveryModes.Persistent,
                Headers = headers,
            };
        }

        var routingKey = message is IRoutingKeyProvider rkp ? rkp.GetRoutingKey() : _topic.RoutingKey;
        try
        {
            await channel.BasicPublishAsync(
                exchange: _topic.Exchange,
                routingKey: routingKey,
                mandatory: false,
                basicProperties: props,
                body: body,
                cancellationToken: linkedCts.Token).ConfigureAwait(false);
            PubSubDiagnostics.PublishCount.Add(1, _routingKeyTag);
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            activity?.SetStatus(ActivityStatusCode.Error, "publish timeout");
            var elapsedMs = Environment.TickCount64 - startedTicks;
            throw new PubSubPublishTimeoutException(_topic.RoutingKey, _timeout, TimeSpan.FromMilliseconds(elapsedMs));
        }
        catch (Exception ex)
        {
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            throw;
        }
        finally
        {
            activity?.Dispose();
        }
    }

    private async ValueTask<IChannel> GetOrOpenChannelAsync(CancellationToken cancellationToken)
    {
        if (_channel is { IsOpen: true }) return _channel;
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
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
            _logger.LogDebug("Publisher channel opened for {RoutingKey}", _topic.RoutingKey);
            return _channel;
        }
        finally
        {
            _gate.Release();
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_channel is not null)
        {
            try { await _channel.CloseAsync().ConfigureAwait(false); }
            catch { }
            _channel.Dispose();
        }
        _gate.Dispose();
    }
}
