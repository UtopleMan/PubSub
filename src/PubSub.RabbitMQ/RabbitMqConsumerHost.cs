using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Text;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace PubSub.RabbitMQ;

internal sealed class RabbitMqConsumerHost<T, TConsumer> : IHostedService, IInFlightTracker, IAsyncDisposable
    where T : class
    where TConsumer : class, ISubscribeTo<T>
{
    private static readonly IPubSubDispatcher<T> Dispatcher = PubSubDispatcherRegistry.GetOrFallback<T>();

    private readonly PubSubConnectionProvider _connectionProvider;
    private readonly PubSubRabbitMqOptions _options;
    private readonly IServiceProvider _services;
    private readonly PubSubTopicAttribute _topic;
    private readonly ushort _prefetch;
    private readonly ILogger<RabbitMqConsumerHost<T, TConsumer>> _logger;
    private readonly string _queueName;
    private readonly string _errorQueueName;
    private readonly KeyValuePair<string, object?> _queueTag;
    private readonly ConcurrentDictionary<ulong, long> _inFlightStartedAtMs = new();
    private IChannel? _channel;
    private IChannel? _dlxChannel;
    private string? _consumerTag;

    public RabbitMqConsumerHost(
        PubSubConnectionProvider connectionProvider,
        PubSubRabbitMqOptions options,
        IServiceProvider services,
        PubSubTopicAttribute topic,
        ushort prefetch,
        ILogger<RabbitMqConsumerHost<T, TConsumer>> logger)
    {
        _connectionProvider = connectionProvider;
        _options = options;
        _services = services;
        _topic = topic;
        _prefetch = prefetch;
        _logger = logger;
        _queueName = PubSubQueueNaming.ResolveQueueName(topic, typeof(TConsumer));
        _errorQueueName = $"{_queueName}.error";
        _queueTag = new KeyValuePair<string, object?>("queue", _queueName);
        PubSubDiagnostics.RegisterTracker(this);
    }

    public string QueueName => _queueName;
    public string ErrorQueueName => _errorQueueName;

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        var conn = await _connectionProvider.GetOrOpenAsync(cancellationToken).ConfigureAwait(false);
        _channel = await conn.CreateChannelAsync(
            new CreateChannelOptions(publisherConfirmationsEnabled: true, publisherConfirmationTrackingEnabled: true),
            cancellationToken).ConfigureAwait(false);

        await _channel.ExchangeDeclareAsync(
            exchange: _topic.Exchange, type: ExchangeType.Topic, durable: true, autoDelete: false,
            cancellationToken: cancellationToken).ConfigureAwait(false);
        await _channel.ExchangeDeclareAsync(
            exchange: _options.DeadLetterExchange, type: ExchangeType.Topic, durable: true, autoDelete: false,
            cancellationToken: cancellationToken).ConfigureAwait(false);

        await _channel.QueueDeclareAsync(
            queue: _queueName, durable: true, exclusive: false, autoDelete: false,
            arguments: new Dictionary<string, object?> { ["x-queue-type"] = "quorum" },
            cancellationToken: cancellationToken).ConfigureAwait(false);
        await _channel.QueueBindAsync(
            queue: _queueName, exchange: _topic.Exchange, routingKey: _topic.RoutingKey,
            cancellationToken: cancellationToken).ConfigureAwait(false);

        await _channel.QueueDeclareAsync(
            queue: _errorQueueName, durable: true, exclusive: false, autoDelete: false,
            arguments: new Dictionary<string, object?> { ["x-queue-type"] = "quorum" },
            cancellationToken: cancellationToken).ConfigureAwait(false);
        await _channel.QueueBindAsync(
            queue: _errorQueueName, exchange: _options.DeadLetterExchange, routingKey: _topic.RoutingKey,
            cancellationToken: cancellationToken).ConfigureAwait(false);

        await _channel.BasicQosAsync(prefetchSize: 0, prefetchCount: _prefetch, global: false, cancellationToken: cancellationToken)
            .ConfigureAwait(false);

        _dlxChannel = await conn.CreateChannelAsync(
            new CreateChannelOptions(publisherConfirmationsEnabled: false, publisherConfirmationTrackingEnabled: false),
            cancellationToken).ConfigureAwait(false);

        var consumer = new AsyncEventingBasicConsumer(_channel);
        consumer.ReceivedAsync += OnReceivedAsync;
        _consumerTag = await _channel.BasicConsumeAsync(
            queue: _queueName,
            autoAck: false,
            consumer: consumer,
            cancellationToken: cancellationToken).ConfigureAwait(false);

        _logger.LogInformation(
            "Consumer {Handler} started on queue {Queue} with prefetch {Prefetch} (consumer-tag {Tag})",
            typeof(TConsumer).FullName, _queueName, _prefetch, _consumerTag);
    }

    private async Task OnReceivedAsync(object sender, BasicDeliverEventArgs ea)
    {
        var channel = _channel ?? throw new InvalidOperationException("Channel not open.");
        var startedMs = Environment.TickCount64;
        _inFlightStartedAtMs[ea.DeliveryTag] = startedMs;

        ActivityContext? parent = null;
        if (ea.BasicProperties?.Headers?.TryGetValue("traceparent", out var tpRaw) == true && tpRaw is byte[] tpBytes)
        {
            var tp = Encoding.UTF8.GetString(tpBytes);
            if (ActivityContext.TryParse(tp, traceState: ExtractHeader(ea.BasicProperties, "tracestate"), out var ctx))
                parent = ctx;
        }
        using var activity = parent is { } p
            ? PubSubDiagnostics.ActivitySource.StartActivity($"pubsub.consume {_queueName}", ActivityKind.Consumer, p)
            : PubSubDiagnostics.ActivitySource.StartActivity($"pubsub.consume {_queueName}", ActivityKind.Consumer);
        activity?.SetTag("messaging.system", "rabbitmq");
        activity?.SetTag("messaging.operation", "receive");
        activity?.SetTag("messaging.destination.name", _queueName);
        activity?.SetTag("messaging.message.body.size", ea.Body.Length);
        if (_consumerTag is not null)
            activity?.SetTag("messaging.consumer.id", _consumerTag);

        try
        {
            T message;
            try
            {
                message = Dispatcher.Deserialize(ea.Body.Span);
            }
            catch (JsonException ex)
            {
                await RouteToDlxAsync(channel, ea, ex, startedMs, ea.CancellationToken).ConfigureAwait(false);
                await channel.BasicNackAsync(ea.DeliveryTag, multiple: false, requeue: false, ea.CancellationToken).ConfigureAwait(false);
                return;
            }

            using var scope = _services.CreateScope();
            var consumer = scope.ServiceProvider.GetRequiredService<TConsumer>();
            await Dispatcher.InvokeAsync(consumer, message, ea.CancellationToken).ConfigureAwait(false);
            PubSubDiagnostics.HandlerMs.Record(Environment.TickCount64 - startedMs, _queueTag);

            await channel.BasicAckAsync(ea.DeliveryTag, multiple: false, ea.CancellationToken).ConfigureAwait(false);
            PubSubDiagnostics.ConsumeCount.Add(1,
                new KeyValuePair<string, object?>("queue", _queueName),
                new KeyValuePair<string, object?>("outcome", "acked"));
        }
        catch (Exception ex)
        {
            PubSubDiagnostics.HandlerMs.Record(Environment.TickCount64 - startedMs, _queueTag);
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            _logger.LogError(ex, "Handler {Handler} failed for delivery {DeliveryTag}; routing to DLX",
                typeof(TConsumer).FullName, ea.DeliveryTag);
            await RouteToDlxAsync(channel, ea, ex, startedMs, ea.CancellationToken).ConfigureAwait(false);
            try
            {
                await channel.BasicNackAsync(ea.DeliveryTag, multiple: false, requeue: false, ea.CancellationToken).ConfigureAwait(false);
            }
            catch (Exception nackEx)
            {
                _logger.LogWarning(nackEx, "BasicNackAsync failed for delivery {DeliveryTag}", ea.DeliveryTag);
            }
            PubSubDiagnostics.ConsumeCount.Add(1,
                new KeyValuePair<string, object?>("queue", _queueName),
                new KeyValuePair<string, object?>("outcome", "nacked-dlq"));
        }
        finally
        {
            _inFlightStartedAtMs.TryRemove(ea.DeliveryTag, out _);
        }
    }

    private async Task RouteToDlxAsync(IChannel _, BasicDeliverEventArgs ea, Exception exception, long startedMs, CancellationToken ct)
    {
        var headers = new Dictionary<string, object?>(ea.BasicProperties?.Headers ?? new Dictionary<string, object?>())
        {
            ["x-exception-type"] = Encoding.UTF8.GetBytes(exception.GetType().FullName ?? exception.GetType().Name),
            ["x-exception"] = Encoding.UTF8.GetBytes(exception.Message ?? string.Empty),
            ["x-stacktrace"] = Encoding.UTF8.GetBytes(exception.ToString()),
            ["x-consumer-handler"] = Encoding.UTF8.GetBytes(typeof(TConsumer).FullName ?? typeof(TConsumer).Name),
            ["x-handler-elapsed-ms"] = Environment.TickCount64 - startedMs,
            ["x-original-routing-key"] = Encoding.UTF8.GetBytes(ea.RoutingKey ?? string.Empty),
            ["x-pod-name"] = Encoding.UTF8.GetBytes(_options.PodName),
        };

        var props = new BasicProperties
        {
            ContentType = ea.BasicProperties?.ContentType ?? "application/json",
            ContentEncoding = ea.BasicProperties?.ContentEncoding ?? "utf-8",
            DeliveryMode = DeliveryModes.Persistent,
            Headers = headers,
        };

        var dlx = _dlxChannel ?? throw new InvalidOperationException("DLX channel not initialized.");
        try
        {
            await dlx.BasicPublishAsync(
                exchange: _options.DeadLetterExchange,
                routingKey: ea.RoutingKey,
                mandatory: false,
                basicProperties: props,
                body: ea.Body,
                cancellationToken: ct).ConfigureAwait(false);
            PubSubDiagnostics.DlqPublishCount.Add(1,
                new KeyValuePair<string, object?>("queue", _queueName),
                new KeyValuePair<string, object?>("error_queue", _errorQueueName));
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to publish exception details to DLX {Dlx}", _options.DeadLetterExchange);
        }
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        if (_channel is not null)
        {
            try
            {
                if (_consumerTag is not null)
                    await _channel.BasicCancelAsync(_consumerTag, cancellationToken: cancellationToken).ConfigureAwait(false);
                await _channel.CloseAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Error closing consumer channel for {Handler}", typeof(TConsumer).FullName);
            }
        }
        if (_dlxChannel is not null)
        {
            try { await _dlxChannel.CloseAsync(cancellationToken).ConfigureAwait(false); }
            catch { }
        }
    }

    public async ValueTask DisposeAsync()
    {
        PubSubDiagnostics.UnregisterTracker(this);
        if (_channel is not null)
        {
            try { await _channel.CloseAsync().ConfigureAwait(false); } catch { }
            _channel.Dispose();
        }
        if (_dlxChannel is not null)
        {
            try { await _dlxChannel.CloseAsync().ConfigureAwait(false); } catch { }
            _dlxChannel.Dispose();
        }
    }

    public IEnumerable<Measurement<long>> Observe()
    {
        if (_consumerTag is null) yield break;
        var now = Environment.TickCount64;
        foreach (var kvp in _inFlightStartedAtMs)
        {
            yield return new Measurement<long>(
                now - kvp.Value,
                new KeyValuePair<string, object?>("queue", _queueName),
                new KeyValuePair<string, object?>("consumer_tag", _consumerTag),
                new KeyValuePair<string, object?>("handler_type", typeof(TConsumer).FullName ?? typeof(TConsumer).Name));
        }
    }

    private static string? ExtractHeader(IReadOnlyBasicProperties props, string name)
    {
        if (props.Headers?.TryGetValue(name, out var raw) == true && raw is byte[] bytes)
            return Encoding.UTF8.GetString(bytes);
        return null;
    }
}
