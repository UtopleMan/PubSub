using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;

namespace PubSub.RabbitMQ;

internal sealed class PubSubRabbitMqHostedService(
    PubSubConnectionProvider connectionProvider,
    PubSubRabbitMqOptions options,
    PubSubRegistrationSnapshot registrations,
    ILogger<PubSubRabbitMqHostedService> logger)
    : IHostedService
{
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        var conn = await connectionProvider.GetOrOpenAsync(cancellationToken).ConfigureAwait(false);
        await using var channel = await conn.CreateChannelAsync(
            new CreateChannelOptions(publisherConfirmationsEnabled: false, publisherConfirmationTrackingEnabled: false),
            cancellationToken).ConfigureAwait(false);

        var exchanges = registrations.Publishers.Select(p => p.Topic.Exchange)
            .Concat(registrations.Consumers.Select(c => c.Topic.Exchange))
            .Distinct(StringComparer.Ordinal)
            .ToArray();
        foreach (var ex in exchanges)
        {
            await channel.ExchangeDeclareAsync(
                exchange: ex,
                type: ExchangeType.Topic,
                durable: true,
                autoDelete: false,
                cancellationToken: cancellationToken).ConfigureAwait(false);
            logger.LogInformation("Declared exchange {Exchange} (topic, durable)", ex);
        }

        await channel.ExchangeDeclareAsync(
            exchange: options.DeadLetterExchange,
            type: ExchangeType.Topic,
            durable: true,
            autoDelete: false,
            cancellationToken: cancellationToken).ConfigureAwait(false);
        logger.LogInformation("Declared DLX {Dlx}", options.DeadLetterExchange);

        foreach (var c in registrations.Consumers)
        {
            var queue = c.Topic.Queue ?? $"{c.Topic.Exchange}.{c.Topic.RoutingKey}";
            var errorQueue = $"{queue}.error";
            await channel.QueueDeclareAsync(
                queue: queue, durable: true, exclusive: false, autoDelete: false,
                arguments: new Dictionary<string, object?> { ["x-queue-type"] = "quorum" },
                cancellationToken: cancellationToken).ConfigureAwait(false);
            await channel.QueueBindAsync(
                queue: queue, exchange: c.Topic.Exchange, routingKey: c.Topic.RoutingKey,
                cancellationToken: cancellationToken).ConfigureAwait(false);
            await channel.QueueDeclareAsync(
                queue: errorQueue, durable: true, exclusive: false, autoDelete: false,
                arguments: new Dictionary<string, object?> { ["x-queue-type"] = "quorum" },
                cancellationToken: cancellationToken).ConfigureAwait(false);
            await channel.QueueBindAsync(
                queue: errorQueue, exchange: options.DeadLetterExchange, routingKey: c.Topic.RoutingKey,
                cancellationToken: cancellationToken).ConfigureAwait(false);
            logger.LogInformation(
                "Declared consumer queue {Queue} (+ {ErrorQueue}) bound to {Exchange}/{RoutingKey} for handler {Handler}",
                queue, errorQueue, c.Topic.Exchange, c.Topic.RoutingKey, c.ConsumerType.FullName);
        }

        await channel.CloseAsync(cancellationToken).ConfigureAwait(false);
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
