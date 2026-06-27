using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace PubSub.RabbitMQ;

public sealed class PubSubRabbitMqBuilder
{
    private readonly IServiceCollection _services;
    private readonly List<PubSubPublisherRegistration> _publishers = new();
    private readonly List<PubSubConsumerRegistration> _consumers = new();

    internal PubSubRabbitMqBuilder(IServiceCollection services) => _services = services;

    public IReadOnlyList<PubSubPublisherRegistration> Publishers => _publishers;
    public IReadOnlyList<PubSubConsumerRegistration> Consumers => _consumers;

    public PubSubRabbitMqBuilder Publish<T>() where T : class
    {
        var topic = PubSubTopicResolver.Resolve<T>();
        var mode = PubSubTopicResolver.ResolvePublishMode<T>();
        _publishers.Add(new PubSubPublisherRegistration(typeof(T), topic, mode));
        switch (mode)
        {
            case PublishMode.ConfirmPerMessage:
                _services.TryAddSingleton<IPublish<T>>(sp => new RabbitMqPublisher<T>(
                    sp.GetRequiredService<PubSubConnectionProvider>(),
                    topic,
                    PubSubTopicResolver.ResolvePublishTimeout<T>(),
                    sp.GetRequiredService<ILogger<RabbitMqPublisher<T>>>()));
                break;
            case PublishMode.Batched:
                var batched = PubSubTopicResolver.ResolveBatchedSettings<T>();
                _services.TryAddSingleton<IBatchPublish<T>>(sp => new RabbitMqBatchPublisher<T>(
                    sp.GetRequiredService<PubSubConnectionProvider>(),
                    topic,
                    batched,
                    sp.GetRequiredService<ILogger<RabbitMqBatchPublisher<T>>>()));
                break;
            case PublishMode.FireAndForget:
                _services.TryAddSingleton<IFireAndForgetPublish<T>>(sp => new RabbitMqFireAndForgetPublisher<T>(
                    sp.GetRequiredService<PubSubConnectionProvider>(),
                    topic,
                    sp.GetRequiredService<ILogger<RabbitMqFireAndForgetPublisher<T>>>()));
                break;
        }
        return this;
    }

    public PubSubRabbitMqBuilder Subscribe<T, TConsumer>(string? queueName = null)
        where T : class
        where TConsumer : class, ISubscribeTo<T>
    {
        var topic = PubSubTopicResolver.Resolve<T>();
        if (queueName is not null)
        {
            topic = new PubSubTopicAttribute(topic.RoutingKey)
            {
                Exchange = topic.Exchange,
                Queue = queueName,
            };
        }
        var prefetch = PubSubTopicResolver.ResolveConsumerPrefetch(typeof(TConsumer));
        _consumers.Add(new PubSubConsumerRegistration(typeof(T), typeof(TConsumer), topic, prefetch));
        _services.TryAddScoped<TConsumer>();
        _services.AddSingleton<IHostedService>(sp => new RabbitMqConsumerHost<T, TConsumer>(
            sp.GetRequiredService<PubSubConnectionProvider>(),
            sp.GetRequiredService<PubSubRabbitMqOptions>(),
            sp,
            topic,
            prefetch,
            sp.GetRequiredService<ILogger<RabbitMqConsumerHost<T, TConsumer>>>()));
        return this;
    }
}

public sealed record PubSubPublisherRegistration(Type MessageType, PubSubTopicAttribute Topic, PublishMode Mode);

public sealed record PubSubConsumerRegistration(
    Type MessageType,
    Type ConsumerType,
    PubSubTopicAttribute Topic,
    ushort Prefetch);
