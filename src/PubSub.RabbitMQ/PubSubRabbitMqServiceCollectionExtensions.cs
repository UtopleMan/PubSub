using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using RabbitMQ.Client;

namespace PubSub.RabbitMQ;

public static class PubSubRabbitMqServiceCollectionExtensions
{
    public static IServiceCollection AddPubSubRabbitMq(
        this IServiceCollection services,
        PubSubRabbitMqOptions options,
        Action<PubSubRabbitMqBuilder> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(configure);

        services.TryAddSingleton(options);
        services.TryAddSingleton<PubSubConnectionProvider>();
        services.TryAddSingleton(sp => sp.GetRequiredService<PubSubConnectionProvider>());

        services.TryAddSingleton<IConnection>(sp =>
            sp.GetRequiredService<PubSubConnectionProvider>()
              .GetOrOpenAsync(CancellationToken.None).AsTask().GetAwaiter().GetResult());

        var builder = new PubSubRabbitMqBuilder(services);
        configure(builder);

        services.AddSingleton(new PubSubRegistrationSnapshot(
            builder.Publishers.ToArray(),
            builder.Consumers.ToArray()));
        services.AddHostedService<PubSubRabbitMqHostedService>();

        return services;
    }
}

internal sealed record PubSubRegistrationSnapshot(
    IReadOnlyList<PubSubPublisherRegistration> Publishers,
    IReadOnlyList<PubSubConsumerRegistration> Consumers);
