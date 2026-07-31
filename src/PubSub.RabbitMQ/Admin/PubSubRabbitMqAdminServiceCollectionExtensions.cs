using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using PubSub.Admin;

namespace PubSub.RabbitMQ;

/// <summary>
/// Registers the RabbitMQ implementation of <see cref="IPubSubAdmin"/> plus the in-process
/// metrics sampler that feeds the <c>PubSub.Pulse</c> console. Call after
/// <see cref="PubSubRabbitMqServiceCollectionExtensions.AddPubSubRabbitMq"/>.
/// </summary>
public static class PubSubRabbitMqAdminServiceCollectionExtensions
{
    /// <summary>
    /// Adds <see cref="IPubSubAdmin"/> (RabbitMQ), the metrics sampler (as a hosted service), and
    /// <see cref="PubSubAdminOptions"/>. Idempotent; safe to call once alongside your PubSub setup.
    /// </summary>
    public static IServiceCollection AddPubSubRabbitMqAdmin(
        this IServiceCollection services,
        Action<PubSubAdminOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);

        var options = new PubSubAdminOptions();
        configure?.Invoke(options);
        services.TryAddSingleton(options);

        services.TryAddSingleton<PubSubMetricsSampler>();
        services.AddHostedService(sp => sp.GetRequiredService<PubSubMetricsSampler>());
        services.TryAddSingleton<IPubSubAdmin, RabbitMqPubSubAdmin>();

        return services;
    }
}
