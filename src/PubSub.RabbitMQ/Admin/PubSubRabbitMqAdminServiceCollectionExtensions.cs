using System.Net.Http.Headers;
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using PubSub.Admin;

namespace PubSub.RabbitMQ.Admin;

/// <summary>
/// Registers the broker-scraping RabbitMQ implementation of <see cref="IPubSubAdmin"/>, which reads
/// the RabbitMQ Management HTTP API. Requires no consumer registration and does not depend on
/// <c>AddPubSubRabbitMq</c>, so <c>PubSub.Pulse</c> can run as a standalone ops binary pointed at a
/// broker.
/// </summary>
public static class PubSubRabbitMqAdminServiceCollectionExtensions
{
    /// <summary>
    /// Adds <see cref="IPubSubAdmin"/> (Management-API backed), the typed management
    /// <see cref="HttpClient"/>, and <see cref="ErrorQueueOperations"/>. The management endpoint,
    /// credentials, vhost, and AMQP connection string are all required and configured explicitly.
    /// </summary>
    public static IServiceCollection AddPubSubRabbitMqAdmin(
        this IServiceCollection services,
        Action<PubSubAdminOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);

        var options = new PubSubAdminOptions();
        configure?.Invoke(options);
        Validate(options);
        services.TryAddSingleton(options);

        services.AddHttpClient<IRabbitMqManagementClient, RabbitMqManagementClient>(http =>
        {
            http.BaseAddress = options.ManagementBaseUrl;
            var raw = $"{options.ManagementUser}:{options.ManagementPassword}";
            http.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue(
                "Basic", Convert.ToBase64String(Encoding.UTF8.GetBytes(raw)));
        });

        services.TryAddSingleton<ErrorQueueOperations>();
        services.TryAddSingleton<IPubSubAdmin, ManagementRabbitMqPubSubAdmin>();

        return services;
    }

    private static void Validate(PubSubAdminOptions o)
    {
        if (o.ManagementBaseUrl is null)
            throw new InvalidOperationException("PubSubAdminOptions.ManagementBaseUrl is required.");
        if (string.IsNullOrWhiteSpace(o.ManagementUser))
            throw new InvalidOperationException("PubSubAdminOptions.ManagementUser is required.");
        if (string.IsNullOrWhiteSpace(o.ManagementPassword))
            throw new InvalidOperationException("PubSubAdminOptions.ManagementPassword is required.");
        if (string.IsNullOrWhiteSpace(o.VHost))
            throw new InvalidOperationException("PubSubAdminOptions.VHost is required.");
        if (string.IsNullOrWhiteSpace(o.ConnectionString))
            throw new InvalidOperationException("PubSubAdminOptions.ConnectionString is required.");
    }
}
