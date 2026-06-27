using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace PubSub.RabbitMQ.IntegrationTests;

internal static class HostBuilder
{
    public static IHost Build(string connectionString, Action<PubSubRabbitMqBuilder> configure, Action<IServiceCollection>? extra = null)
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Logging.SetMinimumLevel(LogLevel.Warning);
        builder.Services.AddSingleton<MessageRecorder>();
        extra?.Invoke(builder.Services);
        builder.Services.AddPubSubRabbitMq(new PubSubRabbitMqOptions
        {
            ConnectionString = connectionString,
            DefaultPublishTimeout = TimeSpan.FromSeconds(10),
            DefaultConsumerPrefetch = 50,
            PodName = "pubsub-itest",
        }, configure);
        return builder.Build();
    }
}
