namespace PubSub.RabbitMQ;

public sealed class PubSubRabbitMqOptions
{
    public string ConnectionString { get; set; } = string.Empty;

    public TimeSpan DefaultPublishTimeout { get; set; } = TimeSpan.FromSeconds(10);

    public ushort DefaultConsumerPrefetch { get; set; } = 50;

    public string DeadLetterExchange { get; set; } = "phoenix.dlx";

    public string PodName { get; set; } = Environment.GetEnvironmentVariable("HOSTNAME") ?? "unknown";
}
