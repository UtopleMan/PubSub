namespace PubSub.RabbitMQ.Bench;

[PubSub.PubSubTopic("bench.publish", Exchange = "pubsub.bench")]
public sealed record BenchPayload(
    Guid Id,
    string Symbol,
    DateTime TimestampUtc,
    double Price,
    int Volume,
    string Note);
