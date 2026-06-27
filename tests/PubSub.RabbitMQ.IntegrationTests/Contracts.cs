namespace PubSub.RabbitMQ.IntegrationTests;

[PubSub.PubSubTopic("test.alpha", Exchange = "pubsub.itests")]
public sealed record AlphaMessage(int Id, string Payload);

[PubSub.PubSubTopic("test.beta", Exchange = "pubsub.itests")]
public sealed record BetaMessage(string Symbol, double Price);

[PubSub.PubSubTopic("test.gamma", Exchange = "pubsub.itests")]
public sealed record GammaMessage(string Marker);

[PubSub.PubSubTopic("test.slow", Exchange = "pubsub.itests")]
public sealed record SlowMessage(int Id);

[PubSub.PubSubTopic("test.fail", Exchange = "pubsub.itests")]
public sealed record FailingMessage(string Symbol);

[PubSub.PubSubTopic("test.fail-once", Exchange = "pubsub.itests")]
public sealed record FailOnceMessage(string Symbol);
