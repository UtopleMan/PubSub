namespace PubSub.RabbitMQ;

/// <summary>
/// Resolves the queue name a consumer binds to, keeping the consumer host and the
/// topology-declaration hosted service in perfect agreement (they MUST derive the same
/// name or a consumer would listen on a queue nobody declared/bound).
/// </summary>
internal static class PubSubQueueNaming
{
    /// <summary>
    /// An explicit <see cref="PubSubTopicAttribute.Queue"/> (set via
    /// <c>Subscribe&lt;T, TConsumer&gt;(queueName)</c>) is honoured verbatim — pass one when you
    /// deliberately want several handler types to COMPETE on a single shared queue.
    /// <para>
    /// Otherwise the default is per-(topic × consumer type):
    /// <c>{Exchange}.{RoutingKey}.{ConsumerName}</c>. This is the correct pub/sub default:
    /// distinct handler types subscribing to the same event each get their own queue and thus
    /// their own copy of every message (fan-out), while replicas of the SAME consumer type share
    /// one queue and load-balance as competing consumers.
    /// </para>
    /// <para>
    /// The suffix is the consumer's fully-qualified type name (namespace-qualified), so two
    /// consumer types can never collide back onto one shared queue — even if they share a simple
    /// name. If you deliberately want handler types to compete, give them the same explicit queueName.
    /// </para>
    /// </summary>
    public static string ResolveQueueName(PubSubTopicAttribute topic, Type consumerType)
        => topic.Queue ?? $"{topic.Exchange}.{topic.RoutingKey}.{consumerType.FullName ?? consumerType.Name}";
}
