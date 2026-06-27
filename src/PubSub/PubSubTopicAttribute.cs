namespace PubSub;

[AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct, AllowMultiple = false, Inherited = false)]
public sealed class PubSubTopicAttribute(string routingKey) : Attribute
{
    public string RoutingKey { get; } = routingKey ?? throw new ArgumentNullException(nameof(routingKey));

    public string Exchange { get; init; } = "phoenix.events";

    public string? Queue { get; init; }
}
