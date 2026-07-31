namespace PubSub;

/// <summary>Binds a message contract to the exchange and routing key the transport routes it by. Required on every published or subscribed contract.</summary>
[AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct, AllowMultiple = false, Inherited = false)]
public sealed class PubSubTopicAttribute(string routingKey) : Attribute
{
    /// <summary>Routing key (topic) messages of this contract are published under.</summary>
    public string RoutingKey { get; } = routingKey ?? throw new ArgumentNullException(nameof(routingKey));

    /// <summary>Exchange the contract binds to. Defaults to <c>phoenix.events</c>.</summary>
    public string Exchange { get; init; } = "phoenix.events";

    /// <summary>Explicit queue name for subscribers; when null the transport derives one from the handler.</summary>
    public string? Queue { get; init; }
}
