namespace PubSub;

/// <summary>Implement on a message contract to compute a per-message routing key instead of the static <see cref="PubSubTopicAttribute.RoutingKey"/>.</summary>
public interface IRoutingKeyProvider
{
    /// <summary>Returns the routing key this message instance should be published under.</summary>
    string GetRoutingKey();
}
