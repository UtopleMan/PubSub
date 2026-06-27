namespace PubSub;

public interface IRoutingKeyProvider
{
    string GetRoutingKey();
}
