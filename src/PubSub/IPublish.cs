namespace PubSub;

public interface IPublish<in T>
{
    Task PublishAsync(T message, CancellationToken cancellationToken = default);
}
