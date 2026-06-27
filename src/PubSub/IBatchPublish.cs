namespace PubSub;

public interface IBatchPublish<in T>
{
    Task PublishAsync(T message, CancellationToken cancellationToken = default);
}
