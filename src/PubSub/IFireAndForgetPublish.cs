namespace PubSub;

public interface IFireAndForgetPublish<in T>
{
    Task PublishAsync(T message, CancellationToken cancellationToken = default);
}
