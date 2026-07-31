namespace PubSub;

/// <summary>Publishes messages without waiting for a broker confirm (see <see cref="FireAndForgetAttribute"/>).</summary>
public interface IFireAndForgetPublish<in T>
{
    /// <summary>Publish <paramref name="message"/> and return without awaiting any delivery confirm.</summary>
    Task PublishAsync(T message, CancellationToken cancellationToken = default);
}
