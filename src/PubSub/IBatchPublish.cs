namespace PubSub;

/// <summary>Publishes messages that are confirmed in batches (see <see cref="BatchedPublishAttribute"/>).</summary>
public interface IBatchPublish<in T>
{
    /// <summary>Enqueue <paramref name="message"/> for the next batch; completes when that batch is confirmed.</summary>
    Task PublishAsync(T message, CancellationToken cancellationToken = default);
}
