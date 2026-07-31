namespace PubSub;

/// <summary>Publishes messages of contract <typeparamref name="T"/> using that contract's configured publish mode.</summary>
public interface IPublish<in T>
{
    /// <summary>Publish <paramref name="message"/>, completing once the contract's delivery guarantee is met.</summary>
    Task PublishAsync(T message, CancellationToken cancellationToken = default);
}
