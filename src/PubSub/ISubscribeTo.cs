namespace PubSub;

/// <summary>Implement to consume messages of contract <typeparamref name="T"/>. Registered handlers are dispatched one message at a time.</summary>
public interface ISubscribeTo<in T>
{
    /// <summary>Handle a delivered <paramref name="message"/>. Throwing dead-letters the message to its error queue.</summary>
    Task Handle(T message, CancellationToken cancellationToken);
}
