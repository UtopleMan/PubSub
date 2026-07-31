namespace PubSub;

/// <summary>Thrown when a subscriber's <see cref="ISubscribeTo{T}.Handle"/> throws; wraps the handler's exception. The offending message is dead-lettered to its error queue.</summary>
public sealed class PubSubHandlerFailedException(
    string topic,
    Type handlerType,
    Exception innerException)
    : PubSubException(
        $"Handler {handlerType.FullName} failed processing topic '{topic}': {innerException.Message}",
        innerException)
{
    /// <summary>Routing key/topic of the message being handled.</summary>
    public string Topic { get; } = topic;

    /// <summary>The <see cref="ISubscribeTo{T}"/> implementation that threw.</summary>
    public Type HandlerType { get; } = handlerType;
}
