namespace PubSub;

public sealed class PubSubHandlerFailedException(
    string topic,
    Type handlerType,
    Exception innerException)
    : PubSubException(
        $"Handler {handlerType.FullName} failed processing topic '{topic}': {innerException.Message}",
        innerException)
{
    public string Topic { get; } = topic;
    public Type HandlerType { get; } = handlerType;
}
