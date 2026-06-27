namespace PubSub;

public interface IPubSubDispatcher<T>
{
    byte[] Serialize(T message);

    T Deserialize(ReadOnlySpan<byte> body);

    Task InvokeAsync(ISubscribeTo<T> handler, T message, CancellationToken cancellationToken);
}
