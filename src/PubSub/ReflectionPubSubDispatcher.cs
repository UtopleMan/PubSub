using System.Text.Json;

namespace PubSub;

public sealed class ReflectionPubSubDispatcher<T> : IPubSubDispatcher<T>
{
    public byte[] Serialize(T message) => JsonSerializer.SerializeToUtf8Bytes(message);

    public T Deserialize(ReadOnlySpan<byte> body)
        => JsonSerializer.Deserialize<T>(body)
           ?? throw new InvalidOperationException($"Deserialized message of type {typeof(T).FullName} was null.");

    public Task InvokeAsync(ISubscribeTo<T> handler, T message, CancellationToken cancellationToken)
        => handler.Handle(message, cancellationToken);
}
