using System.Text.Json;

namespace PubSub;

/// <summary>Reflection/System.Text.Json fallback <see cref="IPubSubDispatcher{T}"/>, used when no generated dispatcher is registered for <typeparamref name="T"/>.</summary>
public sealed class ReflectionPubSubDispatcher<T> : IPubSubDispatcher<T>
{
    /// <inheritdoc/>
    public byte[] Serialize(T message) => JsonSerializer.SerializeToUtf8Bytes(message);

    /// <inheritdoc/>
    public T Deserialize(ReadOnlySpan<byte> body)
        => JsonSerializer.Deserialize<T>(body)
           ?? throw new InvalidOperationException($"Deserialized message of type {typeof(T).FullName} was null.");

    /// <inheritdoc/>
    public Task InvokeAsync(ISubscribeTo<T> handler, T message, CancellationToken cancellationToken)
        => handler.Handle(message, cancellationToken);
}
