namespace PubSub;

/// <summary>
/// Serializes, deserializes, and invokes handlers for a single message contract <typeparamref name="T"/>.
/// The source generator emits a fast implementation per contract; <see cref="ReflectionPubSubDispatcher{T}"/>
/// is the reflection-based fallback.
/// </summary>
public interface IPubSubDispatcher<T>
{
    /// <summary>Serialize <paramref name="message"/> to its wire body.</summary>
    byte[] Serialize(T message);

    /// <summary>Deserialize a wire <paramref name="body"/> back into a <typeparamref name="T"/>.</summary>
    T Deserialize(ReadOnlySpan<byte> body);

    /// <summary>Invoke <paramref name="handler"/> for a deserialized <paramref name="message"/>.</summary>
    Task InvokeAsync(ISubscribeTo<T> handler, T message, CancellationToken cancellationToken);
}
