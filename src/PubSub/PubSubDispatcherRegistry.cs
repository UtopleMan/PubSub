using System.Collections.Concurrent;

namespace PubSub;

/// <summary>Process-wide map from a message contract to its <see cref="IPubSubDispatcher{T}"/>. The source generator registers generated dispatchers at startup; unregistered contracts fall back to reflection.</summary>
public static class PubSubDispatcherRegistry
{
    private static readonly ConcurrentDictionary<Type, object> Dispatchers = new();

    /// <summary>Register the dispatcher for contract <typeparamref name="T"/> (called by generated startup code).</summary>
    public static void Register<T>(IPubSubDispatcher<T> dispatcher)
    {
        ArgumentNullException.ThrowIfNull(dispatcher);
        Dispatchers[typeof(T)] = dispatcher;
    }

    /// <summary>The registered dispatcher for <typeparamref name="T"/>, or a reflection-based fallback.</summary>
    public static IPubSubDispatcher<T> GetOrFallback<T>()
    {
        if (Dispatchers.TryGetValue(typeof(T), out var d))
            return (IPubSubDispatcher<T>)d;
        return ReflectionFallback<T>.Instance;
    }

    /// <summary>True if a generated dispatcher was registered for <typeparamref name="T"/>.</summary>
    public static bool HasGeneratedDispatcher<T>() => Dispatchers.ContainsKey(typeof(T));

    private static class ReflectionFallback<T>
    {
        public static readonly ReflectionPubSubDispatcher<T> Instance = new();
    }
}
