using System.Collections.Concurrent;

namespace PubSub;

public static class PubSubDispatcherRegistry
{
    private static readonly ConcurrentDictionary<Type, object> Dispatchers = new();

    public static void Register<T>(IPubSubDispatcher<T> dispatcher)
    {
        ArgumentNullException.ThrowIfNull(dispatcher);
        Dispatchers[typeof(T)] = dispatcher;
    }

    public static IPubSubDispatcher<T> GetOrFallback<T>()
    {
        if (Dispatchers.TryGetValue(typeof(T), out var d))
            return (IPubSubDispatcher<T>)d;
        return ReflectionFallback<T>.Instance;
    }

    public static bool HasGeneratedDispatcher<T>() => Dispatchers.ContainsKey(typeof(T));

    private static class ReflectionFallback<T>
    {
        public static readonly ReflectionPubSubDispatcher<T> Instance = new();
    }
}
