using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using System.Reflection;

namespace PubSub;

public static class PubSubTopicResolver
{
    private static readonly ConcurrentDictionary<Type, PubSubTopicAttribute> Cache = new();
    private static readonly ConcurrentDictionary<Type, PublishMode> ModeCache = new();
    private static readonly ConcurrentDictionary<Type, BatchedPublishAttribute> BatchedCache = new();

    public static PubSubTopicAttribute Resolve<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] T>()
        => Resolve(typeof(T));

    public static PubSubTopicAttribute Resolve(Type messageType)
    {
        ArgumentNullException.ThrowIfNull(messageType);
        return Cache.GetOrAdd(messageType, static t =>
        {
            var attr = t.GetCustomAttribute<PubSubTopicAttribute>(inherit: false);
            return attr ?? throw new InvalidOperationException(
                $"Type {t.FullName} has no [PubSubTopic] attribute. " +
                $"Add `[PubSubTopic(\"your.routing.key\")]` on the contract record so the transport library can route it.");
        });
    }

    public static TimeSpan ResolvePublishTimeout<T>(int defaultSeconds = 10)
    {
        var attr = typeof(T).GetCustomAttribute<PublishTimeoutAttribute>(inherit: false);
        return TimeSpan.FromSeconds(attr?.Seconds ?? defaultSeconds);
    }

    public static ushort ResolveConsumerPrefetch(Type consumerType, ushort defaultCount = 50)
    {
        ArgumentNullException.ThrowIfNull(consumerType);
        var attr = consumerType.GetCustomAttribute<ConsumerPrefetchAttribute>(inherit: true);
        return attr is null ? defaultCount : checked((ushort)attr.Count);
    }

    public static PublishMode ResolvePublishMode<T>() => ResolvePublishMode(typeof(T));

    public static PublishMode ResolvePublishMode(Type messageType)
    {
        ArgumentNullException.ThrowIfNull(messageType);
        return ModeCache.GetOrAdd(messageType, static t =>
        {
            var modes = 0;
            PublishMode resolved = PublishMode.ConfirmPerMessage;
            if (t.GetCustomAttribute<ConfirmPerMessageAttribute>(inherit: false) is not null) { modes++; resolved = PublishMode.ConfirmPerMessage; }
            if (t.GetCustomAttribute<BatchedPublishAttribute>(inherit: false) is not null) { modes++; resolved = PublishMode.Batched; }
            if (t.GetCustomAttribute<FireAndForgetAttribute>(inherit: false) is not null) { modes++; resolved = PublishMode.FireAndForget; }
            if (modes > 1)
                throw new InvalidOperationException(
                    $"Type {t.FullName} carries more than one of [ConfirmPerMessage], [BatchedPublish], [FireAndForget]. Pick exactly one.");
            return resolved;
        });
    }

    public static BatchedPublishAttribute ResolveBatchedSettings<T>() => ResolveBatchedSettings(typeof(T));

    public static BatchedPublishAttribute ResolveBatchedSettings(Type messageType)
    {
        ArgumentNullException.ThrowIfNull(messageType);
        return BatchedCache.GetOrAdd(messageType, static t =>
        {
            var attr = t.GetCustomAttribute<BatchedPublishAttribute>(inherit: false);
            return attr ?? throw new InvalidOperationException(
                $"Type {t.FullName} has no [BatchedPublish] attribute but was resolved as PublishMode.Batched.");
        });
    }
}
