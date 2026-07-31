using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using System.Reflection;

namespace PubSub;

/// <summary>Reads and caches the PubSub attributes off a message contract: topic/exchange, publish mode, batch settings, publish timeout, and consumer prefetch.</summary>
public static class PubSubTopicResolver
{
    private static readonly ConcurrentDictionary<Type, PubSubTopicAttribute> Cache = new();
    private static readonly ConcurrentDictionary<Type, PublishMode> ModeCache = new();
    private static readonly ConcurrentDictionary<Type, BatchedPublishAttribute> BatchedCache = new();

    /// <summary>The contract's <see cref="PubSubTopicAttribute"/>; throws if the type is not annotated.</summary>
    public static PubSubTopicAttribute Resolve<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] T>()
        => Resolve(typeof(T));

    /// <summary>The contract's <see cref="PubSubTopicAttribute"/>; throws if the type is not annotated.</summary>
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

    /// <summary>The contract's <see cref="PublishTimeoutAttribute"/> as a <see cref="TimeSpan"/>, or <paramref name="defaultSeconds"/> when unset.</summary>
    public static TimeSpan ResolvePublishTimeout<T>(int defaultSeconds = 10)
    {
        var attr = typeof(T).GetCustomAttribute<PublishTimeoutAttribute>(inherit: false);
        return TimeSpan.FromSeconds(attr?.Seconds ?? defaultSeconds);
    }

    /// <summary>The consumer's <see cref="ConsumerPrefetchAttribute"/> count, or <paramref name="defaultCount"/> when unset.</summary>
    public static ushort ResolveConsumerPrefetch(Type consumerType, ushort defaultCount = 50)
    {
        ArgumentNullException.ThrowIfNull(consumerType);
        var attr = consumerType.GetCustomAttribute<ConsumerPrefetchAttribute>(inherit: true);
        return attr is null ? defaultCount : checked((ushort)attr.Count);
    }

    /// <summary>The single <see cref="PublishMode"/> selected by the contract's attributes (defaults to <see cref="PublishMode.ConfirmPerMessage"/>); throws if more than one is present.</summary>
    public static PublishMode ResolvePublishMode<T>() => ResolvePublishMode(typeof(T));

    /// <summary>The single <see cref="PublishMode"/> selected by the contract's attributes (defaults to <see cref="PublishMode.ConfirmPerMessage"/>); throws if more than one is present.</summary>
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

    /// <summary>The contract's <see cref="BatchedPublishAttribute"/>; throws if it is not a <see cref="PublishMode.Batched"/> contract.</summary>
    public static BatchedPublishAttribute ResolveBatchedSettings<T>() => ResolveBatchedSettings(typeof(T));

    /// <summary>The contract's <see cref="BatchedPublishAttribute"/>; throws if it is not a <see cref="PublishMode.Batched"/> contract.</summary>
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
