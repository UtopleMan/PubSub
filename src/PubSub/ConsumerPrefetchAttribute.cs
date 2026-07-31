namespace PubSub;

/// <summary>Sets the broker prefetch (max unacknowledged messages in flight) for a consumer. Applied to the handler type.</summary>
[AttributeUsage(AttributeTargets.Class, AllowMultiple = false, Inherited = false)]
public sealed class ConsumerPrefetchAttribute(int count) : Attribute
{
    /// <summary>Maximum messages delivered to the consumer before earlier ones are acknowledged.</summary>
    public int Count { get; } = count > 0
        ? count
        : throw new ArgumentOutOfRangeException(nameof(count), count, "Prefetch must be positive.");
}
