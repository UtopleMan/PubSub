namespace PubSub;

[AttributeUsage(AttributeTargets.Class, AllowMultiple = false, Inherited = false)]
public sealed class ConsumerPrefetchAttribute(int count) : Attribute
{
    public int Count { get; } = count > 0
        ? count
        : throw new ArgumentOutOfRangeException(nameof(count), count, "Prefetch must be positive.");
}
