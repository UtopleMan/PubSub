namespace PubSub;

/// <summary>
/// Sets how many messages a consumer processes concurrently (the broker's consumer-dispatch
/// concurrency). Applied to the handler type. Defaults to 1 (serial) when absent, preserving
/// in-order, one-at-a-time handling. Raise it only for handlers whose work is independent per
/// message and safe to run in parallel (e.g. an idempotent per-symbol crawl behind a browser pool).
/// </summary>
[AttributeUsage(AttributeTargets.Class, AllowMultiple = false, Inherited = false)]
public sealed class ConsumerConcurrencyAttribute(int count) : Attribute
{
    /// <summary>Maximum handler invocations running concurrently for this consumer.</summary>
    public int Count { get; } = count > 0
        ? count
        : throw new ArgumentOutOfRangeException(nameof(count), count, "Concurrency must be positive.");
}
