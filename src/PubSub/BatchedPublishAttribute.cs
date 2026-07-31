namespace PubSub;

/// <summary>Opts a contract into <see cref="PublishMode.Batched"/> publishing with the given batch size.</summary>
[AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct, AllowMultiple = false, Inherited = false)]
public sealed class BatchedPublishAttribute(int batchSize) : Attribute
{
    /// <summary>Number of messages accumulated before a batch is flushed and confirmed.</summary>
    public int BatchSize { get; } = batchSize > 0
        ? batchSize
        : throw new ArgumentOutOfRangeException(nameof(batchSize), batchSize, "Batch size must be positive.");

    /// <summary>Longest a partial batch waits before being flushed. Defaults to 50 ms.</summary>
    public int FlushIntervalMs { get; init; } = 50;
}
