namespace PubSub;

[AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct, AllowMultiple = false, Inherited = false)]
public sealed class BatchedPublishAttribute(int batchSize) : Attribute
{
    public int BatchSize { get; } = batchSize > 0
        ? batchSize
        : throw new ArgumentOutOfRangeException(nameof(batchSize), batchSize, "Batch size must be positive.");

    public int FlushIntervalMs { get; init; } = 50;
}
