namespace PubSub;

[AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct, AllowMultiple = false, Inherited = false)]
public sealed class PublishTimeoutAttribute(int seconds) : Attribute
{
    public int Seconds { get; } = seconds > 0
        ? seconds
        : throw new ArgumentOutOfRangeException(nameof(seconds), seconds, "Publish timeout must be positive.");
}
