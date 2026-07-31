namespace PubSub;

/// <summary>Overrides how long a confirmed publish waits for the broker before timing out.</summary>
[AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct, AllowMultiple = false, Inherited = false)]
public sealed class PublishTimeoutAttribute(int seconds) : Attribute
{
    /// <summary>Timeout in seconds; a publish that doesn't confirm in time throws <see cref="PubSubPublishTimeoutException"/>.</summary>
    public int Seconds { get; } = seconds > 0
        ? seconds
        : throw new ArgumentOutOfRangeException(nameof(seconds), seconds, "Publish timeout must be positive.");
}
