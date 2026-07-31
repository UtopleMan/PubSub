namespace PubSub;

/// <summary>Thrown when a confirmed publish is not acknowledged by the broker within its timeout.</summary>
public sealed class PubSubPublishTimeoutException(
    string topic,
    TimeSpan timeout,
    TimeSpan elapsed,
    Exception? innerException = null)
    : PubSubException(
        $"Publish to topic '{topic}' did not confirm within {timeout.TotalMilliseconds:F0} ms (elapsed {elapsed.TotalMilliseconds:F0} ms).",
        innerException)
{
    /// <summary>Routing key/topic that was being published.</summary>
    public string Topic { get; } = topic;

    /// <summary>Configured confirm timeout.</summary>
    public TimeSpan Timeout { get; } = timeout;

    /// <summary>Time actually waited before giving up.</summary>
    public TimeSpan Elapsed { get; } = elapsed;
}
