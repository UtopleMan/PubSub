namespace PubSub;

public sealed class PubSubPublishTimeoutException(
    string topic,
    TimeSpan timeout,
    TimeSpan elapsed,
    Exception? innerException = null)
    : PubSubException(
        $"Publish to topic '{topic}' did not confirm within {timeout.TotalMilliseconds:F0} ms (elapsed {elapsed.TotalMilliseconds:F0} ms).",
        innerException)
{
    public string Topic { get; } = topic;
    public TimeSpan Timeout { get; } = timeout;
    public TimeSpan Elapsed { get; } = elapsed;
}
