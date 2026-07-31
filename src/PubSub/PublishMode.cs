namespace PubSub;

/// <summary>How a publisher waits (or not) for the broker to acknowledge a message.</summary>
public enum PublishMode
{
    /// <summary>Await a broker confirm for every message before the publish completes. Safest, slowest.</summary>
    ConfirmPerMessage,

    /// <summary>Buffer messages and confirm them in batches — higher throughput at some added latency.</summary>
    Batched,

    /// <summary>Publish without awaiting any confirm. Highest throughput, no delivery guarantee.</summary>
    FireAndForget,
}
