namespace PubSub.Pulse;

/// <summary>Options for the embedded PubSub Pulse console.</summary>
public sealed class PubSubPulseOptions
{
    /// <summary>Browser tab title / header wordmark. Default "PubSub Pulse".</summary>
    public string Title { get; set; } = "PubSub Pulse";

    /// <summary>How often (ms) the console polls the REST API. Default 3000.</summary>
    public int PollIntervalMs { get; set; } = 3000;

    /// <summary>
    /// In-flight age (ms) the console treats as wedged (drives the gauge's alert line). Should
    /// match <c>PubSubAdminOptions.WedgeThresholdMs</c> on the admin side. Default 90000.
    /// </summary>
    public long WedgeThresholdMs { get; set; } = 90_000;
}
