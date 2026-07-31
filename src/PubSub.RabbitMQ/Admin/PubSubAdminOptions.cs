using System.Reflection;

namespace PubSub.RabbitMQ;

/// <summary>
/// Configuration for the PubSub admin/statistics surface (<see cref="PubSub.Admin.IPubSubAdmin"/>)
/// and the in-process metrics sampler that feeds the <c>PubSub.Pulse</c> console.
/// </summary>
public sealed class PubSubAdminOptions
{
    /// <summary>
    /// Human name for this process/service, shown as the "endpoint" column in the console.
    /// Defaults to the entry assembly's simple name.
    /// </summary>
    public string ServiceName { get; set; } =
        Assembly.GetEntryAssembly()?.GetName().Name ?? "unknown";

    /// <summary>
    /// In-flight age (ms) at or above which a consumer is treated as wedged (critical). The
    /// console draws its alert threshold here; a third of it is the warning line. Default 90s.
    /// </summary>
    public long WedgeThresholdMs { get; set; } = 90_000;

    /// <summary>How often the metrics sampler recomputes rates/p95/in-flight. Default 3s.</summary>
    public TimeSpan SampleInterval { get; set; } = TimeSpan.FromSeconds(3);

    /// <summary>How many sample points to retain per queue for the sparkline series. Default 26.</summary>
    public int SeriesLength { get; set; } = 26;

    /// <summary>
    /// Max messages to peek per <c>.error</c> queue when listing failed messages, to bound the
    /// cost of a refresh against a very deep error queue. Default 100.
    /// </summary>
    public int FailedPeekPerQueue { get; set; } = 100;
}
