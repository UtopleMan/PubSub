using System.Reflection;

namespace PubSub.RabbitMQ.Admin;

/// <summary>
/// Configuration for the broker-scraping PubSub admin/statistics surface
/// (<see cref="PubSub.Admin.IPubSubAdmin"/>) that feeds the <c>PubSub.Pulse</c> console.
/// <para>
/// The management endpoint and its credentials are configured explicitly and are never inferred
/// from the AMQP <see cref="ConnectionString"/>. Everything the console shows (depth, rates,
/// topology) comes from the RabbitMQ Management HTTP API; the AMQP connection is used only for the
/// failed-message error-queue operations (peek/replay/delete).
/// </para>
/// </summary>
public sealed class PubSubAdminOptions
{
    /// <summary>
    /// Base URL of the RabbitMQ Management HTTP API, e.g. <c>http://rabbit:15672</c>. Required.
    /// </summary>
    public Uri? ManagementBaseUrl { get; set; }

    /// <summary>Username for HTTP basic auth against the Management API. Required.</summary>
    public string ManagementUser { get; set; } = string.Empty;

    /// <summary>Password for HTTP basic auth against the Management API. Required.</summary>
    public string ManagementPassword { get; set; } = string.Empty;

    /// <summary>
    /// Virtual host the console scopes to, e.g. <c>/</c>. Required. A single Pulse instance shows
    /// one vhost.
    /// </summary>
    public string VHost { get; set; } = "/";

    /// <summary>
    /// AMQP connection string. Required. Used only for the failed-message error-queue operations
    /// (peek/replay/delete); the admin builds its own lightweight connection from it, independent
    /// of the library's <c>PubSubConnectionProvider</c>, so Pulse can run as a standalone binary.
    /// </summary>
    public string ConnectionString { get; set; } = string.Empty;

    /// <summary>
    /// Human name for this process/service, shown as the "endpoint" column in the console.
    /// Defaults to the entry assembly's simple name.
    /// </summary>
    public string ServiceName { get; set; } =
        Assembly.GetEntryAssembly()?.GetName().Name ?? "unknown";

    /// <summary>
    /// In-flight age (ms) UI threshold, forwarded to the console via <c>PubSubPulseOptions</c>.
    /// <para>
    /// <b>UI-only.</b> Since the broker cannot report handler in-flight age, in-flight is always
    /// <c>0</c> and this value no longer affects queue health (health keys off depth + error%).
    /// Retained for wire/option compatibility. Default 90s.
    /// </para>
    /// </summary>
    public long WedgeThresholdMs { get; set; } = 90_000;

    /// <summary>
    /// How many historical samples to request from the Management API for the sparkline series
    /// (maps to the API's <c>*_age</c>/<c>*_incr</c> windowing). Default 26.
    /// </summary>
    public int SeriesLength { get; set; } = 26;

    /// <summary>
    /// Max messages to peek per <c>.error</c> queue when listing failed messages, to bound the
    /// cost of a refresh against a very deep error queue. Default 100.
    /// </summary>
    public int FailedPeekPerQueue { get; set; } = 100;
}
