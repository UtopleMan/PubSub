namespace PubSub.Admin;

/// <summary>
/// Vendor-neutral administration/statistics surface over a running PubSub installation.
/// The RabbitMQ implementation lives in <c>PubSub.RabbitMQ</c>; a future backend (Kafka, …)
/// can implement the same interface and drive the same <c>PubSub.Pulse</c> console.
/// <para>
/// The RabbitMQ implementation reads the broker directly (RabbitMQ Management HTTP API for
/// topology/depth/rates; AMQP for error-queue contents), so it reflects the whole vhost rather
/// than a single process. Handler latency (p95) and in-flight age are not available from the
/// broker and are reported as <c>0</c>.
/// </para>
/// </summary>
public interface IPubSubAdmin
{
    /// <summary>Identity + connectivity of the broker this console is attached to.</summary>
    Task<MessagingInstallation> GetInstallationAsync(CancellationToken cancellationToken = default);

    /// <summary>Exchanges and routing keys with consumer queues in the vhost.</summary>
    Task<TopologySnapshot> GetTopologyAsync(CancellationToken cancellationToken = default);

    /// <summary>Per-queue live statistics for every consumer queue in the vhost.</summary>
    Task<IReadOnlyList<QueueStat>> GetQueueStatsAsync(CancellationToken cancellationToken = default);

    /// <summary>The headline metric cards (publish/consume/DLQ rates; handler p95 is <c>0</c>).</summary>
    Task<PubSubKpis> GetKpisAsync(CancellationToken cancellationToken = default);

    /// <summary>Messages currently dead-lettered in <c>{queue}.error</c> queues, filtered by <paramref name="query"/>.</summary>
    Task<IReadOnlyList<FailedMessage>> GetFailedMessagesAsync(FailedQuery query, CancellationToken cancellationToken = default);

    /// <summary>Republish dead-lettered messages back to their origin exchange/routing key.</summary>
    Task<ReplayResult> ReplayAsync(ReplayRequest request, CancellationToken cancellationToken = default);

    /// <summary>Permanently drop dead-lettered messages from an error queue.</summary>
    Task<DeleteResult> DeleteAsync(DeleteRequest request, CancellationToken cancellationToken = default);
}
