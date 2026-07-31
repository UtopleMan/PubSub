namespace PubSub.Admin;

/// <summary>
/// Vendor-neutral administration/statistics surface over a running PubSub installation.
/// The RabbitMQ implementation lives in <c>PubSub.RabbitMQ</c>; a future backend (Kafka, …)
/// can implement the same interface and drive the same <c>PubSub.Pulse</c> console.
/// <para>
/// All members reflect the local process: the publishers/consumers registered in this host and
/// the queues they own. Rate metrics are process-scoped (sampled from in-process meters); depth
/// and error-queue contents are read live from the broker.
/// </para>
/// </summary>
public interface IPubSubAdmin
{
    /// <summary>Identity + connectivity of the broker this process is attached to.</summary>
    Task<MessagingInstallation> GetInstallationAsync(CancellationToken cancellationToken = default);

    /// <summary>Registered exchanges and routing keys this process participates in.</summary>
    Task<TopologySnapshot> GetTopologyAsync(CancellationToken cancellationToken = default);

    /// <summary>Per-queue live statistics for every registered consumer endpoint.</summary>
    Task<IReadOnlyList<QueueStat>> GetQueueStatsAsync(CancellationToken cancellationToken = default);

    /// <summary>The headline metric cards (publish/consume/DLQ rates, handler p95).</summary>
    Task<PubSubKpis> GetKpisAsync(CancellationToken cancellationToken = default);

    /// <summary>Messages currently dead-lettered in <c>{queue}.error</c> queues, filtered by <paramref name="query"/>.</summary>
    Task<IReadOnlyList<FailedMessage>> GetFailedMessagesAsync(FailedQuery query, CancellationToken cancellationToken = default);

    /// <summary>Republish dead-lettered messages back to their origin exchange/routing key.</summary>
    Task<ReplayResult> ReplayAsync(ReplayRequest request, CancellationToken cancellationToken = default);

    /// <summary>Permanently drop dead-lettered messages from an error queue.</summary>
    Task<DeleteResult> DeleteAsync(DeleteRequest request, CancellationToken cancellationToken = default);
}
