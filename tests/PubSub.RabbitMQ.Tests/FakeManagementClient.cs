using PubSub.RabbitMQ.Admin;

namespace PubSub.RabbitMQ.Tests;

/// <summary>
/// In-memory <see cref="IRabbitMqManagementClient"/> for admin-mapping unit tests. Each endpoint
/// returns a preset <see cref="ManagementResult{T}"/> (defaults: overview failed, everything else
/// an empty success), so tests set only what they exercise.
/// </summary>
internal sealed class FakeManagementClient : IRabbitMqManagementClient
{
    public ManagementResult<ManagementOverview> Overview { get; set; } = ManagementResult<ManagementOverview>.Failure();
    public ManagementResult<IReadOnlyList<ManagementQueue>> Queues { get; set; } = ManagementResult<IReadOnlyList<ManagementQueue>>.Success([]);
    public ManagementResult<IReadOnlyList<ManagementBinding>> Bindings { get; set; } = ManagementResult<IReadOnlyList<ManagementBinding>>.Success([]);
    public ManagementResult<IReadOnlyList<ManagementExchange>> Exchanges { get; set; } = ManagementResult<IReadOnlyList<ManagementExchange>>.Success([]);
    public ManagementResult<IReadOnlyList<ManagementConsumer>> Consumers { get; set; } = ManagementResult<IReadOnlyList<ManagementConsumer>>.Success([]);

    public Task<ManagementResult<ManagementOverview>> GetOverviewAsync(CancellationToken ct = default)
        => Task.FromResult(Overview);
    public Task<ManagementResult<IReadOnlyList<ManagementQueue>>> GetQueuesAsync(string vhost, CancellationToken ct = default)
        => Task.FromResult(Queues);
    public Task<ManagementResult<IReadOnlyList<ManagementBinding>>> GetBindingsAsync(string vhost, CancellationToken ct = default)
        => Task.FromResult(Bindings);
    public Task<ManagementResult<IReadOnlyList<ManagementExchange>>> GetExchangesAsync(string vhost, CancellationToken ct = default)
        => Task.FromResult(Exchanges);
    public Task<ManagementResult<IReadOnlyList<ManagementConsumer>>> GetConsumersAsync(string vhost, CancellationToken ct = default)
        => Task.FromResult(Consumers);
}
