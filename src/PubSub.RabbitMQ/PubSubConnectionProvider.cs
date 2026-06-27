using Microsoft.Extensions.Logging;
using RabbitMQ.Client;

namespace PubSub.RabbitMQ;

public sealed class PubSubConnectionProvider : IAsyncDisposable
{
    private readonly PubSubRabbitMqOptions _options;
    private readonly ILogger<PubSubConnectionProvider> _logger;
    private readonly SemaphoreSlim _gate = new(1, 1);
    private IConnection? _connection;

    public PubSubConnectionProvider(PubSubRabbitMqOptions options, ILogger<PubSubConnectionProvider> logger)
    {
        _options = options;
        _logger = logger;
    }

    public async ValueTask<IConnection> GetOrOpenAsync(CancellationToken cancellationToken)
    {
        if (_connection is { IsOpen: true }) return _connection;

        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_connection is { IsOpen: true }) return _connection;

            if (string.IsNullOrWhiteSpace(_options.ConnectionString))
                throw new InvalidOperationException(
                    "PubSubRabbitMqOptions.ConnectionString is empty — configure it before AddPubSubRabbitMq.");

            var factory = new ConnectionFactory
            {
                Uri = new Uri(_options.ConnectionString),
                AutomaticRecoveryEnabled = true,
                TopologyRecoveryEnabled = true,
                NetworkRecoveryInterval = TimeSpan.FromSeconds(5),
                ClientProvidedName = $"pubsub-rabbitmq:{_options.PodName}",
            };

            _connection = await factory.CreateConnectionAsync(cancellationToken).ConfigureAwait(false);
            _logger.LogInformation(
                "PubSub.RabbitMQ connection opened to {Endpoint} (client name {ClientName})",
                _connection.Endpoint, factory.ClientProvidedName);
            return _connection;
        }
        finally
        {
            _gate.Release();
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_connection is null) return;
        try
        {
            if (_connection.IsOpen)
            {
                await _connection.CloseAsync().ConfigureAwait(false);
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Error closing PubSub.RabbitMQ connection");
        }
        finally
        {
            _connection.Dispose();
            _gate.Dispose();
        }
    }

    internal IConnection RequireOpenConnection()
        => _connection is { IsOpen: true } c
            ? c
            : throw new InvalidOperationException(
                "Connection is not open. PubSubRabbitMqHostedService must run before any publisher / consumer.");
}
