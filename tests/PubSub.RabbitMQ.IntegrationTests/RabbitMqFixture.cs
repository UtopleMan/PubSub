using Testcontainers.RabbitMq;
using Xunit;

namespace PubSub.RabbitMQ.IntegrationTests;

public sealed class RabbitMqFixture : IAsyncLifetime
{
    private RabbitMqContainer? _container;

    public string ConnectionString { get; private set; } = string.Empty;
    public string ManagementUri { get; private set; } = string.Empty;
    public string ManagementUser { get; private set; } = string.Empty;
    public string ManagementPassword { get; private set; } = string.Empty;

    public async Task InitializeAsync()
    {
        _container = new RabbitMqBuilder()
            .WithImage("rabbitmq:3.13-management")
            .WithUsername("admin")
            .WithPassword("admin")
            .WithPortBinding(15672, assignRandomHostPort: true)
            .Build();
        await _container.StartAsync();
        ConnectionString = _container.GetConnectionString();
        ManagementUri = $"http://{_container.Hostname}:{_container.GetMappedPublicPort(15672)}";
        ManagementUser = "admin";
        ManagementPassword = "admin";
    }

    public async Task DisposeAsync()
    {
        if (_container is not null) await _container.DisposeAsync();
    }
}

[CollectionDefinition(Name)]
public sealed class RabbitMqCollection : ICollectionFixture<RabbitMqFixture>
{
    public const string Name = "rabbitmq";
}
