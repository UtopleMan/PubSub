using System.Text.Json.Serialization;

namespace PubSub.Admin;

/// <summary>
/// Source-generated <see cref="JsonSerializerContext"/> covering every admin DTO, so the REST
/// layer (and a trimmed/AOT host) can serialize the console payloads without reflection.
/// Shared by the RabbitMQ implementation and the <c>PubSub.Pulse</c> host.
/// </summary>
[JsonSourceGenerationOptions(
    PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase,
    UseStringEnumConverter = true,
    DefaultIgnoreCondition = JsonIgnoreCondition.Never)]
[JsonSerializable(typeof(MessagingInstallation))]
[JsonSerializable(typeof(TopologySnapshot))]
[JsonSerializable(typeof(IReadOnlyList<QueueStat>))]
[JsonSerializable(typeof(QueueStat))]
[JsonSerializable(typeof(PubSubKpis))]
[JsonSerializable(typeof(PubSubKpi))]
[JsonSerializable(typeof(IReadOnlyList<FailedMessage>))]
[JsonSerializable(typeof(FailedMessage))]
[JsonSerializable(typeof(FailedQuery))]
[JsonSerializable(typeof(ReplayRequest))]
[JsonSerializable(typeof(ReplayResult))]
[JsonSerializable(typeof(DeleteRequest))]
[JsonSerializable(typeof(DeleteResult))]
public partial class PubSubAdminJsonContext : JsonSerializerContext;
