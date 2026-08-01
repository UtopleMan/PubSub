using System.Text.Json.Serialization;

namespace PubSub.RabbitMQ.Admin;

/// <summary>
/// Source-generated <see cref="JsonSerializerContext"/> for the RabbitMQ Management HTTP API DTOs,
/// so <see cref="RabbitMqManagementClient"/> deserializes without reflection (trim/AOT safe).
/// Property names are mapped explicitly on the DTOs, so no naming policy is applied here.
/// </summary>
[JsonSourceGenerationOptions(DefaultIgnoreCondition = JsonIgnoreCondition.Never)]
[JsonSerializable(typeof(ManagementOverview))]
[JsonSerializable(typeof(ManagementQueue))]
[JsonSerializable(typeof(IReadOnlyList<ManagementQueue>))]
[JsonSerializable(typeof(ManagementBinding))]
[JsonSerializable(typeof(IReadOnlyList<ManagementBinding>))]
[JsonSerializable(typeof(ManagementExchange))]
[JsonSerializable(typeof(IReadOnlyList<ManagementExchange>))]
[JsonSerializable(typeof(ManagementConsumer))]
[JsonSerializable(typeof(IReadOnlyList<ManagementConsumer>))]
internal partial class ManagementJsonContext : JsonSerializerContext;
