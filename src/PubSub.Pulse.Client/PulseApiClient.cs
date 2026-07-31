using System.Net.Http.Json;
using System.Text.Json;
using System.Text.Json.Serialization;
using PubSub.Admin;

namespace PubSub.Pulse.Client;

/// <summary>Client-side view of the host's <c>/api/options</c> payload.</summary>
public sealed record PulseClientOptions(string Title, int PollIntervalMs, long WedgeThresholdMs);

/// <summary>Source-gen JSON for the client-only <see cref="PulseClientOptions"/> (trim-safe in WASM).</summary>
[JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase)]
[JsonSerializable(typeof(PulseClientOptions))]
internal partial class PulseClientJsonContext : JsonSerializerContext;

/// <summary>Typed client over the Pulse REST API (base address is the console's <c>api/</c> root).</summary>
public sealed class PulseApiClient(HttpClient http)
{
    // Chain both source-gen contexts so every payload resolves without reflection (trim/AOT safe).
    // CamelCase must be set here: the naming policy on a context's [JsonSourceGenerationOptions]
    // is NOT honoured when that context is plugged into a foreign options object via the resolver
    // chain — the policy is taken from THIS options object. The host serialises camelCase (ASP.NET
    // web defaults), so without this every property silently deserialises to null/default.
    private static readonly JsonSerializerOptions Json = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        TypeInfoResolverChain = { PubSubAdminJsonContext.Default, PulseClientJsonContext.Default },
    };

    public Task<PulseClientOptions?> GetOptionsAsync(CancellationToken ct = default)
        => http.GetFromJsonAsync<PulseClientOptions>("options", Json, ct);

    public Task<MessagingInstallation?> GetInstallationAsync(CancellationToken ct = default)
        => http.GetFromJsonAsync<MessagingInstallation>("installation", Json, ct);

    public Task<IReadOnlyList<QueueStat>?> GetQueuesAsync(CancellationToken ct = default)
        => http.GetFromJsonAsync<IReadOnlyList<QueueStat>>("queues", Json, ct);

    public Task<PubSubKpis?> GetKpisAsync(CancellationToken ct = default)
        => http.GetFromJsonAsync<PubSubKpis>("kpis", Json, ct);

    public Task<TopologySnapshot?> GetTopologyAsync(CancellationToken ct = default)
        => http.GetFromJsonAsync<TopologySnapshot>("topology", Json, ct);

    public Task<IReadOnlyList<FailedMessage>?> GetFailedAsync(
        string? exceptionType = null, string? search = null, int limit = 200, CancellationToken ct = default)
    {
        var url = $"failed?limit={limit}";
        if (!string.IsNullOrEmpty(exceptionType)) url += $"&exceptionType={Uri.EscapeDataString(exceptionType)}";
        if (!string.IsNullOrEmpty(search)) url += $"&search={Uri.EscapeDataString(search)}";
        return http.GetFromJsonAsync<IReadOnlyList<FailedMessage>>(url, Json, ct);
    }

    public async Task<ReplayResult?> ReplayAsync(ReplayRequest request, CancellationToken ct = default)
    {
        var resp = await http.PostAsJsonAsync("failed/replay", request, Json, ct);
        resp.EnsureSuccessStatusCode();
        return await resp.Content.ReadFromJsonAsync<ReplayResult>(Json, ct);
    }

    public async Task<DeleteResult?> DeleteAsync(DeleteRequest request, CancellationToken ct = default)
    {
        var resp = await http.PostAsJsonAsync("failed/delete", request, Json, ct);
        resp.EnsureSuccessStatusCode();
        return await resp.Content.ReadFromJsonAsync<DeleteResult>(Json, ct);
    }
}
