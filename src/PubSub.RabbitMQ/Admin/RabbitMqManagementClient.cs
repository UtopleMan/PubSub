using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Text;
using Microsoft.Extensions.Logging;

namespace PubSub.RabbitMQ.Admin;

/// <summary>
/// Outcome of a Management HTTP API call. <see cref="Ok"/> is <c>false</c> when the endpoint was
/// unreachable or returned a non-success status; the admin degrades on that (renders
/// "disconnected"/zeroed) instead of surfacing a 500.
/// </summary>
internal readonly record struct ManagementResult<T>(T? Value, bool Ok)
{
    public static ManagementResult<T> Success(T value) => new(value, true);
    public static ManagementResult<T> Failure() => new(default, false);
}

/// <summary>
/// Read surface of the RabbitMQ Management HTTP API consumed by the admin. Abstracted so
/// <see cref="ManagementRabbitMqPubSubAdmin"/> can be unit-tested with a fake client.
/// </summary>
internal interface IRabbitMqManagementClient
{
    Task<ManagementResult<ManagementOverview>> GetOverviewAsync(CancellationToken ct = default);
    Task<ManagementResult<IReadOnlyList<ManagementQueue>>> GetQueuesAsync(string vhost, CancellationToken ct = default);
    Task<ManagementResult<IReadOnlyList<ManagementBinding>>> GetBindingsAsync(string vhost, CancellationToken ct = default);
    Task<ManagementResult<IReadOnlyList<ManagementExchange>>> GetExchangesAsync(string vhost, CancellationToken ct = default);
    Task<ManagementResult<IReadOnlyList<ManagementConsumer>>> GetConsumersAsync(string vhost, CancellationToken ct = default);
}

/// <summary>
/// Typed <see cref="HttpClient"/> wrapper over the RabbitMQ Management HTTP API. Uses HTTP basic
/// auth from <see cref="PubSubAdminOptions"/>; deserializes via <see cref="ManagementJsonContext"/>.
/// Every method degrades to <see cref="ManagementResult{T}.Failure"/> on transport/HTTP failure
/// rather than throwing, so the console can render a disconnected state.
/// <para>
/// This is the isolated, unit-testable seam: point it at a stub <see cref="HttpMessageHandler"/>
/// returning canned management JSON.
/// </para>
/// </summary>
internal sealed class RabbitMqManagementClient : IRabbitMqManagementClient
{
    // Retention window for the requested rate/length sample history. incr = seconds between
    // samples; age = total seconds of history (≈ SeriesLength samples). Sent as the API's
    // *_age/*_incr query params; the broker returns what its retention policy actually holds
    // (empty when management_rates_mode = none — handled as zero/empty upstream).
    private const int SampleIncrementSeconds = 5;

    private readonly HttpClient _http;
    private readonly PubSubAdminOptions _options;
    private readonly ILogger<RabbitMqManagementClient> _logger;

    public RabbitMqManagementClient(
        HttpClient http, PubSubAdminOptions options, ILogger<RabbitMqManagementClient> logger)
    {
        _http = http;
        _options = options;
        _logger = logger;

        if (_http.BaseAddress is null && options.ManagementBaseUrl is not null)
            _http.BaseAddress = options.ManagementBaseUrl;

        if (_http.DefaultRequestHeaders.Authorization is null
            && options.ManagementUser.Length > 0)
        {
            var raw = $"{options.ManagementUser}:{options.ManagementPassword}";
            var token = Convert.ToBase64String(Encoding.UTF8.GetBytes(raw));
            _http.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic", token);
        }
    }

    public Task<ManagementResult<ManagementOverview>> GetOverviewAsync(CancellationToken ct = default)
        => GetAsync("api/overview" + RatesQuery(), ManagementJsonContext.Default.ManagementOverview, ct);

    public Task<ManagementResult<IReadOnlyList<ManagementQueue>>> GetQueuesAsync(
        string vhost, CancellationToken ct = default)
        => GetListAsync($"api/queues/{Encode(vhost)}" + RatesQuery(),
            ManagementJsonContext.Default.IReadOnlyListManagementQueue, ct);

    public Task<ManagementResult<IReadOnlyList<ManagementBinding>>> GetBindingsAsync(
        string vhost, CancellationToken ct = default)
        => GetListAsync($"api/bindings/{Encode(vhost)}",
            ManagementJsonContext.Default.IReadOnlyListManagementBinding, ct);

    public Task<ManagementResult<IReadOnlyList<ManagementExchange>>> GetExchangesAsync(
        string vhost, CancellationToken ct = default)
        => GetListAsync($"api/exchanges/{Encode(vhost)}",
            ManagementJsonContext.Default.IReadOnlyListManagementExchange, ct);

    public Task<ManagementResult<IReadOnlyList<ManagementConsumer>>> GetConsumersAsync(
        string vhost, CancellationToken ct = default)
        => GetListAsync($"api/consumers/{Encode(vhost)}",
            ManagementJsonContext.Default.IReadOnlyListManagementConsumer, ct);

    private async Task<ManagementResult<T>> GetAsync<T>(
        string path, System.Text.Json.Serialization.Metadata.JsonTypeInfo<T> typeInfo,
        CancellationToken ct)
    {
        try
        {
            using var response = await _http.GetAsync(path, ct).ConfigureAwait(false);
            if (!response.IsSuccessStatusCode)
            {
                _logger.LogWarning("Management API GET {Path} returned {Status}", path, (int)response.StatusCode);
                return ManagementResult<T>.Failure();
            }
            var value = await response.Content.ReadFromJsonAsync(typeInfo, ct).ConfigureAwait(false);
            return value is null ? ManagementResult<T>.Failure() : ManagementResult<T>.Success(value);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogWarning(ex, "Management API GET {Path} failed", path);
            return ManagementResult<T>.Failure();
        }
    }

    // List endpoints: a missing/null body degrades to an empty list (still Ok=true), so callers
    // distinguish "reachable, nothing there" from "unreachable".
    private async Task<ManagementResult<IReadOnlyList<T>>> GetListAsync<T>(
        string path, System.Text.Json.Serialization.Metadata.JsonTypeInfo<IReadOnlyList<T>> typeInfo,
        CancellationToken ct)
    {
        try
        {
            using var response = await _http.GetAsync(path, ct).ConfigureAwait(false);
            if (!response.IsSuccessStatusCode)
            {
                _logger.LogWarning("Management API GET {Path} returned {Status}", path, (int)response.StatusCode);
                return ManagementResult<IReadOnlyList<T>>.Failure();
            }
            var value = await response.Content.ReadFromJsonAsync(typeInfo, ct).ConfigureAwait(false);
            return ManagementResult<IReadOnlyList<T>>.Success(value ?? []);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogWarning(ex, "Management API GET {Path} failed", path);
            return ManagementResult<IReadOnlyList<T>>.Failure();
        }
    }

    private string RatesQuery()
    {
        var incr = SampleIncrementSeconds;
        var age = Math.Max(1, _options.SeriesLength) * incr;
        return $"?msg_rates_age={age}&msg_rates_incr={incr}&lengths_age={age}&lengths_incr={incr}";
    }

    // The default vhost "/" must reach the API as "%2F" (not decoded to a path separator).
    private static string Encode(string vhost) => Uri.EscapeDataString(vhost);
}
