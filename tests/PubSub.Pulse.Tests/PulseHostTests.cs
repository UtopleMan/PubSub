using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.RegularExpressions;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using PubSub.Admin;
using Shouldly;
using Xunit;

namespace PubSub.Pulse.Tests;

public class PulseHostTests
{
    // Mirrors the wire contract the host uses (camelCase + string enums, per PubSubAdminJsonContext).
    private static readonly JsonSerializerOptions Wire = new(JsonSerializerDefaults.Web)
    {
        Converters = { new JsonStringEnumConverter() },
    };

    private static async Task<(WebApplication app, HttpClient client, FakeAdmin admin)> StartAsync(bool withAdmin = true)
    {
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseTestServer();
        builder.Services.AddPubSubPulse();
        var admin = new FakeAdmin();
        if (withAdmin) builder.Services.AddSingleton<IPubSubAdmin>(admin);
        var app = builder.Build();
        app.UsePubSubPulse("/pulse");
        await app.StartAsync();
        return (app, app.GetTestClient(), admin);
    }

    [Fact]
    public async Task Get_queues_returns_json_array_of_queue_stats()
    {
        var (app, client, _) = await StartAsync();
        await using var _a = app;

        var resp = await client.GetAsync("/pulse/api/queues");
        resp.StatusCode.ShouldBe(HttpStatusCode.OK);
        resp.Content.Headers.ContentType!.MediaType.ShouldBe("application/json");

        var stats = await resp.Content.ReadFromJsonAsync<List<QueueStat>>(Wire);
        stats.ShouldNotBeNull();
        stats!.Count.ShouldBe(1);
        stats[0].Queue.ShouldBe("shop.events.orders.placed.OrderConsumer");
        stats[0].Status.ShouldBe(QueueHealth.Warning);
    }

    [Fact]
    public async Task Post_replay_binds_body_and_invokes_admin()
    {
        var (app, client, admin) = await StartAsync();
        await using var _a = app;

        var resp = await client.PostAsJsonAsync("/pulse/api/failed/replay",
            new ReplayRequest("shop.events.orders.placed.OrderConsumer.error", ["abc123"]), Wire);

        resp.StatusCode.ShouldBe(HttpStatusCode.OK);
        var result = await resp.Content.ReadFromJsonAsync<ReplayResult>(Wire);
        result!.Replayed.ShouldBe(1);
        admin.LastReplay.ShouldNotBeNull();
        admin.LastReplay!.ErrorQueue.ShouldBe("shop.events.orders.placed.OrderConsumer.error");
        admin.LastReplay.MessageIds.ShouldContain("abc123");
    }

    [Fact]
    public async Task Get_root_serves_index_html_with_pulse_base_href()
    {
        var (app, client, _) = await StartAsync();
        await using var _a = app;

        var resp = await client.GetAsync("/pulse/");
        resp.StatusCode.ShouldBe(HttpStatusCode.OK);
        resp.Content.Headers.ContentType!.MediaType.ShouldBe("text/html");
        var html = await resp.Content.ReadAsStringAsync();
        html.ShouldContain("<base href=\"/pulse/");
    }

    [Fact]
    public async Task Framework_bootstrap_script_is_served_as_javascript()
    {
        var (app, client, _) = await StartAsync();
        await using var _a = app;

        // Match the actual <script src> (fingerprinted at publish), not the unfingerprinted
        // importmap key of the same name — the latter is a bare module specifier the browser
        // remaps, so it is deliberately never served.
        var html = await client.GetStringAsync("/pulse/");
        var m = Regex.Match(html, @"<script src=""(_framework/blazor\.webassembly[^""]*\.js)""");
        m.Success.ShouldBeTrue("index.html should reference the blazor.webassembly bootstrap");

        var resp = await client.GetAsync("/pulse/" + m.Groups[1].Value);
        resp.StatusCode.ShouldBe(HttpStatusCode.OK);
        resp.Content.Headers.ContentType!.MediaType.ShouldBe("text/javascript");
    }

    [Fact]
    public async Task Blueprint_css_content_asset_is_embedded_and_served()
    {
        var (app, client, _) = await StartAsync();
        await using var _a = app;

        var resp = await client.GetAsync("/pulse/_content/BlazorBlueprint.Components/blazorblueprint.css");
        resp.StatusCode.ShouldBe(HttpStatusCode.OK);
        resp.Content.Headers.ContentType!.MediaType.ShouldBe("text/css");
    }

    [Fact]
    public async Task Api_returns_503_when_no_admin_registered()
    {
        var (app, client, _) = await StartAsync(withAdmin: false);
        await using var _a = app;

        var resp = await client.GetAsync("/pulse/api/queues");
        resp.StatusCode.ShouldBe(HttpStatusCode.ServiceUnavailable);
    }
}
