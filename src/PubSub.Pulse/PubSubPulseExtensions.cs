using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.AspNetCore.StaticFiles;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.FileProviders;
using PubSub.Admin;

namespace PubSub.Pulse;

/// <summary>
/// Hosts the embedded Blazor WebAssembly monitoring console and its REST API. The WASM app is
/// embedded in this assembly and served through a <see cref="ManifestEmbeddedFileProvider"/>;
/// the REST endpoints query the registered <see cref="IPubSubAdmin"/>.
/// </summary>
public static class PubSubPulseExtensions
{
    /// <summary>Registers Pulse options and wires the admin JSON contract into HTTP serialization.</summary>
    public static IServiceCollection AddPubSubPulse(
        this IServiceCollection services,
        Action<PubSubPulseOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);

        var options = new PubSubPulseOptions();
        configure?.Invoke(options);
        services.TryAddSingleton(options);

        services.ConfigureHttpJsonOptions(o =>
            o.SerializerOptions.TypeInfoResolverChain.Insert(0, PubSubAdminJsonContext.Default));

        return services;
    }

    /// <summary>
    /// Serves the console at <paramref name="path"/> (default <c>/pulse</c>) and maps its REST API
    /// under <c>{path}/api</c>. Mount behind your own auth if the stats should not be public.
    /// </summary>
    public static WebApplication UsePubSubPulse(this WebApplication app, string path = "/pulse")
    {
        ArgumentNullException.ThrowIfNull(app);
        var mount = "/" + path.Trim('/');

        var files = new EmbeddedPulseFileProvider(typeof(PubSubPulseExtensions).Assembly);

        // Serve the embedded WASM assets. This is pure middleware: because no routing endpoint
        // matches asset paths (only {mount}/api/* are endpoints), StaticFileMiddleware serves them.
        app.UseStaticFiles(new StaticFileOptions
        {
            FileProvider = files,
            RequestPath = mount,
            ServeUnknownFileTypes = true,
            DefaultContentType = "application/octet-stream",
            ContentTypeProvider = ContentTypes(),
        });

        MapApi(app, mount);

        // SPA fallback as terminal middleware (not a routing endpoint, so it never shadows the
        // static assets): serve index.html for extension-less GETs under the mount that no
        // endpoint handled — i.e. the client's own routes and the mount root.
        app.Use(async (ctx, next) =>
        {
            var path = ctx.Request.Path;
            if (HttpMethods.IsGet(ctx.Request.Method)
                && ctx.GetEndpoint() is null
                && path.StartsWithSegments(mount)
                && !path.StartsWithSegments(mount + "/api")
                && !Path.HasExtension(path.Value ?? string.Empty))
            {
                var index = files.GetFileInfo("index.html");
                if (index.Exists)
                {
                    ctx.Response.ContentType = "text/html";
                    await using var stream = index.CreateReadStream();
                    await stream.CopyToAsync(ctx.Response.Body);
                    return;
                }
            }
            await next();
        });

        return app;
    }

    private static void MapApi(WebApplication app, string mount)
    {
        var api = mount + "/api";

        app.MapGet(api + "/installation", async (IServiceProvider sp, CancellationToken ct)
            => await WithAdmin(sp, a => a.GetInstallationAsync(ct)));
        app.MapGet(api + "/topology", async (IServiceProvider sp, CancellationToken ct)
            => await WithAdmin(sp, a => a.GetTopologyAsync(ct)));
        app.MapGet(api + "/queues", async (IServiceProvider sp, CancellationToken ct)
            => await WithAdmin(sp, a => a.GetQueueStatsAsync(ct)));
        app.MapGet(api + "/kpis", async (IServiceProvider sp, CancellationToken ct)
            => await WithAdmin(sp, a => a.GetKpisAsync(ct)));
        app.MapGet(api + "/failed", async (IServiceProvider sp, string? exceptionType, string? search, int? limit, CancellationToken ct)
            => await WithAdmin(sp, a => a.GetFailedMessagesAsync(
                new FailedQuery(exceptionType, search, limit ?? 200), ct)));

        app.MapGet(api + "/options", (PubSubPulseOptions o) => Results.Ok(new PulseClientOptions(o.Title, o.PollIntervalMs, o.WedgeThresholdMs)));

        app.MapPost(api + "/failed/replay", async (ReplayRequest req, IServiceProvider sp, CancellationToken ct)
            => await WithAdmin(sp, a => a.ReplayAsync(req, ct)));
        app.MapPost(api + "/failed/delete", async (DeleteRequest req, IServiceProvider sp, CancellationToken ct)
            => await WithAdmin(sp, a => a.DeleteAsync(req, ct)));
    }

    private static async Task<IResult> WithAdmin<T>(IServiceProvider sp, Func<IPubSubAdmin, Task<T>> query)
    {
        var admin = sp.GetService<IPubSubAdmin>();
        if (admin is null)
            return Results.Problem(
                "No IPubSubAdmin is registered. Add one (e.g. services.AddPubSubRabbitMqAdmin()).",
                statusCode: StatusCodes.Status503ServiceUnavailable);
        return Results.Ok(await query(admin));
    }

    private static FileExtensionContentTypeProvider ContentTypes()
    {
        var provider = new FileExtensionContentTypeProvider();
        provider.Mappings[".wasm"] = "application/wasm";
        provider.Mappings[".blat"] = "application/octet-stream";
        provider.Mappings[".dat"] = "application/octet-stream";
        provider.Mappings[".dll"] = "application/octet-stream";
        provider.Mappings[".pdb"] = "application/octet-stream";
        provider.Mappings[".webcil"] = "application/octet-stream";
        return provider;
    }

    // Small client-facing options payload (subset of PubSubPulseOptions).
    private sealed record PulseClientOptions(string Title, int PollIntervalMs, long WedgeThresholdMs);
}
