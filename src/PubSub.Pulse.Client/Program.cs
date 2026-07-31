using BlazorBlueprint.Components;
using Microsoft.AspNetCore.Components.Web;
using Microsoft.AspNetCore.Components.WebAssembly.Hosting;
using PubSub.Pulse.Client;

var builder = WebAssemblyHostBuilder.CreateDefault(args);
builder.RootComponents.Add<App>("#app");
builder.RootComponents.Add<HeadOutlet>("head::after");

builder.Services.AddBlazorBlueprintComponents();

// REST endpoints are hosted by PubSub.Pulse under {mount}/api. The WASM base href is the
// mount path (e.g. /pulse/), so the API root is "api/" relative to it.
builder.Services.AddScoped(sp => new HttpClient
{
    BaseAddress = new Uri(new Uri(builder.HostEnvironment.BaseAddress), "api/"),
});
builder.Services.AddScoped<PulseApiClient>();

await builder.Build().RunAsync();
