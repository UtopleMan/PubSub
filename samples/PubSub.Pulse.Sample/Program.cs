using PubSub.Pulse;
using PubSub.Pulse.Sample;
using PubSub.RabbitMQ;

var builder = WebApplication.CreateBuilder(args);

var connectionString = builder.Configuration.GetConnectionString("RabbitMq")
    ?? "amqp://admin:admin@localhost:5672/";

builder.Services.AddPubSubRabbitMq(new PubSubRabbitMqOptions
{
    ConnectionString = connectionString,
    PodName = "pulse-sample",
}, b =>
{
    b.Publish<OrderPlaced>();
    b.Subscribe<OrderPlaced, OrderProjector>();
    b.Publish<PaymentCaptured>();
    b.Subscribe<PaymentCaptured, PaymentPoster>();
    b.Publish<InventoryReserved>();
    b.Subscribe<InventoryReserved, FlakyReserver>();
});

builder.Services.AddPubSubRabbitMqAdmin(o => o.ServiceName = "Shop.Api");
builder.Services.AddPubSubPulse(o => o.Title = "PubSub Pulse — Shop");
builder.Services.AddHostedService<DemoTraffic>();

var app = builder.Build();

app.MapGet("/", () => Results.Redirect("/pulse"));
app.UsePubSubPulse("/pulse");

app.Run();
