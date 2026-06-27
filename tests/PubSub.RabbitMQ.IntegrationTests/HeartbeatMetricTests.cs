using System.Diagnostics.Metrics;
using Microsoft.Extensions.DependencyInjection;
using Shouldly;
using Xunit;

namespace PubSub.RabbitMQ.IntegrationTests;

[Collection(RabbitMqCollection.Name)]
public class HeartbeatMetricTests(RabbitMqFixture rmq)
{
    [Fact]
    public async Task InFlightAgeMs_gauge_reports_stuck_handler()
    {
        using var host = HostBuilder.Build(rmq.ConnectionString, b =>
        {
            b.Publish<SlowMessage>();
            b.Subscribe<SlowMessage, SlowConsumer>();
        });

        var samples = new List<long>();
        using var listener = new MeterListener
        {
            InstrumentPublished = (instr, l) =>
            {
                if (instr.Meter.Name == "PubSub.RabbitMQ" && instr.Name == "pubsub.consumer.in_flight_age_ms")
                    l.EnableMeasurementEvents(instr);
            },
        };
        listener.SetMeasurementEventCallback<long>((instr, m, tags, state) => samples.Add(m));
        listener.Start();

        await host.StartAsync();
        var publisher = host.Services.GetRequiredService<PubSub.IPublish<SlowMessage>>();
        var recorder = host.Services.GetRequiredService<MessageRecorder>();

        await publisher.PublishAsync(new SlowMessage(1));
        await recorder.SlowStarted.Task.WaitAsync(TimeSpan.FromSeconds(10));

        await Task.Delay(2_000);
        listener.RecordObservableInstruments();

        samples.ShouldNotBeEmpty();
        samples.Max().ShouldBeGreaterThanOrEqualTo(1_500);

        recorder.ReleaseSlow.TrySetResult();
        await host.StopAsync();
    }
}
