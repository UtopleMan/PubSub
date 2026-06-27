using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace PubSub.RabbitMQ;

internal static class PubSubDiagnostics
{
    public const string SourceName = "PubSub.RabbitMQ";

    public static readonly ActivitySource ActivitySource = new(SourceName);

    public static readonly Meter Meter = new(SourceName);

    public static readonly Counter<long> PublishCount =
        Meter.CreateCounter<long>("pubsub.publish.count", unit: "{message}");

    public static readonly Counter<long> ConsumeCount =
        Meter.CreateCounter<long>("pubsub.consume.count", unit: "{message}");

    public static readonly Counter<long> DlqPublishCount =
        Meter.CreateCounter<long>("pubsub.dlq.publish.count", unit: "{message}");

    private static readonly List<IInFlightTracker> Trackers = new();
    private static readonly Lock TrackersLock = new();
    private static bool _gaugeRegistered;

    public static void RegisterTracker(IInFlightTracker tracker)
    {
        lock (TrackersLock)
        {
            Trackers.Add(tracker);
            if (!_gaugeRegistered)
            {
                Meter.CreateObservableGauge(
                    "pubsub.consumer.in_flight_age_ms",
                    ObserveInFlightAges,
                    unit: "ms",
                    description: "Wall-clock age in ms of the message currently being processed by each consumer.");
                _gaugeRegistered = true;
            }
        }
    }

    public static void UnregisterTracker(IInFlightTracker tracker)
    {
        lock (TrackersLock)
        {
            Trackers.Remove(tracker);
        }
    }

    private static IEnumerable<Measurement<long>> ObserveInFlightAges()
    {
        IInFlightTracker[] snapshot;
        lock (TrackersLock)
        {
            snapshot = Trackers.ToArray();
        }
        foreach (var t in snapshot)
        {
            foreach (var m in t.Observe()) yield return m;
        }
    }
}

internal interface IInFlightTracker
{
    IEnumerable<Measurement<long>> Observe();
}
