using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using System.Runtime.CompilerServices;
using Microsoft.Extensions.Hosting;

namespace PubSub.RabbitMQ;

/// <summary>
/// Immutable point-in-time view of the in-process PubSub meters, produced by
/// <see cref="PubSubMetricsSampler"/> every sample interval and read by the admin.
/// All rates are per second; ages/p95 are milliseconds. Dictionaries are keyed by queue
/// (except publish, which is keyed by routing key, since publishing targets an exchange+key).
/// </summary>
internal sealed record MetricsSnapshot(
    IReadOnlyDictionary<string, double> PublishRateByRoutingKey,
    IReadOnlyDictionary<string, double> ConsumeRateByQueue,
    IReadOnlyDictionary<string, double> DlqRateByQueue,
    IReadOnlyDictionary<string, long> HandlerP95ByQueue,
    IReadOnlyDictionary<string, long> InFlightByQueue,
    IReadOnlyDictionary<string, IReadOnlyList<double>> ConsumeSeriesByQueue,
    IReadOnlyList<double> PublishRateSeries,
    IReadOnlyList<double> ConsumeRateSeries,
    IReadOnlyList<double> DlqRateSeries,
    IReadOnlyList<double> HandlerP95Series,
    long OverallHandlerP95Ms)
{
    public static readonly MetricsSnapshot Empty = new(
        new Dictionary<string, double>(), new Dictionary<string, double>(), new Dictionary<string, double>(),
        new Dictionary<string, long>(), new Dictionary<string, long>(),
        new Dictionary<string, IReadOnlyList<double>>(),
        [], [], [], [], 0);
}

/// <summary>
/// Subscribes a <see cref="MeterListener"/> to the <c>PubSub.RabbitMQ</c> meter and, every
/// <see cref="PubSubAdminOptions.SampleInterval"/>, turns the raw counter/histogram/gauge
/// measurements into per-queue rates, handler p95, and in-flight ages for the console.
/// <para>
/// Rates are computed from counter deltas between ticks. Handler p95 uses fixed exponential ms
/// buckets and the per-interval bucket delta (so it reflects the recent window, not the whole
/// process lifetime). In-flight age comes from <c>RecordObservableInstruments()</c> each tick.
/// </para>
/// </summary>
internal sealed class PubSubMetricsSampler : IHostedService, IDisposable
{
    internal static readonly long[] BucketBoundsMs =
        [1, 2, 5, 10, 20, 50, 100, 200, 500, 1_000, 2_000, 5_000, 10_000, 30_000, long.MaxValue];

    private readonly PubSubAdminOptions _options;
    private readonly MeterListener _listener;

    // Cumulative measurements, mutated from the (hot-path) recording threads.
    private readonly ConcurrentDictionary<string, StrongBox<long>> _publishCum = new();
    private readonly ConcurrentDictionary<string, StrongBox<long>> _consumeCum = new();
    private readonly ConcurrentDictionary<string, StrongBox<long>> _dlqCum = new();
    private readonly ConcurrentDictionary<string, long[]> _handlerBuckets = new();

    // Sampler-thread-only state.
    private Dictionary<string, long> _inflightScratch = new();
    private Dictionary<string, long> _publishPrev = new();
    private Dictionary<string, long> _consumePrev = new();
    private Dictionary<string, long> _dlqPrev = new();
    private readonly Dictionary<string, long[]> _handlerPrev = new();
    private readonly Dictionary<string, Queue<double>> _consumeSeries = new();
    private readonly Queue<double> _publishRateSeries = new();
    private readonly Queue<double> _consumeRateSeries = new();
    private readonly Queue<double> _dlqRateSeries = new();
    private readonly Queue<double> _handlerP95Series = new();

    private volatile MetricsSnapshot _snapshot = MetricsSnapshot.Empty;
    private CancellationTokenSource? _cts;
    private Task? _loop;

    public PubSubMetricsSampler(PubSubAdminOptions options)
    {
        _options = options;
        _listener = new MeterListener
        {
            InstrumentPublished = (inst, listener) =>
            {
                if (inst.Meter.Name == PubSubDiagnostics.SourceName)
                    listener.EnableMeasurementEvents(inst);
            },
        };
        _listener.SetMeasurementEventCallback<long>(OnMeasurement);
    }

    public MetricsSnapshot Current => _snapshot;

    public Task StartAsync(CancellationToken cancellationToken)
    {
        _listener.Start();
        _cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        _loop = Task.Run(() => RunAsync(_cts.Token), CancellationToken.None);
        return Task.CompletedTask;
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        _cts?.Cancel();
        if (_loop is not null)
        {
            try { await _loop.ConfigureAwait(false); }
            catch (OperationCanceledException) { }
        }
        _listener.Dispose();
    }

    private async Task RunAsync(CancellationToken ct)
    {
        using var timer = new PeriodicTimer(_options.SampleInterval);
        try
        {
            while (await timer.WaitForNextTickAsync(ct).ConfigureAwait(false))
                Sample();
        }
        catch (OperationCanceledException) { }
    }

    private void OnMeasurement(Instrument instrument, long measurement,
        ReadOnlySpan<KeyValuePair<string, object?>> tags, object? state)
    {
        switch (instrument.Name)
        {
            case "pubsub.publish.count":
                Bump(_publishCum, Tag(tags, "routing_key"), measurement);
                break;
            case "pubsub.consume.count":
                Bump(_consumeCum, Tag(tags, "queue"), measurement);
                break;
            case "pubsub.dlq.publish.count":
                Bump(_dlqCum, Tag(tags, "queue"), measurement);
                break;
            case "pubsub.consumer.handler_ms":
                RecordHandler(Tag(tags, "queue"), measurement);
                break;
            case "pubsub.consumer.in_flight_age_ms":
                // Fired only during our RecordObservableInstruments() call on the sampler thread.
                var q = Tag(tags, "queue");
                if (q.Length > 0 && (!_inflightScratch.TryGetValue(q, out var cur) || measurement > cur))
                    _inflightScratch[q] = measurement;
                break;
        }
    }

    private static void Bump(ConcurrentDictionary<string, StrongBox<long>> dict, string key, long amount)
    {
        if (key.Length == 0) return;
        var box = dict.GetOrAdd(key, static _ => new StrongBox<long>(0));
        Interlocked.Add(ref box.Value, amount);
    }

    private void RecordHandler(string queue, long ms)
    {
        if (queue.Length == 0) return;
        var buckets = _handlerBuckets.GetOrAdd(queue, static _ => new long[BucketBoundsMs.Length]);
        Interlocked.Increment(ref buckets[BucketIndex(ms)]);
    }

    internal static int BucketIndex(long ms)
    {
        for (var i = 0; i < BucketBoundsMs.Length; i++)
            if (ms <= BucketBoundsMs[i]) return i;
        return BucketBoundsMs.Length - 1;
    }

    private void Sample()
    {
        var secs = _options.SampleInterval.TotalSeconds;
        if (secs <= 0) secs = 1;

        _inflightScratch = new Dictionary<string, long>();
        _listener.RecordObservableInstruments();
        var inflight = _inflightScratch;

        var (publishRate, publishNow) = Rates(_publishCum, _publishPrev, secs);
        var (consumeRate, consumeNow) = Rates(_consumeCum, _consumePrev, secs);
        var (dlqRate, dlqNow) = Rates(_dlqCum, _dlqPrev, secs);
        _publishPrev = publishNow;
        _consumePrev = consumeNow;
        _dlqPrev = dlqNow;

        // Handler p95 per queue from this interval's bucket delta.
        var p95ByQueue = new Dictionary<string, long>();
        var mergedDelta = new long[BucketBoundsMs.Length];
        foreach (var (queue, buckets) in _handlerBuckets)
        {
            var now = new long[BucketBoundsMs.Length];
            for (var i = 0; i < now.Length; i++) now[i] = Interlocked.Read(ref buckets[i]);
            var prev = _handlerPrev.TryGetValue(queue, out var p) ? p : new long[BucketBoundsMs.Length];
            var delta = new long[BucketBoundsMs.Length];
            for (var i = 0; i < delta.Length; i++)
            {
                delta[i] = now[i] - prev[i];
                mergedDelta[i] += delta[i];
            }
            _handlerPrev[queue] = now;
            p95ByQueue[queue] = Percentile(delta, 0.95);
        }
        var overallP95 = Percentile(mergedDelta, 0.95);

        // Per-queue consume-rate sparkline series.
        var seriesByQueue = new Dictionary<string, IReadOnlyList<double>>();
        foreach (var (queue, rate) in consumeRate)
        {
            if (!_consumeSeries.TryGetValue(queue, out var ring))
                _consumeSeries[queue] = ring = new Queue<double>();
            ring.Enqueue(rate);
            while (ring.Count > _options.SeriesLength) ring.Dequeue();
            seriesByQueue[queue] = ring.ToArray();
        }

        var totalPublish = Sum(publishRate);
        var totalConsume = Sum(consumeRate);
        var totalDlq = Sum(dlqRate);
        Push(_publishRateSeries, totalPublish);
        Push(_consumeRateSeries, totalConsume);
        Push(_dlqRateSeries, totalDlq);
        Push(_handlerP95Series, overallP95);

        _snapshot = new MetricsSnapshot(
            publishRate, consumeRate, dlqRate, p95ByQueue, inflight, seriesByQueue,
            _publishRateSeries.ToArray(), _consumeRateSeries.ToArray(),
            _dlqRateSeries.ToArray(), _handlerP95Series.ToArray(), overallP95);
    }

    private void Push(Queue<double> ring, double value)
    {
        ring.Enqueue(value);
        while (ring.Count > _options.SeriesLength) ring.Dequeue();
    }

    private static (Dictionary<string, double> rates, Dictionary<string, long> now) Rates(
        ConcurrentDictionary<string, StrongBox<long>> cum, Dictionary<string, long> prev, double secs)
    {
        var rates = new Dictionary<string, double>();
        var now = new Dictionary<string, long>();
        foreach (var (key, box) in cum)
        {
            var value = Interlocked.Read(ref box.Value);
            now[key] = value;
            var before = prev.TryGetValue(key, out var b) ? b : 0;
            rates[key] = Math.Max(0, value - before) / secs;
        }
        return (rates, now);
    }

    internal static long Percentile(long[] deltaBuckets, double q)
    {
        long total = 0;
        foreach (var c in deltaBuckets) total += c;
        if (total == 0) return 0;
        var target = total * q;
        long cumulative = 0;
        for (var i = 0; i < deltaBuckets.Length; i++)
        {
            cumulative += deltaBuckets[i];
            if (cumulative >= target)
                return Math.Min(BucketBoundsMs[i], 30_000);
        }
        return 30_000;
    }

    private static double Sum(Dictionary<string, double> d)
    {
        double s = 0;
        foreach (var v in d.Values) s += v;
        return s;
    }

    private static string Tag(ReadOnlySpan<KeyValuePair<string, object?>> tags, string name)
    {
        foreach (var t in tags)
            if (t.Key == name)
                return t.Value?.ToString() ?? string.Empty;
        return string.Empty;
    }

    public void Dispose() => _listener.Dispose();
}
