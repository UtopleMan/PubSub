using PubSub.RabbitMQ;
using Shouldly;
using Xunit;

namespace PubSub.Tests;

/// <summary>
/// Pure-math checks for the in-process metrics sampler's bucketing/percentile — the code that
/// turns handler_ms histogram samples into the p95 the console shows. No broker required.
/// </summary>
public class MetricsSamplerMathTests
{
    [Theory]
    [InlineData(0, 0)]     // <=1ms -> bucket 0
    [InlineData(1, 0)]
    [InlineData(2, 1)]
    [InlineData(3, 2)]     // <=5ms -> bucket 2
    [InlineData(50, 5)]
    [InlineData(999, 9)]   // <=1000ms -> bucket 9
    [InlineData(1000, 9)]
    [InlineData(45000, 14)] // beyond 30000 -> last (inf) bucket
    public void BucketIndex_MapsMsToExpectedBucket(long ms, int expected)
        => PubSubMetricsSampler.BucketIndex(ms).ShouldBe(expected);

    [Fact]
    public void Percentile_EmptyDistribution_IsZero()
    {
        var buckets = new long[PubSubMetricsSampler.BucketBoundsMs.Length];
        PubSubMetricsSampler.Percentile(buckets, 0.95).ShouldBe(0);
    }

    [Fact]
    public void Percentile_P95_LandsInTheBucketHoldingThe95thSample()
    {
        // 100 samples: 96 fast (<=5ms, bucket 2), 4 slow (~2000ms, bucket 10).
        // The 95th sample is still in the fast bucket, so p95 == that bucket's bound (5ms).
        var buckets = new long[PubSubMetricsSampler.BucketBoundsMs.Length];
        buckets[2] = 96;
        buckets[10] = 4;

        PubSubMetricsSampler.Percentile(buckets, 0.95).ShouldBe(5);
    }

    [Fact]
    public void Percentile_WhenTailExceeds5Percent_ReportsTheSlowBucket()
    {
        // 90 fast (bucket 2 = 5ms), 10 slow (bucket 10 = 2000ms). p95 falls in the slow bucket.
        var buckets = new long[PubSubMetricsSampler.BucketBoundsMs.Length];
        buckets[2] = 90;
        buckets[10] = 10;

        PubSubMetricsSampler.Percentile(buckets, 0.95).ShouldBe(2000);
    }

    [Fact]
    public void Percentile_InfiniteBucket_IsCappedAt30s()
    {
        var buckets = new long[PubSubMetricsSampler.BucketBoundsMs.Length];
        buckets[^1] = 10; // all samples in the +inf bucket

        PubSubMetricsSampler.Percentile(buckets, 0.95).ShouldBe(30_000);
    }
}
