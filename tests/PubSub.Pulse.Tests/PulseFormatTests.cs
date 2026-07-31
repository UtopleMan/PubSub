using PubSub.Pulse.Client;
using Shouldly;
using Xunit;

namespace PubSub.Pulse.Tests;

/// <summary>Verifies the client formatters match the design's JS output exactly.</summary>
public class PulseFormatTests
{
    [Theory]
    [InlineData(95_000, "1m 35s")]
    [InlineData(60_000, "1m 00s")]
    [InlineData(1_500, "1.5s")]
    [InlineData(999, "999ms")]
    [InlineData(0, "0ms")]
    public void Ms_matches_design(long ms, string expected) => PulseFormat.Ms(ms).ShouldBe(expected);

    [Theory]
    [InlineData(6.0, "6.0")]
    [InlineData(1.25, "1.3")]     // one decimal, rounded
    [InlineData(240.0, "240")]    // >=100 -> rounded, no decimals
    [InlineData(1240.0, "1,240")] // thousands separator
    public void Rate_matches_design(double n, string expected) => PulseFormat.Rate(n).ShouldBe(expected);

    [Theory]
    [InlineData(999, "999")]
    [InlineData(1000, "1,000")]
    [InlineData(90000, "90,000")]
    public void Int_matches_design(long n, string expected) => PulseFormat.Int(n).ShouldBe(expected);

    [Theory]
    [InlineData(0.0, "—")]
    [InlineData(0.04, "—")]
    [InlineData(1.6, "1.6%")]
    public void ErrorPercent_matches_design(double pct, string expected)
        => PulseFormat.ErrorPercent(pct).ShouldBe(expected);

    [Fact]
    public void Spark_produces_a_path_with_one_point_per_value()
    {
        var path = PulseFormat.Spark(new double[] { 1, 2, 3 }, 160, 30);
        path.ShouldStartWith("M");
        path.Split('M', 'L').Where(s => s.Length > 0).Count().ShouldBe(3);
    }

    [Fact]
    public void SparkArea_closes_the_path_to_the_baseline()
        => PulseFormat.SparkArea(new double[] { 1, 2 }, 160, 30).ShouldEndWith("L0 30 Z");
}
