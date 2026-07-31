using System.Globalization;
using System.Text;

namespace PubSub.Pulse.Client;

/// <summary>
/// Number / duration / sparkline formatters, ported 1:1 from the design's JS helpers so the
/// console reads identically to the mockup.
/// </summary>
public static class PulseFormat
{
    private static readonly CultureInfo Inv = CultureInfo.InvariantCulture;

    /// <summary>Integer with thousands separators once ≥ 1000 (e.g. 1,000).</summary>
    public static string Int(long n) => n >= 1000 ? n.ToString("N0", Inv) : n.ToString(Inv);

    /// <summary>Rate: rounded with separators ≥ 100, else one decimal (e.g. 6.0, 1,240).</summary>
    public static string Rate(double n) =>
        n >= 100 ? Math.Round(n).ToString("N0", Inv) : n.ToString("0.0", Inv);

    /// <summary>Duration: ms under 1s, one-decimal seconds under 1m, then "Xm SSs".</summary>
    public static string Ms(long ms) =>
        ms >= 60_000 ? $"{ms / 60_000}m {(ms % 60_000 / 1000):00}s"
        : ms >= 1000 ? $"{(ms / 1000.0).ToString("0.0", Inv)}s"
        : $"{ms}ms";

    /// <summary>Error percent: "—" under 0.05%, else one decimal with a % sign.</summary>
    public static string ErrorPercent(double pct) => pct < 0.05 ? "—" : pct.ToString("0.0", Inv) + "%";

    /// <summary>Build an SVG polyline path across a value series scaled into a w×h box.</summary>
    public static string Spark(IReadOnlyList<double> vals, double w, double h)
    {
        if (vals is null || vals.Count == 0) return string.Empty;
        double min = vals[0], max = vals[0];
        foreach (var v in vals) { if (v < min) min = v; if (v > max) max = v; }
        var span = max - min;
        if (span == 0) span = 1;

        var sb = new StringBuilder();
        var n = vals.Count;
        for (var i = 0; i < n; i++)
        {
            var x = n == 1 ? 0 : (double)i / (n - 1) * w;
            var y = h - 2 - (vals[i] - min) / span * (h - 4);
            sb.Append(i == 0 ? 'M' : 'L')
              .Append(x.ToString("0.#", Inv)).Append(' ')
              .Append(y.ToString("0.#", Inv)).Append(' ');
        }
        return sb.ToString().TrimEnd();
    }

    /// <summary>Area path variant (line closed to the baseline) for KPI card fills.</summary>
    public static string SparkArea(IReadOnlyList<double> vals, double w, double h)
    {
        var line = Spark(vals, w, h);
        return line.Length == 0 ? string.Empty : $"{line} L{w.ToString("0.#", Inv)} {h} L0 {h} Z";
    }
}
