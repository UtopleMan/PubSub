using PubSub;

namespace PubSub.SourceGenerators.Tests;

public enum RunKind
{
    None = 0,
    Weekend = 1,
    Daily = 2,
}

[PubSubTopic("tests.long-default")]
public sealed record LongDefaultContract(string Iso3, long CountryId = 0);

[PubSubTopic("tests.numeric-defaults")]
public sealed record NumericDefaultsContract(
    double Ratio = 0.5,
    float Weight = 1.5f,
    uint Count = 0,
    short Tick = 0,
    byte Flag = 0);

[PubSubTopic("tests.enum-default")]
public sealed record EnumDefaultContract(string Iso3, RunKind RunKind = RunKind.Weekend);

[PubSubTopic("tests.no-defaults")]
public sealed record NoDefaultsContract(string Iso3, long CountryId);
