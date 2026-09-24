using System.Globalization;
using PubSub;
using Shouldly;
using Xunit;

namespace PubSub.SourceGenerators.Tests;

/// <summary>
/// <c>JsonParameterInfoValues.DefaultValue</c> is an <c>object</c>, so the emitted literal must carry the
/// parameter's own type. A bare <c>0</c> for a <c>long</c> parameter boxes an <see cref="int"/>, and
/// <c>JsonParameterInfo&lt;long&gt;</c>'s constructor throws <see cref="InvalidCastException"/> the first
/// time the contract is serialized.
/// </summary>
public class GeneratedDispatcherDefaultValueTests
{
    [Fact]
    public void LongParameterWithDefault_RoundTripsThroughGeneratedDispatcher()
    {
        PubSubDispatcherRegistry.HasGeneratedDispatcher<LongDefaultContract>().ShouldBeTrue();

        var dispatcher = PubSubDispatcherRegistry.GetOrFallback<LongDefaultContract>();

        var decoded = dispatcher.Deserialize(dispatcher.Serialize(new LongDefaultContract("USA", 42)));

        decoded.CountryId.ShouldBe(42);
    }

    [Fact]
    public void OmittedLongParameter_FallsBackToItsDeclaredDefault()
    {
        var dispatcher = PubSubDispatcherRegistry.GetOrFallback<LongDefaultContract>();

        var decoded = dispatcher.Deserialize("""{"iso3":"USA"}"""u8);

        decoded.CountryId.ShouldBe(0);
    }

    [Fact]
    public void NonIntegerNumericParametersWithDefaults_RoundTripThroughGeneratedDispatcher()
    {
        PubSubDispatcherRegistry.HasGeneratedDispatcher<NumericDefaultsContract>().ShouldBeTrue();

        var dispatcher = PubSubDispatcherRegistry.GetOrFallback<NumericDefaultsContract>();

        var decoded = dispatcher.Deserialize(dispatcher.Serialize(new NumericDefaultsContract(1.5d, 2.5f, 3, 4, 5)));

        decoded.Ratio.ShouldBe(1.5d);
        decoded.Weight.ShouldBe(2.5f);
        decoded.Count.ShouldBe(3u);
        decoded.Tick.ShouldBe((short)4);
        decoded.Flag.ShouldBe((byte)5);
    }

    [Fact]
    public void OmittedNonIntegerNumericParameters_FallBackToTheirDeclaredDefaults()
    {
        var dispatcher = PubSubDispatcherRegistry.GetOrFallback<NumericDefaultsContract>();

        var decoded = dispatcher.Deserialize("""{"count":3}"""u8);

        decoded.Ratio.ShouldBe(0.5d);
        decoded.Weight.ShouldBe(1.5f);
    }

    [Fact]
    public void EnumParameterWithDefault_RoundTripsThroughGeneratedDispatcher()
    {
        PubSubDispatcherRegistry.HasGeneratedDispatcher<EnumDefaultContract>().ShouldBeTrue();

        var dispatcher = PubSubDispatcherRegistry.GetOrFallback<EnumDefaultContract>();

        var decoded = dispatcher.Deserialize(dispatcher.Serialize(new EnumDefaultContract("USA", RunKind.Daily)));

        decoded.RunKind.ShouldBe(RunKind.Daily);
    }

    [Fact]
    public void OmittedEnumParameter_FallsBackToItsDeclaredDefault()
    {
        var dispatcher = PubSubDispatcherRegistry.GetOrFallback<EnumDefaultContract>();

        var decoded = dispatcher.Deserialize("""{"iso3":"USA"}"""u8);

        decoded.RunKind.ShouldBe(RunKind.Weekend);
    }

    [Fact]
    public void ParametersWithoutDefaults_StillRoundTrip()
    {
        var dispatcher = PubSubDispatcherRegistry.GetOrFallback<NoDefaultsContract>();

        var decoded = dispatcher.Deserialize(dispatcher.Serialize(new NoDefaultsContract("USA", 7)));

        decoded.CountryId.ShouldBe(7);
    }

    [Fact]
    public void NullableReferenceParameters_RoundTripThroughGeneratedDispatcher()
    {
        PubSubDispatcherRegistry.HasGeneratedDispatcher<NullableReferenceContract>().ShouldBeTrue();

        var dispatcher = PubSubDispatcherRegistry.GetOrFallback<NullableReferenceContract>();

        var decoded = dispatcher.Deserialize(
            dispatcher.Serialize(new NullableReferenceContract("USA", ["ema8", "ema20"], "note")));

        decoded.FactorIds.ShouldBe(["ema8", "ema20"]);
        decoded.Note.ShouldBe("note");
    }

    [Fact]
    public void OmittedNullableReferenceParameters_FallBackToNull()
    {
        var dispatcher = PubSubDispatcherRegistry.GetOrFallback<NullableReferenceContract>();

        var decoded = dispatcher.Deserialize("""{"iso3":"USA"}"""u8);

        decoded.FactorIds.ShouldBeNull();
        decoded.Note.ShouldBeNull();
    }

    /// <summary>
    /// The literal is emitted into C# source, so it must be formatted invariantly — under a comma-decimal
    /// culture <c>0.5.ToString()</c> yields <c>0,5</c>, which is a different literal (and not valid in that
    /// position). The generator runs at build time, so this asserts the shipped metadata, not the runtime
    /// culture.
    /// </summary>
    [Fact]
    public void FractionalDefault_SurvivesAsAnInvariantLiteral()
    {
        var original = CultureInfo.CurrentCulture;
        CultureInfo.CurrentCulture = new CultureInfo("da-DK");
        try
        {
            var dispatcher = PubSubDispatcherRegistry.GetOrFallback<NumericDefaultsContract>();

            var decoded = dispatcher.Deserialize("""{"count":3}"""u8);

            decoded.Ratio.ShouldBe(0.5d);
        }
        finally
        {
            CultureInfo.CurrentCulture = original;
        }
    }
}
