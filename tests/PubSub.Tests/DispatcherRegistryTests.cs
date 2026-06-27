using Shouldly;
using Xunit;

namespace PubSub.Tests;

public class DispatcherRegistryTests
{
    [Fact]
    public void GetOrFallback_NoRegistration_ReturnsReflectionFallback()
    {
        var d = PubSubDispatcherRegistry.GetOrFallback<UnregisteredContract>();
        d.ShouldBeOfType<ReflectionPubSubDispatcher<UnregisteredContract>>();
        PubSubDispatcherRegistry.HasGeneratedDispatcher<UnregisteredContract>().ShouldBeFalse();
    }

    [Fact]
    public void Register_ThenGetOrFallback_ReturnsCustomDispatcher()
    {
        var custom = new CustomDispatcher();
        PubSubDispatcherRegistry.Register<RegisteredContract>(custom);
        PubSubDispatcherRegistry.GetOrFallback<RegisteredContract>().ShouldBeSameAs(custom);
        PubSubDispatcherRegistry.HasGeneratedDispatcher<RegisteredContract>().ShouldBeTrue();
    }

    [Fact]
    public void ReflectionDispatcher_RoundTrip()
    {
        var d = new ReflectionPubSubDispatcher<UnregisteredContract>();
        var original = new UnregisteredContract(42, "hello");
        var body = d.Serialize(original);
        body.Length.ShouldBeGreaterThan(0);
        var decoded = d.Deserialize(body);
        decoded.Id.ShouldBe(42);
        decoded.Note.ShouldBe("hello");
    }

    public sealed record UnregisteredContract(int Id, string Note);
    private sealed record RegisteredContract;

    private sealed class CustomDispatcher : IPubSubDispatcher<RegisteredContract>
    {
        public byte[] Serialize(RegisteredContract message) => new byte[] { 1 };
        public RegisteredContract Deserialize(ReadOnlySpan<byte> body) => new();
        public Task InvokeAsync(ISubscribeTo<RegisteredContract> handler, RegisteredContract message, CancellationToken cancellationToken)
            => handler.Handle(message, cancellationToken);
    }
}
