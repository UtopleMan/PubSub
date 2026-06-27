using Shouldly;
using Xunit;

namespace PubSub.Tests;

public class PubSubTopicResolverTests
{
    [Fact]
    public void Resolve_Annotated_ReturnsAttribute()
    {
        var topic = PubSubTopicResolver.Resolve<Annotated>();
        topic.RoutingKey.ShouldBe("test.routing-key");
        topic.Exchange.ShouldBe("phoenix.events");
    }

    [Fact]
    public void Resolve_AnnotatedWithCustomExchange_HonoursOverride()
    {
        var topic = PubSubTopicResolver.Resolve<AnnotatedCustomExchange>();
        topic.RoutingKey.ShouldBe("test.custom");
        topic.Exchange.ShouldBe("custom.exchange");
    }

    [Fact]
    public void Resolve_Unannotated_Throws_WithUsefulMessage()
    {
        var ex = Should.Throw<InvalidOperationException>(() => PubSubTopicResolver.Resolve<Unannotated>());
        ex.Message.ShouldContain(nameof(Unannotated));
        ex.Message.ShouldContain("[PubSubTopic]");
    }

    [Fact]
    public void Resolve_Cached_ReturnsSameInstance()
    {
        var a = PubSubTopicResolver.Resolve<Annotated>();
        var b = PubSubTopicResolver.Resolve<Annotated>();
        b.ShouldBeSameAs(a);
    }

    [Fact]
    public void ResolvePublishTimeout_NoAttribute_ReturnsDefault()
    {
        PubSubTopicResolver.ResolvePublishTimeout<Annotated>(5).ShouldBe(TimeSpan.FromSeconds(5));
    }

    [Fact]
    public void ResolvePublishTimeout_AttributePresent_ReturnsAttributeValue()
    {
        PubSubTopicResolver.ResolvePublishTimeout<AnnotatedWithTimeout>().ShouldBe(TimeSpan.FromSeconds(30));
    }

    [Fact]
    public void ResolveConsumerPrefetch_NoAttribute_ReturnsDefault()
    {
        PubSubTopicResolver.ResolveConsumerPrefetch(typeof(ConsumerNoAttr), defaultCount: 75).ShouldBe<ushort>(75);
    }

    [Fact]
    public void ResolveConsumerPrefetch_AttributePresent_ReturnsAttributeValue()
    {
        PubSubTopicResolver.ResolveConsumerPrefetch(typeof(ConsumerWithPrefetch)).ShouldBe<ushort>(10);
    }

    [PubSubTopic("test.routing-key")]
    private sealed record Annotated;

    [PubSubTopic("test.custom", Exchange = "custom.exchange")]
    private sealed record AnnotatedCustomExchange;

    [PubSubTopic("test.timeout")]
    [PublishTimeout(30)]
    private sealed record AnnotatedWithTimeout;

    private sealed record Unannotated;

    private sealed class ConsumerNoAttr : ISubscribeTo<Annotated>
    {
        public Task Handle(Annotated message, CancellationToken cancellationToken) => Task.CompletedTask;
    }

    [ConsumerPrefetch(10)]
    private sealed class ConsumerWithPrefetch : ISubscribeTo<Annotated>
    {
        public Task Handle(Annotated message, CancellationToken cancellationToken) => Task.CompletedTask;
    }
}
