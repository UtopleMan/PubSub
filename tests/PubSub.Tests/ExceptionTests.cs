using Shouldly;
using Xunit;

namespace PubSub.Tests;

public class ExceptionTests
{
    [Fact]
    public void PublishTimeout_FormatsMessage_WithTopicAndTimings()
    {
        var ex = new PubSubPublishTimeoutException("a.b.c", TimeSpan.FromSeconds(10), TimeSpan.FromMilliseconds(10_500));
        ex.Topic.ShouldBe("a.b.c");
        ex.Timeout.ShouldBe(TimeSpan.FromSeconds(10));
        ex.Elapsed.ShouldBe(TimeSpan.FromMilliseconds(10_500));
        ex.Message.ShouldContain("a.b.c");
        ex.Message.ShouldContain("10000");
        ex.Message.ShouldContain("10500");
    }

    [Fact]
    public void HandlerFailed_WrapsInner_AndCarriesTopicHandler()
    {
        var inner = new InvalidOperationException("boom");
        var ex = new PubSubHandlerFailedException("a.b.c", typeof(string), inner);
        ex.InnerException.ShouldBeSameAs(inner);
        ex.Topic.ShouldBe("a.b.c");
        ex.HandlerType.ShouldBe(typeof(string));
        ex.Message.ShouldContain("a.b.c");
        ex.Message.ShouldContain("boom");
    }

    [Fact]
    public void BothConcreteExceptions_DeriveFromPubSubException()
    {
        typeof(PubSubPublishTimeoutException).BaseType.ShouldBe(typeof(PubSubException));
        typeof(PubSubHandlerFailedException).BaseType.ShouldBe(typeof(PubSubException));
    }
}
