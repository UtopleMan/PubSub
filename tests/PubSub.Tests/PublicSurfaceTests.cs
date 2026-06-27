using Shouldly;
using Xunit;

namespace PubSub.Tests;

public class PublicSurfaceTests
{
    [Fact]
    public void PubSubAssembly_PublicTypes_MatchExpectedSurface()
    {
        var expected = new[]
        {
            "PubSub.IPublish`1",
            "PubSub.ISubscribeTo`1",
            "PubSub.IBatchPublish`1",
            "PubSub.IFireAndForgetPublish`1",
            "PubSub.PubSubTopicAttribute",
            "PubSub.PublishTimeoutAttribute",
            "PubSub.ConsumerPrefetchAttribute",
            "PubSub.ConfirmPerMessageAttribute",
            "PubSub.BatchedPublishAttribute",
            "PubSub.FireAndForgetAttribute",
            "PubSub.PublishMode",
            "PubSub.PubSubException",
            "PubSub.PubSubPublishTimeoutException",
            "PubSub.PubSubHandlerFailedException",
            "PubSub.PubSubTopicResolver",
            "PubSub.IPubSubDispatcher`1",
            "PubSub.PubSubDispatcherRegistry",
            "PubSub.ReflectionPubSubDispatcher`1",
            "PubSub.IRoutingKeyProvider",
        };

        var actual = typeof(IPublish<>).Assembly
            .GetExportedTypes()
            .Select(t => t.FullName)
            .OrderBy(n => n)
            .ToArray();

        actual.ShouldBe(expected.OrderBy(n => n));
    }

    [Fact]
    public void IPublish_IsGenericInterface_WithOneMethod()
    {
        var t = typeof(IPublish<>);
        t.IsInterface.ShouldBeTrue();
        t.IsGenericTypeDefinition.ShouldBeTrue();
        t.GetMethods().Length.ShouldBe(1);
        t.GetMethods()[0].Name.ShouldBe(nameof(IPublish<object>.PublishAsync));
    }

    [Fact]
    public void IBatchPublish_IsGenericInterface_WithOneMethod()
    {
        var t = typeof(IBatchPublish<>);
        t.IsInterface.ShouldBeTrue();
        t.IsGenericTypeDefinition.ShouldBeTrue();
        t.GetMethods().Length.ShouldBe(1);
        t.GetMethods()[0].Name.ShouldBe(nameof(IBatchPublish<object>.PublishAsync));
    }

    [Fact]
    public void IFireAndForgetPublish_IsGenericInterface_WithOneMethod()
    {
        var t = typeof(IFireAndForgetPublish<>);
        t.IsInterface.ShouldBeTrue();
        t.IsGenericTypeDefinition.ShouldBeTrue();
        t.GetMethods().Length.ShouldBe(1);
        t.GetMethods()[0].Name.ShouldBe(nameof(IFireAndForgetPublish<object>.PublishAsync));
    }

    [Fact]
    public void ISubscribeTo_IsGenericInterface_WithOneMethod()
    {
        var t = typeof(ISubscribeTo<>);
        t.IsInterface.ShouldBeTrue();
        t.IsGenericTypeDefinition.ShouldBeTrue();
        t.GetMethods().Length.ShouldBe(1);
        t.GetMethods()[0].Name.ShouldBe("Handle");
    }
}
