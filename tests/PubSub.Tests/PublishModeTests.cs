using Shouldly;
using Xunit;

namespace PubSub.Tests;

public class PublishModeTests
{
    [Fact]
    public void ResolvePublishMode_NoAttribute_ReturnsConfirmPerMessage()
    {
        PubSubTopicResolver.ResolvePublishMode<DefaultContract>().ShouldBe(PublishMode.ConfirmPerMessage);
    }

    [Fact]
    public void ResolvePublishMode_ConfirmPerMessage_ReturnsConfirmPerMessage()
    {
        PubSubTopicResolver.ResolvePublishMode<ExplicitConfirm>().ShouldBe(PublishMode.ConfirmPerMessage);
    }

    [Fact]
    public void ResolvePublishMode_Batched_ReturnsBatched()
    {
        PubSubTopicResolver.ResolvePublishMode<BatchedContract>().ShouldBe(PublishMode.Batched);
    }

    [Fact]
    public void ResolvePublishMode_FireAndForget_ReturnsFireAndForget()
    {
        PubSubTopicResolver.ResolvePublishMode<FireAndForgetContract>().ShouldBe(PublishMode.FireAndForget);
    }

    [Fact]
    public void ResolvePublishMode_MultipleModes_Throws()
    {
        var ex = Should.Throw<InvalidOperationException>(() => PubSubTopicResolver.ResolvePublishMode<ConflictingModes>());
        ex.Message.ShouldContain(nameof(ConflictingModes));
    }

    [Fact]
    public void ResolveBatchedSettings_ReturnsAttribute()
    {
        var settings = PubSubTopicResolver.ResolveBatchedSettings<BatchedContract>();
        settings.BatchSize.ShouldBe(100);
        settings.FlushIntervalMs.ShouldBe(25);
    }

    [PubSubTopic("default.contract")]
    private sealed record DefaultContract;

    [PubSubTopic("explicit.confirm")]
    [ConfirmPerMessage]
    private sealed record ExplicitConfirm;

    [PubSubTopic("batched.contract")]
    [BatchedPublish(100, FlushIntervalMs = 25)]
    private sealed record BatchedContract;

    [PubSubTopic("fnf.contract")]
    [FireAndForget]
    private sealed record FireAndForgetContract;

    [PubSubTopic("conflicting.modes")]
    [BatchedPublish(50)]
    [FireAndForget]
    private sealed record ConflictingModes;
}
