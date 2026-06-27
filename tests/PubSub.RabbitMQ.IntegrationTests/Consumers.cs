using System.Collections.Concurrent;

namespace PubSub.RabbitMQ.IntegrationTests;

public sealed class MessageRecorder
{
    public ConcurrentBag<AlphaMessage> Alpha { get; } = new();
    public ConcurrentBag<BetaMessage> Beta { get; } = new();
    public ConcurrentBag<GammaMessage> Gamma { get; } = new();
    public ConcurrentBag<SlowMessage> Slow { get; } = new();
    public ConcurrentBag<FailingMessage> Fail { get; } = new();
    public TaskCompletionSource SlowStarted { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    public TaskCompletionSource ReleaseSlow { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
}

public sealed class AlphaConsumer(MessageRecorder recorder) : PubSub.ISubscribeTo<AlphaMessage>
{
    public Task Handle(AlphaMessage message, CancellationToken cancellationToken)
    {
        recorder.Alpha.Add(message);
        return Task.CompletedTask;
    }
}

public sealed class BetaConsumer(MessageRecorder recorder) : PubSub.ISubscribeTo<BetaMessage>
{
    public Task Handle(BetaMessage message, CancellationToken cancellationToken)
    {
        recorder.Beta.Add(message);
        return Task.CompletedTask;
    }
}

public sealed class GammaConsumer(MessageRecorder recorder) : PubSub.ISubscribeTo<GammaMessage>
{
    public Task Handle(GammaMessage message, CancellationToken cancellationToken)
    {
        recorder.Gamma.Add(message);
        return Task.CompletedTask;
    }
}

public sealed class SlowConsumer(MessageRecorder recorder) : PubSub.ISubscribeTo<SlowMessage>
{
    public async Task Handle(SlowMessage message, CancellationToken cancellationToken)
    {
        recorder.Slow.Add(message);
        recorder.SlowStarted.TrySetResult();
        await recorder.ReleaseSlow.Task;
    }
}

public sealed class FailingConsumer(MessageRecorder recorder) : PubSub.ISubscribeTo<FailingMessage>
{
    public Task Handle(FailingMessage message, CancellationToken cancellationToken)
    {
        recorder.Fail.Add(message);
        throw new InvalidOperationException($"KMF lookup failed for {message.Symbol}");
    }
}
