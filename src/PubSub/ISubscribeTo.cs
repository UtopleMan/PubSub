namespace PubSub;

public interface ISubscribeTo<in T>
{
    Task Handle(T message, CancellationToken cancellationToken);
}
