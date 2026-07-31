namespace PubSub;

/// <summary>Base type for all exceptions raised by PubSub.</summary>
public abstract class PubSubException(string message, Exception? innerException = null)
    : Exception(message, innerException);
