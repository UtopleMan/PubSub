namespace PubSub;

public abstract class PubSubException(string message, Exception? innerException = null)
    : Exception(message, innerException);
