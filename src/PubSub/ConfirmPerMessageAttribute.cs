namespace PubSub;

[AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct, AllowMultiple = false, Inherited = false)]
public sealed class ConfirmPerMessageAttribute : Attribute;
