namespace PubSub;

/// <summary>Opts a contract into <see cref="PublishMode.ConfirmPerMessage"/>. This is also the default when no publish-mode attribute is present.</summary>
[AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct, AllowMultiple = false, Inherited = false)]
public sealed class ConfirmPerMessageAttribute : Attribute;
