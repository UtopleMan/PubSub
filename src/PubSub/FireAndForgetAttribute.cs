namespace PubSub;

/// <summary>Opts a contract into <see cref="PublishMode.FireAndForget"/> publishing (no delivery confirm).</summary>
[AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct, AllowMultiple = false, Inherited = false)]
public sealed class FireAndForgetAttribute : Attribute;
