using System.Collections.Immutable;
using System.Linq;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.Diagnostics;

namespace PubSub.SourceGenerators;

[DiagnosticAnalyzer(LanguageNames.CSharp)]
public sealed class PubSubTopicAnalyzer : DiagnosticAnalyzer
{
    public const string DiagnosticId = "PUBSUB001";

    private static readonly DiagnosticDescriptor Rule = new(
        id: DiagnosticId,
        title: "Message contract missing [PubSubTopic]",
        messageFormat: "Type '{0}' is used as a PubSub message but does not carry [PubSubTopic]. Add `[PubSubTopic(\"your.routing.key\")]` on the contract record.",
        category: "PubSub",
        defaultSeverity: DiagnosticSeverity.Error,
        isEnabledByDefault: true,
        description: "Every type passed to PubSubRabbitMqBuilder.Publish<T>() or Subscribe<T, TConsumer>() must declare its routing-key via [PubSubTopic].");

    public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics { get; } = ImmutableArray.Create(Rule);

    public override void Initialize(AnalysisContext context)
    {
        context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.None);
        context.EnableConcurrentExecution();
        context.RegisterOperationAction(AnalyzeInvocation, OperationKind.Invocation);
    }

    private static void AnalyzeInvocation(OperationAnalysisContext context)
    {
        if (context.Operation is not Microsoft.CodeAnalysis.Operations.IInvocationOperation invocation) return;
        var method = invocation.TargetMethod;
        var containing = method.ContainingType?.Name;
        if (containing is null || !containing.EndsWith("Builder")) return;
        if (method.Name != "Publish" && method.Name != "Subscribe") return;
        if (method.TypeArguments.IsDefaultOrEmpty) return;

        var messageType = method.TypeArguments[0];
        if (messageType is not INamedTypeSymbol named) return;

        var hasTopic = named.GetAttributes().Any(a =>
            a.AttributeClass?.Name == "PubSubTopicAttribute"
            && a.AttributeClass.ContainingNamespace?.ToDisplayString() == "PubSub");

        if (!hasTopic)
        {
            context.ReportDiagnostic(Diagnostic.Create(
                Rule,
                invocation.Syntax.GetLocation(),
                named.ToDisplayString()));
        }
    }
}
