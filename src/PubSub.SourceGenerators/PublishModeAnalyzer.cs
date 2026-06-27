using System.Collections.Immutable;
using System.Linq;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.Diagnostics;

namespace PubSub.SourceGenerators;

[DiagnosticAnalyzer(LanguageNames.CSharp)]
public sealed class PublishModeAnalyzer : DiagnosticAnalyzer
{
    public const string DiagnosticId = "PUBSUB002";

    private static readonly DiagnosticDescriptor Rule = new(
        id: DiagnosticId,
        title: "Multiple PubSub mode attributes on one type",
        messageFormat: "Type '{0}' carries more than one of [ConfirmPerMessage], [BatchedPublish], [FireAndForget]. Pick exactly one.",
        category: "PubSub",
        defaultSeverity: DiagnosticSeverity.Error,
        isEnabledByDefault: true,
        description: "[ConfirmPerMessage], [BatchedPublish], and [FireAndForget] are mutually exclusive — each contract picks exactly one publish mode.");

    public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics { get; } = ImmutableArray.Create(Rule);

    public override void Initialize(AnalysisContext context)
    {
        context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.None);
        context.EnableConcurrentExecution();
        context.RegisterSymbolAction(AnalyzeNamedType, SymbolKind.NamedType);
    }

    private static void AnalyzeNamedType(SymbolAnalysisContext context)
    {
        var symbol = (INamedTypeSymbol)context.Symbol;
        var attrs = symbol.GetAttributes();
        var modes = 0;
        foreach (var a in attrs)
        {
            var name = a.AttributeClass?.Name;
            if (name is "ConfirmPerMessageAttribute" or "BatchedPublishAttribute" or "FireAndForgetAttribute"
                && a.AttributeClass!.ContainingNamespace?.ToDisplayString() == "PubSub")
            {
                modes++;
            }
        }
        if (modes > 1)
        {
            context.ReportDiagnostic(Diagnostic.Create(
                Rule,
                symbol.Locations.FirstOrDefault(),
                symbol.ToDisplayString()));
        }
    }
}
