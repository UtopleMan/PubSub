using BenchmarkDotNet.Running;

namespace PubSub.RabbitMQ.Bench;

internal static class Program
{
    public static async Task Main(string[] args)
    {
        if (args.Length > 0 && args[0] == "diagnose-wolverine")
        {
            await WolverineRoutingDiagnostic.RunAsync();
            return;
        }
        BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);
    }
}
