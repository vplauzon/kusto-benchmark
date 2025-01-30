using BenchmarkLib;
using System.Diagnostics;

namespace EventHubConsole
{
    internal class Program
    {
        public static string AssemblyVersion
        {
            get
            {
                var version = typeof(Program).Assembly.GetName().Version;
                var versionText = version == null
                    ? "<VERSION MISSING>"
                    : version.ToString();

                return versionText;
            }
        }

        internal static async Task<int> Main(string[] args)
        {
            Console.WriteLine();
            Console.WriteLine($"Kusto Query Console {AssemblyVersion}");
            Console.WriteLine();
            Console.WriteLine($"Command line:  {string.Join(" ", args)}");
            Console.Out.Flush();

            return await ProgramHelper.RunAsync<CommandLineOptions>(args, RunOptionsAsync);
        }

        private static async Task RunOptionsAsync(CommandLineOptions options)
        {
            var cancellationTokenSource = new CancellationTokenSource();
            var taskCompletionSource = new TaskCompletionSource();

            ProgramHelper.EnsureTraceLevel(options.SourceLevel);
            AppDomain.CurrentDomain.ProcessExit += (e, s) =>
            {
                Trace.TraceInformation("Exiting process...");
                cancellationTokenSource.Cancel();
                taskCompletionSource.Task.Wait();
            };
            try
            {
                Trace.WriteLine("");
                Trace.WriteLine("Parameterization:");
                Trace.WriteLine("");
                Trace.WriteLine(options.ToString());
                Trace.WriteLine("");
                await using (var orchestration = await EventHubOrchestration.CreateAsync(
                    options,
                    cancellationTokenSource.Token))
                {
                    Trace.WriteLine("Processing...");
                    Trace.WriteLine("");
                    await orchestration.ProcessAsync(cancellationTokenSource.Token);
                }
            }
            finally
            {
                taskCompletionSource.SetResult();
            }
        }
    }
}