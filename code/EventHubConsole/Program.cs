using Azure.Core.Diagnostics;
using BenchmarkLib;
using System.Diagnostics;
using System.Diagnostics.Tracing;

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
            using var listener = CreateAzureEventSourceListener();

            Console.WriteLine();
            Console.WriteLine($"Kusto Event Hub Console {AssemblyVersion}");
            Console.WriteLine();
            Console.WriteLine($"Command line:  {string.Join(" ", args)}");
            Console.Out.Flush();

            try
            {
                return await ProgramHelper.RunAsync<CommandLineOptions>(args, RunOptionsAsync);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Exception:  {ex.Message}");

                return -1;
            }
            finally
            {
                Console.WriteLine("Exiting...");
            }
        }

        private static AzureEventSourceListener CreateAzureEventSourceListener()
        {
            var listener = new AzureEventSourceListener(
                (eventArgs, message) =>
                {
                    Console.WriteLine($"Warning: {message}");
                },
                // Only log warnings and above
                EventLevel.Warning);

            return listener;
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