using BenchmarkLib;
using CommandLine;
using CommandLine.Text;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Threading.Tasks;

// See https://aka.ms/new-console-template for more information

namespace IngestorConsole
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
            Console.WriteLine($"Kusto Ingestor Console {AssemblyVersion}");
            Console.WriteLine();
            Console.WriteLine($"Command line:  {string.Join(" ", args)}");

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
                await using (var orchestration = await IngestorOrchestration.CreateAsync(
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