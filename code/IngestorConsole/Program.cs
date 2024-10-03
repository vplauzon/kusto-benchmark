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
            CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo("en-US");
            CultureInfo.CurrentUICulture = CultureInfo.GetCultureInfo("en-US");

            Console.WriteLine();
            Console.WriteLine($"Kusto Ingestor Console {AssemblyVersion}");
            Console.WriteLine();
            Console.WriteLine($"Command line:  {string.Join(" ", args)}");

            try
            {
                //  Use CommandLineParser NuGet package to parse command line
                //  See https://github.com/commandlineparser/commandline
                var parser = new Parser(with =>
                {
                    with.HelpWriter = null;
                });
                var options = parser.ParseArguments<CommandLineOptions>(args);

                await options
                    .WithNotParsed(errors => HandleParseError(options, errors))
                    .WithParsedAsync(RunOptionsAsync);

                return options.Tag == ParserResultType.Parsed
                    ? 0
                    : 1;
            }
            catch (Exception ex)
            {
                DisplayGenericException(ex);

                return 1;
            }
        }

        private static void DisplayGenericException(Exception ex, string tab = "")
        {
            Console.Error.WriteLine(
                $"{tab}Exception encountered:  {ex.GetType().FullName} ; {ex.Message}");
            Console.Error.WriteLine($"{tab}Stack trace:  {ex.StackTrace}");
            if (ex.InnerException != null)
            {
                DisplayGenericException(ex.InnerException, tab + "  ");
            }
        }

        private static async Task RunOptionsAsync(CommandLineOptions options)
        {
            var cancellationTokenSource = new CancellationTokenSource();
            var taskCompletionSource = new TaskCompletionSource();
            var sourceLevel = ParseSourceLevel(options.SourceLevel);

            //  Ensure traces go to console even in a Docker container
            Trace.Listeners.Add(new ConsoleTraceListener
            {
                Filter = new EventTypeFilter(sourceLevel)
            });
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

        private static SourceLevels ParseSourceLevel(string sourceLevel)
        {
            if (Enum.TryParse<SourceLevels>(sourceLevel, true, out var level))
            {
                return level;
            }
            else
            {
                throw new FormatException($"Can't parse source level '{sourceLevel}'");
            }
        }

        private static void HandleParseError(
            ParserResult<CommandLineOptions> result,
            IEnumerable<Error> errors)
        {
            var helpText = HelpText.AutoBuild(result, h =>
            {
                h.AutoVersion = false;
                h.Copyright = string.Empty;
                h.Heading = string.Empty;

                return HelpText.DefaultParsingErrorsHandler(result, h);
            }, example => example);

            Console.WriteLine(helpText);
        }
    }
}