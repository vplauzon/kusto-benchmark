using CommandLine;
using CommandLine.Text;
using Kusto.Data.DataProvider;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BenchmarkLib
{
    public static class ProgramHelper
    {
        public static async Task<int> RunAsync<T>(string[] args, Func<T, Task> runOptionsAsync)
        {
            CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo("en-US");
            CultureInfo.CurrentUICulture = CultureInfo.GetCultureInfo("en-US");

            try
            {
                //  Use CommandLineParser NuGet package to parse command line
                //  See https://github.com/commandlineparser/commandline
                var parser = new Parser(with =>
                {
                    with.HelpWriter = null;
                });
                var options = parser.ParseArguments<T>(args);

                await options
                    .WithNotParsed(errors => HandleParseError(options, errors))
                    .WithParsedAsync(runOptionsAsync);

                return options.Tag == ParserResultType.Parsed
                    ? 0
                    : 1;
            }
            catch (Exception ex)
            {
                ProgramHelper.DisplayGenericException(ex);

                return 1;
            }
        }

        public static void EnsureTraceLevel(string sourceLevelText)
        {
            var sourceLevel = ProgramHelper.ParseSourceLevel(sourceLevelText);

            //  Ensure traces go to console even in a Docker container
            Trace.Listeners.Add(new ConsoleTraceListener
            {
                Filter = new EventTypeFilter(sourceLevel)
            });
        }

        private static void HandleParseError<T>(ParserResult<T> result, IEnumerable<Error> errors)
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

    }
}