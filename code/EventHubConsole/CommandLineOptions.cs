using CommandLine;
using System;
using System.Collections.Generic;
using System.Text;

namespace EventHubConsole
{
    public class CommandLineOptions
    {
        [Option('l', "source-level", Required = false, HelpText = "Set source level")]
        public string SourceLevel { get; set; } = "warning";

        [Option(
            't',
            "template-text",
            Required = true,
            HelpText = "Set the template text")]
        public string TemplateText { get; set; } = string.Empty;

        [Option(
            'r',
            "rate",
            Required = false,
            HelpText = "Set the rate (in MBs/minute)")]
        public long Rate { get; set; } = 10;

        [Option(
            'p',
            "records-per-payload",
            Required = false,
            HelpText = "Set the records per payload")]
        public int RecordsPerPayload { get; set; } = 5;

        [Option('a', "auth", Required = false, HelpText = "Set authentication method:  'AzCli' or 'System'")]
        public string Authentication { get; set; } = string.Empty;

        public override string ToString()
        {
            return $@"
Template Text:  {TemplateText}
Rate (MBs/minute):  {Rate}
Authentication:  {Authentication}
Source level:  {SourceLevel}";
        }
    }
}