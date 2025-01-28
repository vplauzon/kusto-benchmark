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

        [Option('f', "fqdn", Required = false, HelpText = "Set the fully qualified domain name (FQDN)")]
        public string Fqdn { get; set; } = string.Empty;

        [Option('e', "event-hub", Required = false, HelpText = "Set the event hub")]
        public string EventHub { get; set; } = string.Empty;

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
            "records-per-payload",
            Required = false,
            HelpText = "Set the records per payload")]
        public int RecordsPerPayload { get; set; } = 5;

        [Option(
            'b',
            "batch-size",
            Required = false,
            HelpText = "Set the batch size")]
        public int BatchSize { get; set; } = 5;

        [Option(
            'p',
            "parallel-partitions",
            Required = false,
            HelpText = "Set the number of parallel partitions written to")]
        public int ParallelPartitions { get; set; } = 1;

        [Option('a', "auth", Required = false, HelpText = "Set authentication method:  'AzCli' or 'System'")]
        public string Authentication { get; set; } = string.Empty;

        public override string ToString()
        {
            return $@"
Fqdn:  {Fqdn}
EventHub:  {EventHub}
Template Text:  {TemplateText}
Rate (MBs/minute):  {Rate}
RecordsPerPayload:  {RecordsPerPayload}
BatchSize:  {BatchSize}
Authentication:  {Authentication}
Source level:  {SourceLevel}";
        }
    }
}