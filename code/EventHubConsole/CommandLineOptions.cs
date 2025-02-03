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

        [Option('a', "auth", Required = false, HelpText = "Set authentication method:  'AzCli' or 'System'")]
        public string Authentication { get; set; } = string.Empty;

        [Option('f', "fqdn", Required = false, HelpText = "Set the fully qualified domain name (FQDN)")]
        public string Fqdn { get; set; } = string.Empty;

        [Option('e', "event-hub", Required = false, HelpText = "Set the event hub")]
        public string EventHub { get; set; } = string.Empty;

        [Option(
            'd',
            "db-uri",
            Required = true,
            HelpText = "Set the db URI, e.g. https://mycluster.westus.kusto.windows.net/mydb/")]
        public string DbUri { get; set; } = string.Empty;

        [Option(
            't',
            "template-name",
            Required = true,
            HelpText = "Set the template name")]
        public string TemplateName { get; set; } = string.Empty;

        [Option(
            'r',
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

        [Option(
            "throughput-target",
            Required = false,
            HelpText = "Set the throughput target, in MBs/minute")]
        public int TargetThroughput { get; set; } = 100;

        [Option(
            'c',
            "compression",
            Required = false,
            HelpText = "Set the output compression (true / false)")]
        public bool? IsOutputCompressed { get; set; } = true;

        public override string ToString()
        {
            return $@"
Fqdn:  {Fqdn}
EventHub:  {EventHub}
DbUri:  {DbUri}
Template Name:  {TemplateName}
RecordsPerPayload:  {RecordsPerPayload}
BatchSize:  {BatchSize}
TargetThroughput (in MBs/minute):  {TargetThroughput}
IsOutputCompressed:  {IsOutputCompressed}
Authentication:  {Authentication}
Source level:  {SourceLevel}";
        }
    }
}