using CommandLine;
using Kusto.Data.Common;
using System;
using System.Collections.Generic;
using System.Text;

namespace StreamingApiConsole
{
    public class CommandLineOptions
    {
        [Option('l', "source-level", Required = false, HelpText = "Set source level")]
        public string SourceLevel { get; set; } = "warning";

        [Option('a', "auth", Required = false, HelpText = "Set authentication method:  'AzCli' or 'System'")]
        public string Authentication { get; set; } = string.Empty;

        [Option(
            'd',
            "db-uri",
            Required = true,
            HelpText = "Set the db URI (this is for templates), e.g. https://mycluster.westus.kusto.windows.net/mydb/")]
        public string DbUri { get; set; } = string.Empty;

        [Option(
            't',
            "template-name",
            Required = true,
            HelpText = "Set the template name")]
        public string TemplateName { get; set; } = string.Empty;

        [Option(
            "table-uri",
            Required = true,
            HelpText = "Set the table URI (for ingestion), e.g. https://mycluster.westus.kusto.windows.net/mydb/mytable")]
        public string TableUri { get; set; } = string.Empty;

        [Option(
            "throughput-target",
            Required = false,
            HelpText = "Set the throughput target, in MBs/minute")]
        public double TargetThroughput { get; set; } = 10;

        [Option(
            'f',
            "data-format",
            Required = false,
            HelpText = "Ingestion data format")]
        public DataSourceFormat DataFormat { get; set; } = DataSourceFormat.multijson;

        [Option(
            'm',
            "data-mapping",
            Required = false,
            HelpText = "Data mapping reference name")]
        public string DataMapping { get; set; } = string.Empty;

        [Option(
            'c',
            "compression",
            Required = false,
            HelpText = "Set the output compression (true / false)")]
        public bool IsOutputCompressed { get; set; } = false;

        public override string ToString()
        {
            return $@"
DbUri:  {DbUri}
Template Name:  {TemplateName}
TableUri:  {TableUri}
TargetThroughput (in MBs/minute):  {TargetThroughput}
IsOutputCompressed:  {IsOutputCompressed}
Authentication:  {Authentication}
Source level:  {SourceLevel}";
        }
    }
}