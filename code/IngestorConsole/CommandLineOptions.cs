using CommandLine;
using System;
using System.Collections.Generic;
using System.Text;

namespace IngestorConsole
{
    public class CommandLineOptions
    {
        [Option('l', "source-level", Required = false, HelpText = "Set source level")]
        public string SourceLevel { get; set; } = "warning";

        [Option(
            'd',
            "db-uri",
            Required = true,
            HelpText = "Set the db URI, e.g. https://mycluster.westus.kusto.windows.net/mydb/")]
        public string DbUri { get; set; } = string.Empty;

        [Option(
            'i',
            "ingestion-table",
            Required = true,
            HelpText = "Set the ingestion table")]
        public string IngestionTable { get; set; } = string.Empty;

        [Option(
            'm',
            "ingestion-mapping",
            Required = false,
            HelpText = "Set the ingestion mapping")]
        public string IngestionMapping { get; set; } = string.Empty;

        [Option(
            'f',
            "ingestion-format",
            Required = false,
            HelpText = "Set the ingestion format")]
        public string IngestionFormat { get; set; } = "multijson";

        [Option(
            't',
            "template-name",
            Required = true,
            HelpText = "Set the template name")]
        public string TemplateName { get; set; } = string.Empty;

        [Option(
            's',
            "blob-size",
            Required = false,
            HelpText = "Set the blob size, in compressed MBs")]
        public int BlobSize { get; set; } = 10;

        [Option(
            'p',
            "parallel-streams",
            Required = false,
            HelpText = "Set the number of parallel streams (blobs) sending to Kusto")]
        public int ParallelStreams { get; set; } = 1;

        [Option('a', "auth", Required = false, HelpText = "Set authentication method:  'AzCli' or 'System'")]
        public string Authentication { get; set; } = string.Empty;

        public override string ToString()
        {
            return $@"
DB Uri:  {DbUri}
Ingestion table:  {IngestionTable}
Template name:  {TemplateName}
Authentication:  {Authentication}
Source level:  {SourceLevel}";
        }
    }
}