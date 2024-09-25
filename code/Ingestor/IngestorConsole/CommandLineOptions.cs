using CommandLine;
using System;
using System.Collections.Generic;
using System.Text;

namespace IngestorConsole
{
    public class CommandLineOptions
    {
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
            't',
            "template-table",
            Required = true,
            HelpText = "Set the template table")]
        public string TemplateTable { get; set; } = string.Empty;

        [Option(
            's',
            "blob-size",
            Required = false,
            HelpText = "Set the blob size, in compressed MBs")]
        public int BlobSize { get; set; } = 10;

        [Option('a', "auth", Required = false, HelpText = "Set authentication method:  'AzCli' or 'System'")]
        public string Authentication { get; set; } = string.Empty;

        public override string ToString()
        {
            return $@"
DB Uri:  {DbUri}
Ingestion table:  {IngestionTable}
Template table:  {TemplateTable}
Authentication:  {Authentication}";
        }
    }
}