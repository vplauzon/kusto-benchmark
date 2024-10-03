using CommandLine;
using System;
using System.Collections.Generic;
using System.Text;

namespace QueryConsole
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
            't',
            "template-table",
            Required = true,
            HelpText = "Set the template table")]
        public string TemplateTable { get; set; } = string.Empty;

        [Option('a', "auth", Required = false, HelpText = "Set authentication method:  'AzCli' or 'System'")]
        public string Authentication { get; set; } = string.Empty;

        public override string ToString()
        {
            return $@"
DB Uri:  {DbUri}
Template table:  {TemplateTable}
Authentication:  {Authentication}
Source level:  {SourceLevel}";
        }
    }
}