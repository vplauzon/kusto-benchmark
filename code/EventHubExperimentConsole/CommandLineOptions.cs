using CommandLine;
using System;
using System.Collections.Generic;
using System.Text;

namespace EventHubExperimentConsole
{
    public class CommandLineOptions
    {
        [Option('l', "source-level", Required = false, HelpText = "Set source level")]
        public string SourceLevel { get; set; } = "warning";

        [Option('a', "auth", Required = false, HelpText = "Set authentication method:  'AzCli' or 'System'")]
        public string Authentication { get; set; } = string.Empty;

        [Option('a', "path", Required = true, HelpText = "ADLS Gen2 path of the experiment config file")]
        public string Path { get; set; } = string.Empty;

        public override string ToString()
        {
            return $@"
Authentication:  {Authentication}
Source level:  {SourceLevel}
Path:  {Path}";
        }
    }
}