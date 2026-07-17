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

        [Option('c', "config-uri", Required = true, HelpText = "ADLS Gen2 URI of the experiment config file")]
        public string ConfigUri { get; set; } = string.Empty;

        public override string ToString()
        {
            return $@"
Authentication:  {Authentication}
Source level:  {SourceLevel}
ConfigUri:  {ConfigUri}";
        }
    }
}