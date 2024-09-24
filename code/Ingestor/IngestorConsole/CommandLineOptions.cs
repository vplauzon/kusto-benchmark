﻿using CommandLine;
using System;
using System.Collections.Generic;
using System.Text;

namespace IngestorConsole
{
    public class CommandLineOptions
    {
        [Option(
            's',
            "sample",
            Required = false,
            HelpText = "Set the sample file to generate from")]
        public string Source { get; set; } = string.Empty;

        [Option('a', "auth", Required = false, HelpText = "Set authentication method.")]
        public string Authentication { get; set; } = string.Empty;
   }
}