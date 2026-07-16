using System;
using System.Collections.Generic;
using System.Text;

namespace EventHubExperimentConsole
{
    internal record TaggedResult<T>(T Result, string Tag);
}