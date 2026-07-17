using System;
using System.Collections.Generic;
using System.Text;

namespace EventHubExperimentConsole.Items
{
    internal record LogItem(
        TtlRegistrationItem? TtlRegistrationItem);
}