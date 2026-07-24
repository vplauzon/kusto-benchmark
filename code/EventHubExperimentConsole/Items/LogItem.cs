using System;
using System.Collections.Generic;
using System.Text;

namespace EventHubExperimentConsole.Items
{
    internal record LogItem(
        TtlRegistrationItem? TtlRegistrationItem)
    {
        public static LogItem Create(TtlRegistrationItem ttlRegistrationItem)
        {
            return new LogItem(ttlRegistrationItem);
        }
    }
}