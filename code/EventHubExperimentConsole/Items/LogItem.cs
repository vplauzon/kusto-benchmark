using System;
using System.Collections.Generic;
using System.Text;

namespace EventHubExperimentConsole.Items
{
    internal record LogItem(
        TtlRegistrationItem? TtlRegistrationItem,
        SubExperimentItem? SubExperimentItem)
    {
        public static LogItem Create(TtlRegistrationItem ttlRegistrationItem)
        {
            return new LogItem(ttlRegistrationItem, null);
        }

        public static LogItem Create(SubExperimentItem subExperimentItem)
        {
            return new LogItem(null, subExperimentItem);
        }
    }
}