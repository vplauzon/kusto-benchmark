using System;
using System.Collections.Generic;
using System.Text;

namespace EventHubExperimentConsole.Items
{
    /// <summary>
    /// A <see cref="LogItem"/> represents either of its sub component (e.g. <see cref="TtlRegistrationItem"/>),
    /// i.e. one-and-only one will be non-null.
    /// </summary>
    /// <param name="TtlRegistrationItem"></param>
    /// <param name="SubExperimentItem"></param>
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