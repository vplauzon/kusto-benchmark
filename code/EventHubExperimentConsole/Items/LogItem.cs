using System;
using System.Collections.Generic;
using System.Text;

namespace EventHubExperimentConsole.Items
{
    /// <summary>
    /// Root of what is persisted in the log file.
    /// A <see cref="LogItem"/> represents exactly one persisted log record type.
    /// </summary>
    internal record LogItem(
        NodeRegistrationItem? NodeRegistrationItem,
        NodeAssignationItem? NodeAssignationItem,
        ExperimentStepItem? ExperimentStepItem)
    {
        public static LogItem Create(NodeRegistrationItem nodeRegistrationItem)
        {
            return new LogItem(nodeRegistrationItem, null, null);
        }

        public static LogItem Create(NodeAssignationItem nodeAssignationItem)
        {
            return new LogItem(null, nodeAssignationItem, null);
        }

        public static LogItem Create(ExperimentStepItem experimentStepItem)
        {
            return new LogItem(null, null, experimentStepItem);
        }
    }
}
