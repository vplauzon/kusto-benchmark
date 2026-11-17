using System;
using System.Collections.Generic;
using System.Text;

namespace EventHubExperimentConsole.Items
{
    /// <summary>
    /// Root of what is persisted in the log file.
    /// A <see cref="LogItem"/> represents either of its sub component (e.g. <see cref="TtlRegistrationItem"/>),
    /// i.e. one-and-only one will be non-null.
    /// </summary>
    /// <param name="TtlRegistrationItem"></param>
    /// <param name="ExperimentStepItem"></param>
    internal record LogItem(
        TtlRegistrationItem? TtlRegistrationItem,
        NodeRegistrationItem? NodeRegistrationItem,
        NodeAssignationItem? NodeAssignationItem,
        ExperimentStepItem? ExperimentStepItem)
    {
        public static LogItem Create(TtlRegistrationItem ttlRegistrationItem)
        {
            return new LogItem(ttlRegistrationItem, null, null, null);
        }

        public static LogItem Create(NodeRegistrationItem nodeRegistrationItem)
        {
            return new LogItem(null, nodeRegistrationItem, null, null);
        }

        public static LogItem Create(NodeAssignationItem nodeAssignationItem)
        {
            return new LogItem(null, null, nodeAssignationItem, null);
        }

        public static LogItem Create(ExperimentStepItem experimentStepItem)
        {
            return new LogItem(null, null, null, experimentStepItem);
        }
    }
}