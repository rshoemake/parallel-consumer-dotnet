using System;
using NLog;
using ProgressBar;

namespace Confluent.Csid.Utils
{
    public static class ProgressBarUtils
    {
        public static ProgressBar GetNewMessagesBar(Logger log, long initialMax)
        {
            return GetNewMessagesBar(null, log, initialMax);
        }

        public static ProgressBar GetNewMessagesBar(string name, Logger log, long initialMax)
        {
            var delegatingProgressBarConsumer = new DelegatingProgressBarConsumer(log.Info);

            string usedName = "progress";
            if (name != null)
                usedName = name;

            return new ProgressBarBuilder()
                .SetConsumer(delegatingProgressBarConsumer)
                .SetInitialMax(initialMax)
                .ShowSpeed()
                .SetTaskName(usedName)
                .SetUnit("msg", 1)
                .Build();
        }
    }
}