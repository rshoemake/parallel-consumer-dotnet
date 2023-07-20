using System;
using Microsoft.Extensions.Logging;

namespace io.confluent.csid.utils
{
    public static class GeneralTestUtils
    {
        private static readonly ILogger log = LoggerFactory.GetLogger(typeof(GeneralTestUtils));

        public static void ChangeLogLevelTo(LogLevel targetLevel)
        {
            log.LogWarning("Making sure log level isn't too low");
            // Logger root = (Logger)LoggerFactory.GetLogger(Logger.ROOT_LOGGER_NAME);
            // root.SetLevel(Level.INFO);

            Logger csid = (Logger)LoggerFactory.GetLogger("io.confluent.csid");
            csid.SetLevel(targetLevel);
        }

        public static TimeSpan Time(Action task)
        {
            DateTime start = DateTime.Now;
            log.LogDebug("Timed function starting at: {0}", start);
            task();
            DateTime end = DateTime.Now;
            TimeSpan between = end - start;
            log.LogDebug("Finished, took {0}", between);
            return between;
        }
    }
}