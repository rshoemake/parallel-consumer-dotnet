using System.Collections.Generic;
using Microsoft.Extensions.Logging;
using TestContainers.Containers.OutputConsumers;
using TestContainers.Containers.OutputConsumers.OutputFrame;
using LogLevel = Microsoft.Extensions.Logging.LogLevel;

namespace io.confluent.csid.testcontainers
{
    public class FilteredTestContainerSlf4jLogConsumer : Slf4jLogConsumer
    {
        public List<LogLevel> FilteredLevels { get; set; } = new List<LogLevel> { LogLevel.Trace, LogLevel.Debug };

        public FilteredTestContainerSlf4jLogConsumer(ILogger logger) : base(logger)
        {
        }

        public FilteredTestContainerSlf4jLogConsumer(ILogger logger, bool separateOutputStreams) : base(logger, separateOutputStreams)
        {
        }

        public override void Accept(IOutputFrame outputFrame)
        {
            if (Logger.IsEnabled(LogLevel.Debug))
            {
                string utf8String = outputFrame.GetUtf8String();
                bool isFilteredOut = FilteredLevels.Exists(level => utf8String.Contains(level.ToString()));
                if (!isFilteredOut)
                {
                    base.Accept(outputFrame);
                }
                else
                {
                    // ignoring trace level logging
                }
            }
        }
    }
}