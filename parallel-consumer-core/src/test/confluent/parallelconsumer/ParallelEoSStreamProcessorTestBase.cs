using Confluent.ParallelConsumer.Internal;
using Microsoft.Extensions.Logging;

namespace Confluent.ParallelConsumer
{
    public class ParallelEoSStreamProcessorTestBase : AbstractParallelEoSStreamProcessorTestBase
    {
        protected ParallelEoSStreamProcessor<string, string> parallelConsumer;

        protected override AbstractParallelEoSStreamProcessor<string, string> InitAsyncConsumer(ParallelConsumerOptions<string, string> parallelConsumerOptions)
        {
            return InitPollingAsyncConsumer(parallelConsumerOptions);
        }

        protected ParallelEoSStreamProcessor<string, string> InitPollingAsyncConsumer(ParallelConsumerOptions<string, string> parallelConsumerOptions)
        {
            parallelConsumer = new ParallelEoSStreamProcessor<string, string>(parallelConsumerOptions);
            base.ParentParallelConsumer = parallelConsumer;
            return parallelConsumer;
        }
    }
}