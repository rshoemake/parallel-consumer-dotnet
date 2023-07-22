using System;

namespace Confluent.ParallelConsumer
{
    public interface IBatchTestBase
    {
        void AverageBatchSizeTest();

        void SimpleBatchTest(ParallelConsumerOptions.ProcessingOrder order);

        void BatchFailureTest(ParallelConsumerOptions.ProcessingOrder order);
    }
}