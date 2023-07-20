using System;
using System.Collections.Generic;
using System.Threading;
using Xunit;
using Xunit.Abstractions;

namespace io.confluent.parallelconsumer
{
    public class CoreBatchTest : ParallelEoSStreamProcessorTestBase, BatchTestBase
    {
        private BatchTestMethods<Void> batchTestMethods;

        public CoreBatchTest(ITestOutputHelper output) : base(output)
        {
        }

        public void Setup()
        {
            batchTestMethods = new BatchTestMethods<Void>(this)
            {
                protected KafkaTestUtils GetKtu()
                {
                    return ktu;
                }

                protected Void AverageBatchSizeTestPollStep(PollContext<string, string> recordList)
                {
                    try
                    {
                        Thread.Sleep(30);
                    }
                    catch (InterruptedException e)
                    {
                        log.error(e.Message, e);
                    }
                    return null;
                }

                protected void AverageBatchSizeTestPoll(AtomicInteger numBatches, AtomicInteger numRecords, RateLimiter statusLogger)
                {
                    parallelConsumer.Poll(pollBatch =>
                    {
                        AverageBatchSizeTestPollInner(numBatches, numRecords, statusLogger, pollBatch);
                    });
                }

                protected AbstractParallelEoSStreamProcessor GetPC()
                {
                    return parallelConsumer;
                }

                public void SimpleBatchTestPoll(List<PollContext<string, string>> batchesReceived)
                {
                    parallelConsumer.Poll(context =>
                    {
                        log.debug("Batch of messages: {}", context.GetOffsetsFlattened());
                        batchesReceived.Add(context);
                    });
                }

                protected void BatchFailPoll(List<PollContext<string, string>> accumlativeReceivedBatches)
                {
                    parallelConsumer.Poll(pollBatch =>
                    {
                        log.debug("Batch of messages: {}", pollBatch.GetOffsetsFlattened());
                        BatchFailPollInner(pollBatch);
                        accumlativeReceivedBatches.Add(pollBatch);
                    });
                }
            };
        }

        [Fact]
        public void AverageBatchSizeTest()
        {
            batchTestMethods.AverageBatchSizeTest(50000);
        }

        [Theory]
        [EnumSource]
        public void SimpleBatchTest(ParallelConsumerOptions.ProcessingOrder order)
        {
            batchTestMethods.SimpleBatchTest(order);
        }

        [Theory]
        [EnumSource]
        public void BatchFailureTest(ParallelConsumerOptions.ProcessingOrder order)
        {
            batchTestMethods.BatchFailureTest(order);
        }
    }
}