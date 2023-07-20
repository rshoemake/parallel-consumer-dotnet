using System;
using System.Collections.Generic;
using System.Threading;
using Confluent.Kafka;
using Xunit;

namespace io.confluent.parallelconsumer
{
    public class ParallelEoSSStreamProcessorRebalancedTest : ParallelEoSStreamProcessorTestBase
    {
        private static readonly AtomicInteger RECORD_SET_KEY_GENERATOR = new AtomicInteger();

        [Fact]
        public void PausingAndResumingProcessingShouldWork()
        {
            var countDownLatch = new CountdownEvent(1);
            var pcModuleTestEnv = new PCModuleTestEnv(GetBaseOptions(CommitMode), countDownLatch);
            parallelConsumer = new ParallelEoSStreamProcessor<string, string>(GetBaseOptions(CommitMode), pcModuleTestEnv);
            parentParallelConsumer = parallelConsumer;
            parallelConsumer.Subscribe(new List<string> { INPUT_TOPIC });
            this.consumerSpy.SubscribeWithRebalanceAndAssignment(new List<string> { INPUT_TOPIC }, 2);
            AttachLoopCounter(parallelConsumer);

            parallelConsumer.Poll(context =>
            {
                //do nothing call never lands here
            });

            AddRecordsWithSetKeyForEachPartition();
            awaitUntilTrue(() => parallelConsumer.GetWm().GetNumberRecordsOutForProcessing() > 0);
            consumerSpy.Revoke(new List<TopicPartition> { new TopicPartition(INPUT_TOPIC, 0) });
            consumerSpy.RebalanceWithoutAssignment(consumerSpy.Assignment());
            consumerSpy.Assign(new List<TopicPartition> { new TopicPartition(INPUT_TOPIC, 0) });

            AddRecordsWithSetKeyForEachPartition();
            countDownLatch.Signal();
            awaitForCommit(4);
        }

        private void AddRecordsWithSetKeyForEachPartition()
        {
            long recordSetKey = RECORD_SET_KEY_GENERATOR.IncrementAndGet();
            log.Debug("Producing {0} records with set key {1}.", 2, recordSetKey);
            consumerSpy.AddRecord(ktu.MakeRecord(0, "key-" + recordSetKey + 0, "v0-test-" + 0));
            consumerSpy.AddRecord(ktu.MakeRecord(1, "key-" + recordSetKey + 0, "v0-test-" + 0));
            log.Debug("Finished producing {0} records with set key {1}.", 2, recordSetKey);
        }

        private ParallelConsumerOptions<string, string> GetBaseOptions(CommitMode commitMode)
        {
            var optionsBuilder = ParallelConsumerOptions<string, string>.Builder()
                .CommitMode(commitMode)
                .Consumer(consumerSpy)
                .BatchSize(2)
                .MaxConcurrency(1);

            if (commitMode == CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER)
            {
                optionsBuilder.Producer(producerSpy);
            }

            return optionsBuilder.Build();
        }
    }
}