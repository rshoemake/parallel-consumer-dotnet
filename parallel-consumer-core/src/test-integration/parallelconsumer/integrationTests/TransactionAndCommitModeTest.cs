using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.ParallelConsumer;
using Confluent.ParallelConsumer.Options;
using Confluent.ParallelConsumer.Internal;
using Lombok;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using static Confluent.ParallelConsumer.Options.ParallelConsumerOptions;
using static Confluent.ParallelConsumer.Options.ParallelConsumerOptions.CommitMode;
using static Confluent.ParallelConsumer.Options.ParallelConsumerOptions.ProcessingOrder;
using static Confluent.ParallelConsumer.IntegrationTests.Utils.KafkaClientUtils;

namespace Confluent.ParallelConsumer.IntegrationTests
{
    [TestClass]
    public class RebalanceEoSDeadlockTest : BrokerIntegrationTest<string, string>
    {
        private static readonly string PC_CONTROL = "pc-control";
        public static readonly string PC_BROKER_POLL = "pc-broker-poll";
        private Consumer<string, string> consumer;
        private Producer<string, string> producer;
        private CountdownEvent rebalanceLatch;
        private long sleepTimeMs = 0L;
        private ParallelEoSStreamProcessor<string, string> pc;

        public RebalanceEoSDeadlockTest()
        {
            base.numPartitions = 2;
        }

        private string outputTopic;

        [TestInitialize]
        public void Setup()
        {
            rebalanceLatch = new CountdownEvent(1);
            SetupTopic();
            outputTopic = SetupTopic("output-topic");
            producer = GetKcu().CreateNewProducer(ProducerMode.Transactional);
            consumer = GetKcu().CreateNewConsumer(GroupOption.NewGroup);
            var pcOptions = ParallelConsumerOptions<string, string>.Builder()
                .CommitMode(PeriodicTransactionalProducer)
                .Consumer(consumer)
                .ProduceLockAcquisitionTimeout(TimeSpan.FromMinutes(2))
                .Producer(producer)
                .Ordering(Partition)
                .Build();

            pc = new ParallelEoSStreamProcessor<string, string>(pcOptions, new PCModule<string, string>(pcOptions))
            {
                CommitOffsetsThatAreReady = () =>
                {
                    var threadName = Thread.CurrentThread.Name;
                    if (threadName.Contains(PC_CONTROL))
                    {
                        Console.WriteLine($"Delaying pc-control thread {sleepTimeMs}ms to force the potential deadlock on rebalance");
                        Thread.Sleep((int)sleepTimeMs);
                    }

                    base.CommitOffsetsThatAreReady();

                    if (threadName.Contains(PC_BROKER_POLL))
                    {
                        rebalanceLatch.Signal();
                    }
                },
                OnPartitionsRevoked = (partitions) =>
                {
                    base.OnPartitionsRevoked(partitions);
                }
            };

            pc.Subscribe(new List<string> { topic });
        }

        private const long SLEEP_TIME_MS = 3000L;

        [TestMethod]
        public void NoDeadlockOnRevoke()
        {
            sleepTimeMs = (long)(SLEEP_TIME_MS + (new Random().NextDouble() * 1000));
            var numberOfRecordsToProduce = 100L;
            var count = new AtomicLong();

            GetKcu().ProduceMessages(topic, numberOfRecordsToProduce);
            pc.TimeBetweenCommits = TimeSpan.FromSeconds(1);
            pc.PollAndProduce((recordContexts) =>
            {
                count.GetAndIncrement();
                Console.WriteLine($"Processed record, count now {count} - offset: {recordContexts.Offset}");
                return new ProducerRecord<string, string>(outputTopic, recordContexts.Key, recordContexts.Value);
            });

            await().Timeout(TimeSpan.FromSeconds(30)).Until(() => count.Value > 5);
            Console.WriteLine("Records are getting consumed");

            var newPollTimeout = TimeSpan.FromSeconds(5);
            Console.WriteLine($"Creating new consumer in same group and subscribing to same topic set with a no record timeout of {newPollTimeout}, expect this phase to take entire timeout...");
            using (var newConsumer = GetKcu().CreateNewConsumer(REUSE_GROUP))
            {
                newConsumer.Subscribe(new List<string> { topic });
                newConsumer.Poll(newPollTimeout);

                if (!rebalanceLatch.Wait(TimeSpan.FromSeconds(30)))
                {
                    Assert.Fail("Rebalance did not finish");
                }
                Console.WriteLine("Test finished");
            }
        }
    }
}