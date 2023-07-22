using System;
using System.Collections.Generic;
using System.Threading;
using Confluent.Kafka;
using NUnit.Framework;
using ParallelConsumer;
using ParallelConsumer.Options;
using ParallelConsumer.Processor;
using ParallelConsumer.Tests.Utils;
using TLinkowski.Unij;
using TLinkowski.Unij.Collections;
using TLinkowski.Unij.Set;

namespace ParallelConsumer.IntegrationTests
{
    [TestFixture]
    public class RebalanceTest : BrokerIntegrationTest<string, string>
    {
        private Consumer<string, string> _consumer;
        private ParallelEoSStreamProcessor<string, string> _pc;

        public static readonly TimeSpan Infinite = TimeSpan.FromDays(1);

        public RebalanceTest()
        {
            NumPartitions = 2;
        }

        [SetUp]
        public void Setup()
        {
            SetupTopic();
            _consumer = GetKcu().CreateNewConsumer(KafkaClientUtils.GroupOption.NEW_GROUP);

            _pc = new ParallelEoSStreamProcessor<string, string>(ParallelConsumerOptions<string, string>.Builder()
                .Consumer(_consumer)
                .Ordering(ProcessingOrder.PARTITION)
                .Build());

            _pc.Subscribe(UniSets.Of(Topic));
        }

        [Test]
        public void CommitUponRevoke()
        {
            var numberOfRecordsToProduce = 20L;
            var count = new AtomicLong();

            GetKcu().ProduceMessages(Topic, numberOfRecordsToProduce);

            _pc.SetTimeBetweenCommits(Infinite);

            _pc.Poll(recordContexts =>
            {
                count.GetAndIncrement();
                Console.WriteLine($"Processed record, count now {count} - offset: {recordContexts.Offset}");
            });

            while (count.Value != numberOfRecordsToProduce)
            {
                Thread.Sleep(100);
            }

            var newPollTimeout = TimeSpan.FromSeconds(5);
            Console.WriteLine($"Creating new consumer in same group and subscribing to same topic set with a no record timeout of {newPollTimeout}, expect this phase to take entire timeout...");
            var newConsumer = GetKcu().CreateNewConsumer(KafkaClientUtils.GroupOption.REUSE_GROUP);
            newConsumer.Subscribe(UniLists.Of(Topic));
            Console.WriteLine($"Polling with new group member for records with timeout {newPollTimeout}...");
            var newConsumersPollResult = newConsumer.Poll(newPollTimeout);
            Console.WriteLine("Poll complete");

            Assert.AreEqual(0, newConsumersPollResult.Count);
            Console.WriteLine("Test finished");
        }
    }
}