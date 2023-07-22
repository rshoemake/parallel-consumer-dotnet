using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using Confluent.Kafka;
using NUnit.Framework;
using Awaitility = NUnit.Framework.Awaitility;

namespace io.confluent.parallelconsumer
{
    [TestFixture]
    public class MockConsumerTest
    {
        private readonly string topic = typeof(MockConsumerTest).Name;

        [Test]
        public void MockConsumer()
        {
            var mockConsumer = new MockConsumer<string, string>(ConsumerConfig.DefaultConsumerConfig());
            Dictionary<TopicPartition, long> startOffsets = new Dictionary<TopicPartition, long>();
            TopicPartition tp = new TopicPartition(topic, 0);
            startOffsets.Add(tp, 0L);

            var options = ParallelConsumerOptions<string, string>.Builder()
                .Consumer(mockConsumer)
                .Build();
            var parallelConsumer = new ParallelEoSStreamProcessor<string, string>(options);
            parallelConsumer.Subscribe(new List<string> { topic });

            mockConsumer.Assign(new List<TopicPartition> { tp });
            parallelConsumer.OnPartitionsAssigned(new List<TopicPartition> { tp });
            mockConsumer.UpdateBeginningOffsets(startOffsets);

            AddRecords(mockConsumer);

            ConcurrentQueue<RecordContext<string, string>> records = new ConcurrentQueue<RecordContext<string, string>>();
            parallelConsumer.Poll(recordContexts =>
            {
                foreach (var recordContext in recordContexts)
                {
                    Console.WriteLine("Processing: {0}", recordContext);
                    records.Enqueue(recordContext);
                }
            });

            Awaitility.Await().UntilAsserted(() =>
            {
                Assert.That(records.Count, Is.EqualTo(3));
            });
        }

        private void AddRecords(MockConsumer<string, string> mockConsumer)
        {
            mockConsumer.AddRecord(new Confluent.Kafka.ConsumerRecord<string, string>(topic, 0, 0, "key", "value"));
            mockConsumer.AddRecord(new Confluent.Kafka.ConsumerRecord<string, string>(topic, 0, 1, "key", "value"));
            mockConsumer.AddRecord(new Confluent.Kafka.ConsumerRecord<string, string>(topic, 0, 2, "key", "value"));
        }
    }
}