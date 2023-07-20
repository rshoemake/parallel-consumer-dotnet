using System;
using System.Collections.Generic;
using Confluent.Kafka;
using NUnit.Framework;
using System.Threading.Tasks;
using System.Linq;
using System.Threading;
using System.Diagnostics;

namespace ParallelConsumer.IntegrationTests.Utils
{
    [TestFixture]
    public class BrokerCommitAsserter
    {
        private readonly string _defaultTopic;
        private readonly KafkaConsumer<Ignore, Ignore> _assertConsumer;

        public BrokerCommitAsserter(string defaultTopic, KafkaConsumer<Ignore, Ignore> assertConsumer)
        {
            _defaultTopic = defaultTopic;
            _assertConsumer = assertConsumer;
        }

        public void AssertConsumedAtLeastOffset(int target)
        {
            AssertConsumedAtLeastOffset(_defaultTopic, target);
        }

        public void AssertConsumedAtLeastOffset(string topic, int target)
        {
            Setup(topic, target);

            await().UntilAsserted(() =>
            {
                var poll = _assertConsumer.Poll(TimeSpan.FromSeconds(1));

                Debug.WriteLine($"Polled {poll.Count} records, looking for at least offset {target}");
                Assert.That(poll, Has.OffsetAtLeast(target));
            });

            Post();
        }

        private void Post()
        {
            _assertConsumer.Unsubscribe();
        }

        private void Setup(string topic, int target)
        {
            Debug.WriteLine($"Asserting against topic: {topic}, expecting to consume at LEAST offset {target}");
            var topicSet = new HashSet<string> { topic };
            _assertConsumer.Subscribe(topicSet);
            _assertConsumer.SeekToBeginning(topicSet.Select(tp => new TopicPartitionOffset(tp, target)));
        }

        public void AssertConsumedAtMostOffset(string topic, int atMost)
        {
            Setup(topic, atMost);

            var delay = TimeSpan.FromSeconds(5);
            Debug.WriteLine($"Delaying by {delay} to check consumption from topic {topic} by at most {atMost}");
            await()
                .PollDelay(delay)
                .Timeout(delay.Add(TimeSpan.FromSeconds(1)))
                .UntilAsserted(() =>
                {
                    var poll = _assertConsumer.Poll(TimeSpan.FromSeconds(1));

                    Debug.WriteLine($"Polled {poll.Count} records, looking for at MOST offset {atMost}");
                    Assert.That(poll, Has.OffsetAtMost(atMost));
                });

            Post();
        }
    }
}