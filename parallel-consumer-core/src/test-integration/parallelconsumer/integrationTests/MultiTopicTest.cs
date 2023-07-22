using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using FluentAssertions;
using NUnit.Framework;

namespace ParallelConsumer.IntegrationTests
{
    [TestFixture]
    public class MultiTopicTest : BrokerIntegrationTest<string, string>
    {
        [TestCase(ProcessingOrder.Unordered)]
        [TestCase(ProcessingOrder.Ordered)]
        public void MultiTopic(ProcessingOrder order)
        {
            int numTopics = 3;
            List<NewTopic> multiTopics = GetKcu().CreateTopics(numTopics);
            int recordsPerTopic = 1;
            multiTopics.ForEach(singleTopic => SendMessages(singleTopic, recordsPerTopic));

            var pc = GetKcu().BuildPc<string, string>(order);
            pc.Subscribe(multiTopics.Select(t => t.Topic).ToList());

            var messageProcessedCount = new AtomicInteger();

            pc.Poll(pollContext =>
            {
                Console.WriteLine(pollContext.ToString());
                messageProcessedCount.IncrementAndGet();
            });

            // processed
            int expectedMessagesCount = recordsPerTopic * numTopics;
            await().Until(() => messageProcessedCount.Value == expectedMessagesCount);

            // commits
            pc.RequestCommitAsap();
            pc.Close();

            //
            var assertingConsumer = GetKcu().CreateNewConsumer<string, string>(false);
            await().AtMost(TimeSpan.FromSeconds(10))
                .Until(() =>
                {
                    AssertSeparateConsumerCommit(assertingConsumer, new HashSet<NewTopic>(multiTopics), recordsPerTopic);
                });
        }

        private void AssertSeparateConsumerCommit(Consumer<string, string> assertingConsumer, HashSet<NewTopic> topics, int expectedOffset)
        {
            var partitions = topics.Select(newTopic => new TopicPartition(newTopic.Topic, 0)).ToHashSet();
            var committed = assertingConsumer.Committed(partitions);
            var partitionSubjects = assertingConsumer.Should().HaveCommittedToPartition(partitions);
            foreach (var (topicPartition, commitHistorySubject) in partitionSubjects)
            {
                commitHistorySubject.Should().HaveAtLeastOffset(expectedOffset);
            }
        }

        private void SendMessages(NewTopic newTopic, int recordsPerTopic)
        {
            GetKcu().ProduceMessages(newTopic.Topic, recordsPerTopic);
        }
    }
}