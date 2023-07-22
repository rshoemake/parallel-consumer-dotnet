using Confluent.ParallelConsumer.Truth;
using Confluent.ParallelConsumer.Utils;
using Kafka.Public;
using Kafka.Public.Utils;
using NUnit.Framework;
using System.Collections.Generic;

namespace Confluent.ParallelConsumer.Tests.Truth
{
    [TestFixture]
    public class TruthGeneratorTests
    {
        [Test]
        public void Generate()
        {
            Assert.IsTrue(ManagedTruth.AssertTruth(new ConsumerRecords<UniMap>(UniMaps.Of())).Partitions.IsEmpty());

            Assert.IsNotNull(ManagedTruth.AssertTruth(PodamUtils.CreateInstance<OffsetAndMetadata>()).Offset);

            Assert.IsTrue(ManagedTruth.AssertTruth(PodamUtils.CreateInstance<TopicPartition>()).HasTopic().IsNotEmpty());

            Assert.IsTrue(ManagedTruth.AssertTruth(PodamUtils.CreateInstance<RecordMetadata>()).HasTimestamp());

            Assert.IsTrue(ManagedTruth.AssertTruth(PodamUtils.CreateInstance<ProducerRecord<string, string>>()).Headers.IsEmpty());
        }
    }
}