using System.Collections.Generic;
using Confluent.Kafka;
using Moq;
using Xunit;

namespace io.confluent.parallelconsumer.state
{
    public class ModelUtils
    {
        private readonly PCModuleTestEnv module;

        public ModelUtils(PCModuleTestEnv module)
        {
            this.module = module;
        }

        public ModelUtils() : this(new PCModuleTestEnv())
        {
        }

        public WorkContainer<string, string> CreateWorkFor(long offset)
        {
            var mockCr = new Mock<ConsumerRecord<string, string>>();
            var workContainer = new WorkContainer<string, string>(0, mockCr.Object, module);
            mockCr.Setup(cr => cr.Offset).Returns(offset);
            return workContainer;
        }

        public EpochAndRecordsMap<string, string> CreateFreshWork()
        {
            return new EpochAndRecordsMap<string, string>(CreateConsumerRecords(), module.WorkManager.Pm);
        }

        public ConsumerRecords<string, string> CreateConsumerRecords()
        {
            return new ConsumerRecords<string, string>(new Dictionary<TopicPartition, List<ConsumerRecord<string, string>>>
            {
                { GetPartition(), new List<ConsumerRecord<string, string>> { CreateConsumerRecord(topic) } }
            });
        }

        public TopicPartition GetPartition()
        {
            return new TopicPartition(topic, 0);
        }

        public List<TopicPartition> GetPartitions()
        {
            return new List<TopicPartition> { new TopicPartition(topic, 0) };
        }

        private long nextOffset = 0L;

        private ConsumerRecord<string, string> CreateConsumerRecord(string topic)
        {
            var cr = new ConsumerRecord<string, string>(topic, 0, nextOffset, "a-key", "a-value");
            nextOffset++;
            return cr;
        }

        public ProducerRecord<string, string> CreateProducerRecords()
        {
            return new ProducerRecord<string, string>(topic, "a-key", "a-value");
        }

        private readonly string topic = "topic";

        private readonly string groupId = "cg-1";

        public ConsumerGroupMetadata ConsumerGroupMeta()
        {
            return new ConsumerGroupMetadata(groupId);
        }

        public List<ProducerRecord<string, string>> CreateProducerRecords(string topicName, long numberToSend)
        {
            return CreateProducerRecords(topicName, numberToSend, "");
        }

        public List<ProducerRecord<string, string>> CreateProducerRecords(string topicName, long numberToSend, string prefix)
        {
            var recs = new List<ProducerRecord<string, string>>();
            for (int i = 0; i < numberToSend; i++)
            {
                var key = prefix + "key-" + i;
                var record = new ProducerRecord<string, string>(topicName, key, "value-" + i);
                recs.Add(record);
            }
            return recs;
        }
    }
}