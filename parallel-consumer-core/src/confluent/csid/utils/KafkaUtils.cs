using Confluent.Kafka;
using System;

namespace Confluent.Csid.Utils
{
    public static class KafkaUtils
    {
        public static TopicPartition ToTopicPartition(ConsumeResult<object, object> rec)
        {
            return new TopicPartition(rec.Topic, rec.Partition);
        }
    }
}