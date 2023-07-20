using System;
using Confluent.Kafka;

namespace io.confluent.parallelconsumer.state
{
    /**
     * Simple value class for processing {@link ShardKey}s to make the various key systems type safe and extendable.
     */
    public class ShardKey
    {
        public static ShardKey Of(WorkContainer<object, object> wc, ParallelConsumerOptions.ProcessingOrder ordering)
        {
            return Of(wc.GetCr(), ordering);
        }

        public static ShardKey Of(ConsumerRecord<object, object> rec, ParallelConsumerOptions.ProcessingOrder ordering)
        {
            return ordering switch
            {
                ParallelConsumerOptions.ProcessingOrder.KEY => OfKey(rec),
                ParallelConsumerOptions.ProcessingOrder.PARTITION or ParallelConsumerOptions.ProcessingOrder.UNORDERED => OfTopicPartition(rec),
                _ => throw new ArgumentException("Invalid processing order")
            };
        }

        public static KeyOrderedKey OfKey(ConsumerRecord<object, object> rec)
        {
            return new KeyOrderedKey(rec);
        }

        public static ShardKey OfTopicPartition(ConsumerRecord<object, object> rec)
        {
            return new TopicPartitionKey(new TopicPartition(rec.Topic, rec.Partition));
        }

        public abstract class KeyOrderedKey : ShardKey
        {
            /**
             * Note: We use just the topic name here, and not the partition, so that if we were to receive records from the
             * same key from the partitions we're assigned, they will be put into the same queue.
             */
            public TopicPartition TopicName { get; }

            /**
             * The key of the record being referenced. Nullable if record is produced with a null key.
             */
            public object Key { get; }

            public KeyOrderedKey(TopicPartition topicName, object key)
            {
                TopicName = topicName;
                Key = key;
            }

            public KeyOrderedKey(ConsumerRecord<object, object> rec) : this(new TopicPartition(rec.Topic, rec.Partition), rec.Key)
            {
            }
        }

        public class TopicPartitionKey : ShardKey
        {
            public TopicPartition TopicPartition { get; }

            public TopicPartitionKey(TopicPartition topicPartition)
            {
                TopicPartition = topicPartition;
            }
        }
    }
}