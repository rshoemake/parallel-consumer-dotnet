using System;

namespace Confluent.ParallelConsumer.State
{
    /**
     * Useful identifier for a ConsumerRecord.
     */
    public class ConsumerRecordId
    {
        public TopicPartition Tp { get; }
        public long Offset { get; }

        public ConsumerRecordId(TopicPartition tp, long offset)
        {
            Tp = tp;
            Offset = offset;
        }
    }
}