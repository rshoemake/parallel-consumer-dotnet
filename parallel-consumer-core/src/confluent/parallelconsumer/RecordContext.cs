using System;
using System.Collections.Generic;
using Confluent.Kafka;
using Confluent.ParallelConsumer.State;
using Lombok;
using Lombok.experimental.Delegate;

namespace Confluent.ParallelConsumer
{
    /**
     * Context information for the wrapped {@link ConsumerRecord}.
     * <p>
     * Includes all accessors (~getters) in {@link ConsumerRecord} via delegation ({@link Delegate}).
     *
     * @see #getNumberOfFailedAttempts()
     */
    [Builder(ToBuilder = true)]
    [AllArgsConstructor]
    [ToString]
    [EqualsAndHashCode]
    public class RecordContext<K, V>
    {
        [Getter(AccessLevel.PROTECTED)]
        protected readonly WorkContainer<K, V> workContainer;

        [Getter]
        [Delegate]
        private readonly ConsumerRecord<K, V> consumerRecord;

        public RecordContext(WorkContainer<K, V> wc)
        {
            this.consumerRecord = wc.GetCr();
            this.workContainer = wc;
        }

        /**
         * A useful ID class for consumer records.
         *
         * @return the ID for the contained record
         */
        public ConsumerRecordId GetRecordId()
        {
            var topicPartition = new TopicPartition(Topic, Partition);
            return new ConsumerRecordId(topicPartition, Offset);
        }

        /**
         * @return the number of times this {@link ConsumerRecord} has failed processing already
         */
        public int GetNumberOfFailedAttempts()
        {
            return workContainer.GetNumberOfFailedAttempts();
        }

        /**
         * @return if the record has failed, return the time at which is last failed at
         */
        public Optional<Instant> GetLastFailureAt()
        {
            return workContainer.GetLastFailedAt();
        }

        /**
         * @return if the record had succeeded, returns the time at this the user function returned
         */
        public Optional<Instant> GetSucceededAt()
        {
            return workContainer.GetSucceededAt();
        }
    }
}