using System;
using System.Collections.Generic;
using System.Linq;

namespace io.confluent.parallelconsumer
{
    /**
     * Context object used to pass messages to process to users processing functions.
     * <p>
     * Results sets can be iterated in a variety of ways. Explore the different methods available.
     * <p>
     * You can access for {@link ConsumerRecord}s directly, or you can get the {@link RecordContext} wrappers, which provide
     * extra information about the specific records, such as {@link RecordContext#getNumberOfFailedAttempts()}.
     * <p>
     * Note that if you are not setting a {@link ParallelConsumerOptions#batchSize}, then you can use the {@link
     * #getSingleRecord()}, and it's convenience accessors ({@link #value()}, {@link #offset()}, {@link #key()} {@link
     * #getSingleConsumerRecord()}). But if you have configured batching, they will all throw an {@link
     * IllegalArgumentException}, as it's not valid to have batches of messages and yet tread the batch input as a single
     * record.
     */
    public class PollContext<K, V> : IEnumerable<RecordContext<K, V>>
    {
        protected Dictionary<TopicPartition, HashSet<RecordContextInternal<K, V>>> records;

        public PollContext(List<WorkContainer<K, V>> workContainers)
        {
            records = new Dictionary<TopicPartition, HashSet<RecordContextInternal<K, V>>>();

            foreach (var wc in workContainers)
            {
                TopicPartition topicPartition = wc.GetTopicPartition();
                if (!records.ContainsKey(topicPartition))
                {
                    records[topicPartition] = new HashSet<RecordContextInternal<K, V>>();
                }
                records[topicPartition].Add(new RecordContextInternal<K, V>(wc));
            }
        }

        /**
         * @return a flat {@link Stream} of {@link RecordContext}s, which wrap the {@link ConsumerRecord}s in this result
         * set
         */
        public IEnumerable<RecordContextInternal<K, V>> StreamInternal()
        {
            return records.Values.SelectMany(x => x);
        }

        /**
         * @return a flat {@link Stream} of {@link RecordContext}s, which wrap the {@link ConsumerRecord}s in this result
         * set
         */
        public IEnumerable<RecordContext<K, V>> Stream()
        {
            return GetByTopicPartitionMap().Values.SelectMany(x => x);
        }

        /**
         * @return a flat {@link Stream} of {@link ConsumerRecord} in this poll set
         */
        public IEnumerable<ConsumerRecord<K, V>> StreamConsumerRecords()
        {
            return Stream().Select(x => x.GetConsumerRecord());
        }

        /**
         * Must not be using batching ({@link ParallelConsumerOptions#batchSize}).
         *
         * @return the single {@link RecordContext} entry in this poll set
         * @throws IllegalArgumentException if a {@link ParallelConsumerOptions#getBatchSize()} has been set.
         */
        public RecordContext<K, V> GetSingleRecord()
        {
            if (Size() != 1)
            {
                throw new ArgumentException("A 'batch size' has been specified in `options`, so you must use the `batch` versions of the polling methods. See {}", getLinkHtmlToDocSection("#batching"));
            }
            return Stream().FirstOrDefault();
        }

        /**
         * Must not be using batching ({@link ParallelConsumerOptions#batchSize}).
         *
         * @return the single {@link ConsumerRecord} entry in this poll set
         * @see #getSingleRecord()
         */
        public ConsumerRecord<K, V> GetSingleConsumerRecord()
        {
            return GetSingleRecord().GetConsumerRecord();
        }

        /**
         * For backwards compatibility with {@link ConsumerRecord#value()}.
         * <p>
         * Must not be using batching ({@link ParallelConsumerOptions#batchSize}).
         *
         * @return the single {@link ConsumerRecord#value()} entry in this poll set
         * @see #getSingleRecord()
         */
        public V Value()
        {
            return GetSingleConsumerRecord().Value;
        }

        /**
         * For backwards compatibility with {@link ConsumerRecord#key()}.
         * <p>
         * Must not be using batching ({@link ParallelConsumerOptions#batchSize}).
         *
         * @return the single {@link ConsumerRecord#key()} entry in this poll set
         * @see #getSingleRecord()
         */
        public K Key()
        {
            return GetSingleConsumerRecord().Key;
        }

        /**
         * For backwards compatibility with {@link ConsumerRecord#offset()}.
         * <p>
         * Must not be using batching ({@link ParallelConsumerOptions#batchSize}).
         *
         * @return the single {@link ConsumerRecord#offset()} entry in this poll set
         * @see #getSingleRecord()
         */
        public long Offset()
        {
            return GetSingleConsumerRecord().Offset;
        }

        /**
         * @return a flat {@link List} of {@link RecordContext}s, which wrap the {@link ConsumerRecord}s in this result set
         */
        public List<RecordContext<K, V>> GetContextsFlattened()
        {
            return records.Values.SelectMany(x => x).Select(x => x.GetRecordContext()).ToList();
        }

        /**
         * @return a flat {@link List} of {@link ConsumerRecord}s in this result set
         */
        public List<ConsumerRecord<K, V>> GetConsumerRecordsFlattened()
        {
            return StreamConsumerRecords().ToList();
        }

        /**
         * @return a flat {@link Iterator} of the {@link RecordContext}s, which wrap the {@link ConsumerRecord}s in this
         * result set
         */
        public IEnumerator<RecordContext<K, V>> GetEnumerator()
        {
            return Stream().GetEnumerator();
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        /**
         * @param action to perform on the {@link RecordContext}s, which wrap the {@link ConsumerRecord}s in this result
         *               set
         */
        public void ForEach(Action<RecordContext<K, V>> action)
        {
            foreach (var recordContext in this)
            {
                action(recordContext);
            }
        }

        /**
         * @return a flat {@link Spliterator} of the {@link RecordContext}s, which wrap the {@link ConsumerRecord}s in this
         * result set
         */
        public Spliterator<RecordContext<K, V>> Spliterator()
        {
            return Stream().Spliterator();
        }

        /**
         * @return a {@link Map} of {@link TopicPartition} to {@link RecordContext} {@link Set}, which wrap the {@link
         * ConsumerRecord}s in this result set
         */
        public Dictionary<TopicPartition, HashSet<RecordContext<K, V>>> GetByTopicPartitionMap()
        {
            return records.ToDictionary(x => x.Key, x => x.Value.Select(y => y.GetRecordContext()).ToHashSet());
        }

        /**
         * @return the total count of records in this result set
         */
        public long Size()
        {
            return Stream().Count();
        }

        /**
         * Get all the offsets for the records in this result set.
         * <p>
         * Note that this flattens the result, so if there are records from multiple {@link TopicPartition}s, the partition
         * they belong to will be lost. If you want that information as well, try {@link #getOffsets()}.
         *
         * @return a flat List of offsets in this result set
         * @see #getOffsets()
         */
        public List<long> GetOffsetsFlattened()
        {
            return StreamConsumerRecords().Select(x => x.Offset).ToList();
        }

        /**
         * Map of partitions to offsets.
         * <p>
         * If you don't need the partition information, try {@link #getOffsetsFlattened()}.
         *
         * @return a map of {@link TopicPartition} to offsets, of the records in this result set
         * @see #getOffsetsFlattened()
         */
        public Dictionary<TopicPartition, List<long>> GetOffsets()
        {
            return GetByTopicPartitionMap().ToDictionary(x => x.Key, x => x.Value.Select(y => y.Offset).ToList());
        }
    }
}