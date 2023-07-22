using System.Collections.Generic;
using System.Linq;
using Confluent.Kafka;
using Confluent.ParallelConsumer.State;
using System;

namespace Confluent.ParallelConsumer.Internal
{
    /// <summary>
    /// For tagging polled records with our epoch
    /// </summary>
    /// <typeparam name="K">The type of the key</typeparam>
    /// <typeparam name="V">The type of the value</typeparam>
    public class EpochAndRecordsMap<K, V>
    {
        private Dictionary<TopicPartition, RecordsAndEpoch> recordMap = new Dictionary<TopicPartition, RecordsAndEpoch>();

        public EpochAndRecordsMap(ConsumeResult<K, V> poll, PartitionStateManager<K, V> pm)
        {
            foreach (var partition in poll.Partition)
            {
                var records = poll.Message;
                long epochOfPartition = pm.GetEpochOfPartition(partition);
                var entry = new RecordsAndEpoch(partition, epochOfPartition, records);
                recordMap.Add(partition, entry);
            }
        }

        /// <summary>
        /// Get the partitions which have records contained in this record set.
        /// </summary>
        /// <returns>The set of partitions with data in this record set (may be empty if no data was returned)</returns>
        public HashSet<TopicPartition> Partitions()
        {
            return new HashSet<TopicPartition>(recordMap.Keys);
        }

        /// <summary>
        /// Get just the records for the given partition
        /// </summary>
        /// <param name="partition">The partition to get records for</param>
        /// <returns>The records and epoch for the given partition</returns>
        public RecordsAndEpoch Records(TopicPartition partition)
        {
            return recordMap[partition];
        }

        /// <summary>
        /// The number of records for all topics
        /// </summary>
        /// <returns>The count of records</returns>
        public int Count()
        {
            return recordMap.Values.Sum(x => x.Records.Count);
        }

        public class RecordsAndEpoch
        {
            public TopicPartition TopicPartition { get; }
            public long EpochOfPartitionAtPoll { get; }
            public List<ConsumeResult<K, V>> Records { get; }

            public RecordsAndEpoch(TopicPartition topicPartition, long epochOfPartitionAtPoll, List<ConsumeResult<K, V>> records)
            {
                TopicPartition = topicPartition;
                EpochOfPartitionAtPoll = epochOfPartitionAtPoll;
                Records = records;
            }
        }
    }
}