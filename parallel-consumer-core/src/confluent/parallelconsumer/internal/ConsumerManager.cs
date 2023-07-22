using System;
using System.Collections.Generic;
using System.Threading;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace io.confluent.parallelconsumer.internal
{
    internal class ConsumerManager<K, V>
    {
        private readonly IConsumer<K, V> consumer;
        private readonly AtomicBoolean pollingBroker = new AtomicBoolean(false);
        private ConsumerGroupMetadata metaCache;
        private int erroneousWakups = 0;
        private int correctPollWakeups = 0;
        private int noWakeups = 0;
        private bool commitRequested;

        public ConsumerManager(IConsumer<K, V> consumer)
        {
            this.consumer = consumer;
        }

        public ConsumerRecords<K, V> Poll(TimeSpan requestedLongPollTimeout)
        {
            TimeSpan timeoutToUse = requestedLongPollTimeout;
            ConsumerRecords<K, V> records;
            try
            {
                if (commitRequested)
                {
                    Console.WriteLine("Commit requested, so will not long poll as need to perform the commit");
                    timeoutToUse = TimeSpan.FromMilliseconds(1); // disable long poll, as commit needs performing
                    commitRequested = false;
                }
                pollingBroker.Set(true);
                UpdateMetadataCache();
                Console.WriteLine($"Poll starting with timeout: {timeoutToUse}");
                records = consumer.Poll(timeoutToUse);
                Console.WriteLine($"Poll completed normally (after timeout of {timeoutToUse}) and returned {records.Count}...");
                UpdateMetadataCache();
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (WakeupException w)
            {
                correctPollWakeups++;
                Console.WriteLine("Awoken from broker poll");
                Console.WriteLine($"Wakeup caller is: {w}");
                records = new ConsumerRecords<K, V>(new Dictionary<TopicPartition, List<ConsumeResult<K, V>>>());
            }
            finally
            {
                pollingBroker.Set(false);
            }
            return records;
        }

        protected void UpdateMetadataCache()
        {
            metaCache = consumer.GroupMetadata;
        }

        public void Wakeup()
        {
            if (pollingBroker.Get())
            {
                Console.WriteLine("Waking up consumer");
                consumer.Wakeup();
            }
        }

        public void CommitSync(Dictionary<TopicPartition, OffsetAndMetadata> offsetsToSend)
        {
            bool inProgress = true;
            noWakeups++;
            while (inProgress)
            {
                try
                {
                    consumer.Commit(offsetsToSend);
                    inProgress = false;
                }
                catch (WakeupException w)
                {
                    Console.WriteLine($"Got woken up, retry. errors: {erroneousWakups} none: {noWakeups} correct: {correctPollWakeups}");
                    erroneousWakups++;
                }
            }
        }

        public void CommitAsync(Dictionary<TopicPartition, OffsetAndMetadata> offsets, Action<CommittedOffsets> callback)
        {
            bool inProgress = true;
            noWakeups++;
            while (inProgress)
            {
                try
                {
                    consumer.Commit(offsets, callback);
                    inProgress = false;
                }
                catch (WakeupException w)
                {
                    Console.WriteLine($"Got woken up, retry. errors: {erroneousWakups} none: {noWakeups} correct: {correctPollWakeups}");
                    erroneousWakups++;
                }
            }
        }

        public ConsumerGroupMetadata GroupMetadata()
        {
            return metaCache;
        }

        public void Close(TimeSpan defaultTimeout)
        {
            consumer.Close(defaultTimeout);
        }

        public HashSet<TopicPartition> Assignment()
        {
            return consumer.Assignment;
        }

        public void Pause(HashSet<TopicPartition> assignment)
        {
            consumer.Pause(assignment);
        }

        public HashSet<TopicPartition> Paused()
        {
            return consumer.Paused();
        }

        public void Resume(HashSet<TopicPartition> pausedTopics)
        {
            consumer.Resume(pausedTopics);
        }

        public void OnCommitRequested()
        {
            commitRequested = true;
        }
    }
}