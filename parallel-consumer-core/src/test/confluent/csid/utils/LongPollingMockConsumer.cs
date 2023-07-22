using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Confluent.Kafka;
using Confluent.Kafka.Internal;
using Confluent.ParallelConsumer.Internal;
using Lombok;
using Lombok.extern.slf4j.Slf4j;
using Pl.Tlinkowski.Unij.Api;
using Pl.Tlinkowski.Unij.Extensions;

namespace Io.Confluent.Csid.Utils
{
    /**
     * Used in tests to stub out the behaviour of the real Broker and Client's long polling system (the mock Kafka Consumer
     * doesn't have this behaviour).
     *
     * @author Antony Stubbs
     */
    [ToString]
    [Slf4j]
    public class LongPollingMockConsumer<K, V> : MockConsumer<K, V>
    {
        // thread safe for easy parallel tests - no need for performance considerations as is test harness
        [Getter]
        private readonly CopyOnWriteArrayList<Dictionary<TopicPartition, OffsetAndMetadata>> commitHistoryInt = new CopyOnWriteArrayList<Dictionary<TopicPartition, OffsetAndMetadata>>();

        private readonly AtomicBoolean statePretendingToLongPoll = new AtomicBoolean(false);

        public LongPollingMockConsumer(OffsetResetStrategy offsetResetStrategy) : base(offsetResetStrategy)
        {
        }

        public override ConsumerRecords<K, V> Poll(TimeSpan timeout)
        {
            var records = base.Poll(timeout);

            if (records.IsEmpty)
            {
                log.Debug("No records returned, simulating long poll with sleep for requested long poll timeout of {}...", timeout);
                lock (this)
                {
                    var sleepUntil = Instant.Now.Plus(timeout);
                    statePretendingToLongPoll.Set(true);
                    while (statePretendingToLongPoll.Get() && !TimeoutReached(sleepUntil))
                    {
                        var left = Duration.Between(Instant.Now, sleepUntil);
                        log.Debug("Time remaining: {}", left);
                        try
                        {
                            // a sleep of 0 ms causes an indefinite sleep
                            var msLeft = (long)left.TotalMilliseconds;
                            if (msLeft > 0)
                            {
                                Monitor.Wait(this, (int)msLeft);
                            }
                        }
                        catch (ThreadInterruptedException e)
                        {
                            log.Warn("Interrupted, ending this long poll early", e);
                            statePretendingToLongPoll.Set(false);
                        }
                    }
                    if (statePretendingToLongPoll.Get() && !TimeoutReached(sleepUntil))
                    {
                        log.Debug("Don't know why I was notified to wake up");
                    }
                    else if (statePretendingToLongPoll.Get() && TimeoutReached(sleepUntil))
                    {
                        log.Debug("Simulated long poll of ({}) finished. Now: {} vs sleep until: {}", timeout, Instant.Now, sleepUntil);
                    }
                    else if (!statePretendingToLongPoll.Get())
                    {
                        log.Debug("Simulated long poll was interrupted by by WAKEUP command...");
                    }
                    statePretendingToLongPoll.Set(false);
                }
            }
            else
            {
                log.Debug("Polled and found {} records...", records.Count);
            }
            return records;
        }

        /**
         * Restricted to ms precision to match {@link #wait()} semantics
         */
        private bool TimeoutReached(Instant sleepUntil)
        {
            var now = Instant.Now.ToEpochMilliseconds();
            var until = sleepUntil.ToEpochMilliseconds();
            // plus one due to truncation/rounding semantics
            return now + 1 >= until;
        }

        public override void AddRecord(ConsumeResult<K, V> record)
        {
            base.AddRecord(record);
            // wake me up if I'm pretending to long poll
            Wakeup();
        }

        public override void Wakeup()
        {
            if (statePretendingToLongPoll.Get())
            {
                statePretendingToLongPoll.Set(false);
                log.Debug("Interrupting mock long poll...");
                lock (this)
                {
                    Monitor.PulseAll(this);
                }
            }
        }

        public override void CommitAsync(Dictionary<TopicPartition, OffsetAndMetadata> offsets, Action<CommittedOffsets> callback)
        {
            commitHistoryInt.Add(offsets);
            base.CommitAsync(offsets, callback);
        }

        /**
         * Makes the commit history look like the {@link MockProducer}s one so we can use the same assert method.
         *
         * @see KafkaTestUtils#assertCommitLists(List, List, Optional)
         */
        private List<Dictionary<string, Dictionary<TopicPartition, OffsetAndMetadata>>> InjectConsumerGroupId(List<Dictionary<TopicPartition, OffsetAndMetadata>> commitHistory)
        {
            var groupId = this.GroupMetadata.GroupId;
            return commitHistory.Select(x => UniMaps.Of(groupId, x)).ToList();
        }

        /*
         * Makes the commit history look like the {@link MockProducer}s one, so we can use the same assert method.
         *
         * @see KafkaTestUtils#assertCommitLists(List, List, Optional)
         */
        public List<Dictionary<string, Dictionary<TopicPartition, OffsetAndMetadata>>> GetCommitHistoryWithGroupId()
        {
            var commitHistoryInt = GetCommitHistoryInt();
            return InjectConsumerGroupId(commitHistoryInt);
        }

        public override void Close(TimeSpan timeout)
        {
            RevokeAssignment();
            base.Close(timeout);
        }

        /**
         * {@link MockConsumer} fails to implement any {@link ConsumerRebalanceListener} system, so we manually revoke
         * assignments, use reflection to access the registered rebalance listener, call the listener, and only then close
         * the consumer.
         *
         * @see AbstractParallelEoSStreamProcessor#onPartitionsRevoked
         */
        private void RevokeAssignment()
        {
            var consumerRebalanceListener = GetRebalanceListener();

            // execute
            if (consumerRebalanceListener == null)
            {
                log.Warn("No rebalance listener assigned - on revoke can't fire");
            }
            else
            {
                var assignment = base.Assignment;
                consumerRebalanceListener.OnPartitionsRevoked(assignment);
            }
        }

        private ConsumerRebalanceListener<K, V> GetRebalanceListener()
        {
            // access listener
            var subscriptionsField = typeof(MockConsumer<K, V>).GetDeclaredField("subscriptions"); //NoSuchFieldException
            subscriptionsField.SetAccessible(true);
            var subscriptionState = (SubscriptionState<K, V>)subscriptionsField.Get(this); //IllegalAccessException
            var consumerRebalanceListener = subscriptionState.RebalanceListener;
            return consumerRebalanceListener;
        }

        public void SubscribeWithRebalanceAndAssignment(List<string> topics, int partitions)
        {
            var topicPartitions = topics.SelectMany(y => Enumerable.Range(0, partitions).Select(x => new TopicPartition(y, x))).ToList();
            Rebalance(topicPartitions);

            //
            var beginningOffsets = new Dictionary<TopicPartition, long>();
            foreach (var tp in topicPartitions)
            {
                beginningOffsets[tp] = 0L;
            }
            base.UpdateBeginningOffsets(beginningOffsets);
        }

        public override void Rebalance(List<TopicPartition> newAssignment)
        {
            base.Rebalance(newAssignment);
            var rebalanceListeners = GetRebalanceListener();
            if (rebalanceListeners != null)
            {
                rebalanceListeners.OnPartitionsAssigned(newAssignment);
            }
        }

        public void Revoke(List<TopicPartition> newAssignment)
        {
            var rebalanceListeners = GetRebalanceListener();
            if (rebalanceListeners != null)
            {
                rebalanceListeners.OnPartitionsRevoked(newAssignment);
            }
        }

        public void Assign(List<TopicPartition> newAssignment)
        {
            var rebalanceListeners = GetRebalanceListener();
            if (rebalanceListeners != null)
            {
                rebalanceListeners.OnPartitionsAssigned(newAssignment);
            }
        }

        public void RebalanceWithoutAssignment(List<TopicPartition> newAssignment)
        {
            base.Rebalance(newAssignment);
        }
    }
}