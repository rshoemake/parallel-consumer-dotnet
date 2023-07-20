using System;
using System.Collections.Generic;
using System.Linq;
using Confluent.Kafka;
using io.confluent.parallelconsumer;
using io.confluent.parallelconsumer.internal;
using Lombok;
using Microsoft.Extensions.Logging;
using pl.tlinkowski.unij.api;
using static System.Boolean;

namespace io.confluent.parallelconsumer.state
{
    /**
     * Sharded, prioritised, offset managed, order controlled, delayed work queue.
     * <p>
     * Low Watermark - the highest offset (continuously successful) with all it's previous messages succeeded (the offset
     * one commits to broker)
     * <p>
     * High Water Mark - the highest offset which has succeeded (previous may be incomplete)
     * <p>
     * Highest seen offset - the highest ever seen offset
     * <p>
     * This state is shared between the {@link BrokerPollSystem} thread and the {@link AbstractParallelEoSStreamProcessor}.
     *
     * @author Antony Stubbs
     */
    [Slf4j]
    public class WorkManager<K, V> : ConsumerRebalanceListener
    {
        public ParallelConsumerOptions<K, V> Options { get; }

        // todo make private
        public PartitionStateManager<K, V> Pm { get; }

        // todo make private
        public ShardManager<K, V> Sm { get; }

        /**
         * The multiple of {@link ParallelConsumerOptions#getMaxConcurrency()} that should be pre-loaded awaiting
         * processing.
         * <p>
         * We use it here as well to make sure we have a matching number of messages in queues available.
         */
        private readonly DynamicLoadFactor dynamicLoadFactor;

        public int NumberRecordsOutForProcessing { get; private set; }

        /**
         * Useful for testing
         */
        public List<Action<WorkContainer<K, V>>> SuccessfulWorkListeners { get; } = new List<Action<WorkContainer<K, V>>>();

        public WorkManager(PCModule<K, V> module, DynamicLoadFactor dynamicExtraLoadFactor)
        {
            Options = module.Options();
            dynamicLoadFactor = dynamicExtraLoadFactor;
            Sm = new ShardManager<K, V>(module, this);
            Pm = new PartitionStateManager<K, V>(module, Sm);
        }

        /**
         * Load offset map for assigned partitions
         */
        public void OnPartitionsAssigned(ICollection<TopicPartition> partitions)
        {
            Pm.OnPartitionsAssigned(partitions);
        }

        /**
         * Clear offset map for revoked partitions
         * <p>
         * {@link AbstractParallelEoSStreamProcessor#onPartitionsRevoked} handles committing off offsets upon revoke
         *
         * @see AbstractParallelEoSStreamProcessor#onPartitionsRevoked
         */
        public void OnPartitionsRevoked(ICollection<TopicPartition> partitions)
        {
            Pm.OnPartitionsRevoked(partitions);
            OnPartitionsRemoved(partitions);
        }

        /**
         * Clear offset map for lost partitions
         */
        public void OnPartitionsLost(ICollection<TopicPartition> partitions)
        {
            Pm.OnPartitionsLost(partitions);
            OnPartitionsRemoved(partitions);
        }

        private void OnPartitionsRemoved(ICollection<TopicPartition> partitions)
        {
            // no-op - nothing to do
        }

        public void RegisterWork(EpochAndRecordsMap<K, V> records)
        {
            Pm.MaybeRegisterNewRecordAsWork(records);
        }

        /**
         * Get work with no limit on quantity, useful for testing.
         */
        public List<WorkContainer<K, V>> GetWorkIfAvailable()
        {
            return GetWorkIfAvailable(int.MaxValue);
        }

        /**
         * Depth first work retrieval.
         */
        public List<WorkContainer<K, V>> GetWorkIfAvailable(int requestedMaxWorkToRetrieve)
        {
            // optimise early
            if (requestedMaxWorkToRetrieve < 1)
            {
                return UniLists.Of<WorkContainer<K, V>>();
            }

            //
            var work = Sm.GetWorkIfAvailable(requestedMaxWorkToRetrieve);

            //
            if (log.isDebugEnabled())
            {
                log.debug("Got {} of {} requested records of work. In-flight: {}, Awaiting in commit (partition) queues: {}",
                        work.Count,
                        requestedMaxWorkToRetrieve,
                        NumberRecordsOutForProcessing,
                        NumberOfIncompleteOffsets);
            }
            NumberRecordsOutForProcessing += work.Count;
            return work;
        }

        public void OnSuccessResult(WorkContainer<K, V> wc)
        {
            log.Trace("Work success ({}), removing from processing shard queue", wc);

            wc.EndFlight();

            // update as we go
            Pm.OnSuccess(wc);
            Sm.OnSuccess(wc);

            // notify listeners
            SuccessfulWorkListeners.ForEach(c => c(wc));

            NumberRecordsOutForProcessing--;
        }

        /**
         * Can run from controller or poller thread, depending on which is responsible for committing
         *
         * @see PartitionStateManager#onOffsetCommitSuccess(Map)
         */
        public void OnOffsetCommitSuccess(Dictionary<TopicPartition, OffsetAndMetadata> committed)
        {
            Pm.OnOffsetCommitSuccess(committed);
        }

        public void OnFailureResult(WorkContainer<K, V> wc)
        {
            // error occurred, put it back in the queue if it can be retried
            wc.EndFlight();
            Pm.OnFailure(wc);
            Sm.OnFailure(wc);
            NumberRecordsOutForProcessing--;
        }

        public long NumberOfIncompleteOffsets => Pm.NumberOfIncompleteOffsets;

        public Dictionary<TopicPartition, OffsetAndMetadata> CollectCommitDataForDirtyPartitions()
        {
            return Pm.CollectDirtyCommitData();
        }

        /**
         * Have our partitions been revoked? Can a batch contain messages of different epochs?
         *
         * @return true if any epoch is stale, false if not
         * @see #checkIfWorkIsStale(WorkContainer)
         */
        public bool CheckIfWorkIsStale(List<WorkContainer<K, V>> workContainers)
        {
            foreach (var workContainer in workContainers)
            {
                if (CheckIfWorkIsStale(workContainer)) return true;
            }
            return false;
        }

        /**
         * Have our partitions been revoked?
         *
         * @return true if epoch doesn't match, false if ok
         */
        public bool CheckIfWorkIsStale(WorkContainer<K, V> workContainer)
        {
            return Pm.GetPartitionState(workContainer).CheckIfWorkIsStale(workContainer);
        }

        public bool ShouldThrottle()
        {
            return IsSufficientlyLoaded();
        }

        /**
         * @return true if there's enough messages downloaded from the broker already to satisfy the pipeline, false if more
         *         should be downloaded (or pipelined in the Consumer)
         */
        public bool IsSufficientlyLoaded()
        {
            return NumberOfWorkQueuedInShardsAwaitingSelection > (long)Options.TargetAmountOfRecordsInFlight * GetLoadingFactor();
        }

        private int GetLoadingFactor()
        {
            return dynamicLoadFactor.CurrentFactor;
        }

        public bool WorkIsWaitingToBeProcessed()
        {
            return Sm.WorkIsWaitingToBeProcessed();
        }

        public bool HasWorkInFlight()
        {
            return NumberRecordsOutForProcessing != 0;
        }

        public bool IsWorkInFlightMeetingTarget()
        {
            return NumberRecordsOutForProcessing >= Options.TargetAmountOfRecordsInFlight;
        }

        public long NumberOfWorkQueuedInShardsAwaitingSelection => Sm.NumberOfWorkQueuedInShardsAwaitingSelection;

        public bool HasIncompleteOffsets()
        {
            return Pm.HasIncompleteOffsets();
        }

        public bool IsRecordsAwaitingProcessing()
        {
            return Sm.NumberOfWorkQueuedInShardsAwaitingSelection > 0;
        }

        public void HandleFutureResult(WorkContainer<K, V> wc)
        {
            if (CheckIfWorkIsStale(wc))
            {
                // no op, partition has been revoked
                log.Debug("Work result received, but from an old generation. Dropping work from revoked partition {}", wc);
                wc.EndFlight();
                NumberRecordsOutForProcessing--;
            }
            else
            {
                var userFunctionSucceeded = wc.MaybeUserFunctionSucceeded;
                if (userFunctionSucceeded.HasValue)
                {
                    if (TRUE.Equals(userFunctionSucceeded.Value))
                    {
                        OnSuccessResult(wc);
                    }
                    else
                    {
                        OnFailureResult(wc);
                    }
                }
                else
                {
                    throw new InvalidOperationException("Work returned, but without a success flag - report a bug");
                }
            }
        }

        public bool IsNoRecordsOutForProcessing()
        {
            return NumberRecordsOutForProcessing == 0;
        }

        public Optional<TimeSpan> GetLowestRetryTime()
        {
            return Sm.GetLowestRetryTime();
        }

        public bool IsDirty()
        {
            return Pm.IsDirty();
        }
    }
}