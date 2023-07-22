using System;
using System.Collections.Generic;
using Confluent.Kafka;
using Confluent.ParallelConsumer.Internal;
using Confluent.ParallelConsumer.Offsets;
using Confluent.ParallelConsumer.Utils;
using Microsoft.Extensions.Logging;

namespace Confluent.ParallelConsumer.State
{
    /**
     * No op version of {@link PartitionState} used for when partition assignments are removed, to avoid managing null
     * references or {@link Optional}s. By replacing with a no op implementation, we protect for stale messages still in
     * queues which reference it, among other things.
     * <p>
     * The alternative to this implementation, is having {@link PartitionStateManager#getPartitionState(TopicPartition)}
     * return {@link Optional}, which forces the implicit null check everywhere partition state is retrieved. This was
     * drafted to a degree, but found to be extremely invasive, where this solution with decent separation of concerns and
     * encapsulation, is sufficient and potentially more useful as is non-destructive. Potential issue is that of memory
     * leak as the collection will forever expand. However, even massive partition counts to a single consumer would be in
     * the hundreds of thousands, this would only result in hundreds of thousands of {@link TopicPartition} object keys all
     * pointing to the same instance of {@link RemovedPartitionState}.
     *
     * @author Antony Stubbs
     */
    public class RemovedPartitionState<K, V> : PartitionState<K, V>
    {
        private static readonly SortedSet<long> READ_ONLY_EMPTY_SET = new SortedSet<long>();

        private static readonly PartitionState singleton = new RemovedPartitionState<K, V>();

        public static PartitionState GetSingleton()
        {
            return RemovedPartitionState.singleton;
        }

        public RemovedPartitionState() : base(NO_EPOCH, null, null, OffsetMapCodecManager.HighestOffsetAndIncompletes.Of())
        {
        }

        public override bool IsRemoved()
        {
            // by definition true in this implementation
            return true;
        }

        public override TopicPartition GetTp()
        {
            return null;
        }

        public override void MaybeRegisterNewPollBatchAsWork(EpochAndRecordsMap<K, V>.RecordsAndEpoch recordsAndEpoch)
        {
            // no-op
            log.LogWarning("Dropping polled record batch for partition no longer assigned. WC: {0}", recordsAndEpoch);
        }

        /**
         * Don't allow more records to be processed for this partition. Eventually these records triggering this check will
         * be cleaned out.
         *
         * @return always returns false
         */
        protected override bool IsAllowedMoreRecords()
        {
            log.LogDebug(NO_OP);
            return true;
        }

        public override SortedSet<long> GetIncompleteOffsetsBelowHighestSucceeded()
        {
            log.LogDebug(NO_OP);
            return READ_ONLY_EMPTY_SET;
        }

        public override long GetOffsetHighestSeen()
        {
            log.LogDebug(NO_OP);
            return PartitionState.KAFKA_OFFSET_ABSENCE;
        }

        public override long GetOffsetHighestSucceeded()
        {
            log.LogDebug(NO_OP);
            return PartitionState.KAFKA_OFFSET_ABSENCE;
        }

        public override bool IsRecordPreviouslyCompleted(ConsumeResult<K, V> rec)
        {
            log.LogDebug("Ignoring previously completed request for partition no longer assigned. Partition: {0}", KafkaUtils.ToTopicPartition(rec));
            return false;
        }

        public override bool HasIncompleteOffsets()
        {
            return false;
        }

        public override int GetNumberOfIncompleteOffsets()
        {
            return 0;
        }

        public override void OnSuccess(long offset)
        {
            log.LogDebug("Dropping completed work container for partition no longer assigned. WC: {0}, partition: {1}", offset, GetTp());
        }

        public override bool IsPartitionRemovedOrNeverAssigned()
        {
            return true;
        }
    }
}