using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace io.confluent.parallelconsumer.internal
{
    public abstract class AbstractOffsetCommitter<K, V> : IOffsetCommitter
    {
        protected readonly ConsumerManager<K, V> consumerMgr;
        protected readonly WorkManager<K, V> wm;

        protected AbstractOffsetCommitter(ConsumerManager<K, V> consumerMgr, WorkManager<K, V> wm)
        {
            this.consumerMgr = consumerMgr;
            this.wm = wm;
        }

        public async Task RetrieveOffsetsAndCommit()
        {
            Logger.LogDebug("Find completed work to commit offsets");
            PreAcquireOffsetsToCommit();
            try
            {
                var offsetsToCommit = wm.CollectCommitDataForDirtyPartitions();
                if (offsetsToCommit.Count == 0)
                {
                    Logger.LogDebug("No offsets ready");
                }
                else
                {
                    Logger.LogDebug("Will commit offsets for {0} partition(s): {1}", offsetsToCommit.Count, offsetsToCommit);
                    var groupMetadata = consumerMgr.GroupMetadata();

                    Logger.LogDebug("Begin commit offsets");
                    await CommitOffsets(offsetsToCommit, groupMetadata);

                    Logger.LogDebug("On commit success");
                    OnOffsetCommitSuccess(offsetsToCommit);
                }
            }
            finally
            {
                PostCommit();
            }
        }

        protected virtual void PostCommit()
        {
            // default noop
        }

        protected virtual void PreAcquireOffsetsToCommit()
        {
            // default noop
        }

        private void OnOffsetCommitSuccess(Dictionary<TopicPartition, OffsetAndMetadata> committed)
        {
            wm.OnOffsetCommitSuccess(committed);
        }

        protected abstract Task CommitOffsets(Dictionary<TopicPartition, OffsetAndMetadata> offsetsToSend, ConsumerGroupMetadata groupMetadata);
    }
}