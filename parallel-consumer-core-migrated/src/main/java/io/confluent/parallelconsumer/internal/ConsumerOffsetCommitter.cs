using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Confluent.Kafka.SyncOverAsync;
using Microsoft.Extensions.Logging;

namespace io.confluent.parallelconsumer.internal
{
    public class ConsumerOffsetCommitter<K, V> : AbstractOffsetCommitter<K, V>, IOffsetCommitter
    {
        private static readonly int ARBITRARY_RETRY_LIMIT = 50;

        private readonly CommitMode commitMode;

        private readonly TimeSpan commitTimeout;

        private Optional<Thread> owningThread = Optional<Thread>.Empty;

        private readonly ConcurrentQueue<CommitRequest> commitRequestQueue = new ConcurrentQueue<CommitRequest>();

        private readonly BlockingCollection<CommitResponse> commitResponseQueue = new BlockingCollection<CommitResponse>();

        public ConsumerOffsetCommitter(ConsumerManager<K, V> newConsumer, WorkManager<K, V> newWorkManager, ParallelConsumerOptions options) : base(newConsumer, newWorkManager)
        {
            commitMode = options.CommitMode;
            commitTimeout = options.OffsetCommitTimeout;
            if (commitMode.Equals(CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER))
            {
                throw new ArgumentException("Cannot use " + commitMode + " when using " + this.GetType().Name);
            }
        }

        public void Commit()
        {
            if (IsOwner())
            {
                RetrieveOffsetsAndCommit();
            }
            else if (IsSync())
            {
                Log.Debug("Sync commit");
                CommitAndWait();
                Log.Debug("Finished waiting");
            }
            else
            {
                Log.Debug("Async commit to be requested");
                RequestCommitInternal();
            }
        }

        protected override void CommitOffsets(Dictionary<TopicPartition, OffsetAndMetadata> offsetsToSend, ConsumerGroupMetadata groupMetadata)
        {
            if (offsetsToSend.Count == 0)
            {
                Log.Trace("Nothing to commit");
                return;
            }
            switch (commitMode)
            {
                case CommitMode.PERIODIC_CONSUMER_SYNC:
                    Log.Debug("Committing offsets Sync");
                    consumerMgr.CommitSync(offsetsToSend);
                    break;
                case CommitMode.PERIODIC_CONSUMER_ASYNCHRONOUS:
                    Log.Debug("Committing offsets Async");
                    consumerMgr.CommitAsync(offsetsToSend, (offsets, exception) =>
                    {
                        if (exception != null)
                        {
                            Log.Error("Error committing offsets", exception);
                        }
                    });
                    break;
                default:
                    throw new ArgumentException("Cannot use " + commitMode + " when using " + this.GetType().Name);
            }
        }

        protected override void PostCommit()
        {
        }

        private bool IsOwner()
        {
            return Thread.CurrentThread.Equals(owningThread.OrElse(null));
        }

        public void MaybeDoCommit()
        {
            CommitRequest poll;
            if (commitRequestQueue.TryDequeue(out poll))
            {
                Log.Debug("Commit requested, performing...");
                RetrieveOffsetsAndCommit();
                if (IsSync())
                {
                    Log.Debug("Adding commit response to queue...");
                    commitResponseQueue.Add(new CommitResponse(poll));
                }
            }
        }

        private void CommitAndWait()
        {
            CommitRequest commitRequest = RequestCommitInternal();
            bool waitingOnCommitResponse = true;
            int attempts = 0;
            while (waitingOnCommitResponse)
            {
                if (attempts > ARBITRARY_RETRY_LIMIT)
                {
                    throw new TimeoutException("Too many attempts taking commit responses");
                }
                try
                {
                    Log.Debug("Waiting on a commit response");
                    TimeSpan timeout = AbstractParallelEoSStreamProcessor.DEFAULT_TIMEOUT;
                    CommitResponse take;
                    if (!commitResponseQueue.TryTake(out take, commitTimeout))
                    {
                        throw new TimeoutException("Timeout waiting for commit response " + timeout + " to request " + commitRequest);
                    }
                    waitingOnCommitResponse = take.Request.Id != commitRequest.Id;
                }
                catch (Exception e)
                {
                    Log.Debug("Interrupted waiting for commit response", e);
                }
                attempts++;
            }
        }

        private CommitRequest RequestCommitInternal()
        {
            CommitRequest request = new CommitRequest();
            commitRequestQueue.Enqueue(request);
            consumerMgr.Wakeup();
            return request;
        }

        private bool IsSync()
        {
            return commitMode.Equals(CommitMode.PERIODIC_CONSUMER_SYNC);
        }

        public void Claim()
        {
            owningThread = Optional<Thread>.Of(Thread.CurrentThread);
        }
    }
}