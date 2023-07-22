using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.ParallelConsumer.Internal;
using Confluent.ParallelConsumer.Internal.ProducerManager;
using Confluent.ParallelConsumer.State;
using Confluent.ParallelConsumer.Utils;
using Nito.AsyncEx;

namespace Confluent.ParallelConsumer.State
{
    /// <summary>
    /// Context object for a given <see cref="ConsumeResult{TKey, TValue}"/>, carrying completion status, various time stamps, retry data etc..
    /// </summary>
    /// <typeparam name="TKey">The type of the key.</typeparam>
    /// <typeparam name="TValue">The type of the value.</typeparam>
    public class WorkContainer<TKey, TValue> : IComparable<WorkContainer<TKey, TValue>>
    {
        private static readonly string DEFAULT_TYPE = "DEFAULT";

        private readonly PCModule<TKey, TValue> module;

        public long Epoch { get; }
        public string WorkType { get; set; }
        public ConsumeResult<TKey, TValue> ConsumeResult { get; }
        public int NumberOfFailedAttempts { get; private set; }
        public Optional<Instant> LastFailedAt { get; private set; }
        public Optional<Instant> SucceededAt { get; private set; }
        public Optional<Exception> LastFailureReason { get; private set; }
        private bool inFlight;
        public Optional<bool> MaybeUserFunctionSucceeded { get; private set; }
        public Task<List<object>> Future { get; set; }
        private Optional<long> timeTakenAsWorkMs;

        public WorkContainer(long epoch, ConsumeResult<TKey, TValue> consumeResult, PCModule<TKey, TValue> module, string workType)
        {
            Epoch = epoch;
            ConsumeResult = consumeResult;
            WorkType = workType;
            this.module = module;
        }

        public WorkContainer(long epoch, ConsumeResult<TKey, TValue> consumeResult, PCModule<TKey, TValue> module)
            : this(epoch, consumeResult, module, DEFAULT_TYPE)
        {
        }

        public void EndFlight()
        {
            inFlight = false;
        }

        public bool IsDelayPassed()
        {
            if (!HasPreviouslyFailed())
            {
                return true;
            }

            Duration delay = GetDelayUntilRetryDue();
            return delay <= TimeSpan.Zero;
        }

        public Duration GetDelayUntilRetryDue()
        {
            Instant now = module.Clock.Instant;
            Temporal nextAttemptAt = GetRetryDueAt();
            return Duration.Between(now, nextAttemptAt);
        }

        public Instant GetRetryDueAt()
        {
            if (LastFailedAt.HasValue)
            {
                Duration retryDelay = GetRetryDelayConfig();
                return LastFailedAt.Value.Plus(retryDelay);
            }
            else
            {
                return Instant.MinValue;
            }
        }

        public Duration GetRetryDelayConfig()
        {
            var options = module.Options;
            var retryDelayProvider = options.RetryDelayProvider;
            if (retryDelayProvider != null)
            {
                return retryDelayProvider(new RecordContext<TKey, TValue>(this));
            }
            else
            {
                return options.DefaultMessageRetryDelay;
            }
        }

        public int CompareTo(WorkContainer<TKey, TValue> other)
        {
            long myOffset = this.ConsumeResult.Offset.Value;
            long theirOffset = other.ConsumeResult.Offset.Value;
            return myOffset.CompareTo(theirOffset);
        }

        public bool IsNotInFlight()
        {
            return !IsInFlight();
        }

        public bool IsInFlight()
        {
            return inFlight;
        }

        public void OnQueueingForExecution()
        {
            inFlight = true;
            timeTakenAsWorkMs = Optional.Of(DateTimeOffset.Now.ToUnixTimeMilliseconds());
        }

        public TopicPartition GetTopicPartition()
        {
            return new TopicPartition(ConsumeResult.Topic, ConsumeResult.Partition);
        }

        public void OnUserFunctionSuccess()
        {
            SucceededAt = Optional.Of(module.Clock.Instant);
            MaybeUserFunctionSucceeded = Optional.Of(true);
        }

        public void OnUserFunctionFailure(Exception cause)
        {
            UpdateFailureHistory(cause);
            MaybeUserFunctionSucceeded = Optional.Of(false);
        }

        private void UpdateFailureHistory(Exception cause)
        {
            NumberOfFailedAttempts++;
            LastFailedAt = Optional.Of(Instant.Now);
            LastFailureReason = Optional.OfNullable(cause);
        }

        public bool IsUserFunctionComplete()
        {
            return MaybeUserFunctionSucceeded.HasValue;
        }

        public bool IsUserFunctionSucceeded()
        {
            return MaybeUserFunctionSucceeded.GetValueOrDefault(false);
        }

        public override string ToString()
        {
            return $"WorkContainer(tp:{ConsumeResult.TopicPartition}:o:{ConsumeResult.Offset}:k:{ConsumeResult.Key})";
        }

        public Duration GetTimeInFlight()
        {
            if (!timeTakenAsWorkMs.HasValue)
            {
                return Duration.Zero;
            }

            long millis = DateTimeOffset.Now.ToUnixTimeMilliseconds() - timeTakenAsWorkMs.Value;
            return Duration.FromMilliseconds(millis);
        }

        public long Offset()
        {
            return ConsumeResult.Offset.Value;
        }

        public bool HasPreviouslyFailed()
        {
            return NumberOfFailedAttempts > 0;
        }

        public bool IsAvailableToTakeAsWork()
        {
            return IsNotInFlight() && !IsUserFunctionSucceeded() && IsDelayPassed();
        }

        public void OnPostAddToMailBox(PollContextInternal<TKey, TValue> context, Optional<ProducerManager<TKey, TValue>> producerManager)
        {
            producerManager.IfPresent(pm =>
            {
                var producingLock = context.ProducingLock;
                if (producingLock.HasValue)
                {
                    pm.FinishProducing(producingLock.Value);
                }
            });
        }
    }
}