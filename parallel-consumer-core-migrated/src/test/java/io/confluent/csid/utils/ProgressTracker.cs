using System;
using System.Threading;
using System.Threading.Tasks;

namespace io.confluent.csid.utils
{
    public class ProgressTracker
    {
        public const int WARMED_UP_AFTER_X_MESSAGES = 50;

        private readonly AtomicInteger processedCount;
        private readonly AtomicInteger lastSeen = new AtomicInteger(0);
        public AtomicInteger rounds { get; }
        public Duration timeout { get; }
        private int roundsAllowed = 10;
        private readonly int coldRoundsAllowed = 20;
        public int highestRoundCountSeen { get; }
        private readonly Instant startTime = Instant.Now;

        public ProgressTracker(AtomicInteger processedCount, int? roundsAllowed, Duration timeout)
        {
            this.processedCount = processedCount;
            if (roundsAllowed != null && timeout != null)
                throw new ArgumentException("Can't provide both a timeout and a number of rounds");
            this.roundsAllowed = roundsAllowed ?? this.roundsAllowed;
            this.timeout = timeout ?? defaultTimeout;
            rounds = new AtomicInteger(0);
        }

        public ProgressTracker(AtomicInteger processedCount)
        {
            this.processedCount = processedCount;
            rounds = new AtomicInteger(0);
        }

        public bool hasProgressNotBeenMade()
        {
            bool progress = processedCount.Get() > lastSeen.Get();
            bool warmedUp = processedCount.Get() > WARMED_UP_AFTER_X_MESSAGES;
            bool enoughAttempts = hasTimeoutPassed();
            if (warmedUp && !progress && enoughAttempts)
            {
                return true;
            }
            else if (!warmedUp && this.roundsAllowed != null && rounds.Get() > coldRoundsAllowed)
            {
                return true;
            }
            else if (progress)
            {
                reset();
            }
            lastSeen.Set(processedCount.Get());
            rounds.IncrementAndGet();
            return false;
        }

        private bool hasTimeoutPassed()
        {
            if (roundsAllowed != null)
            {
                return rounds.Get() > roundsAllowed;
            }
            else
            {
                Duration remainingTime = Duration.Between(Instant.Now, getDeadline());
                return remainingTime.IsNegative;
            }
        }

        private DateTime getDeadline()
        {
            return startTime.Add(timeout);
        }

        private void reset()
        {
            if (rounds.Get() > highestRoundCountSeen)
                highestRoundCountSeen = rounds.Get();
            rounds.Set(0);
        }

        public void checkForProgressExceptionally()
        {
            bool noProgress = hasProgressNotBeenMade();
            if (noProgress)
                throw constructError();
        }

        public Exception constructError()
        {
            return constructError("");
        }

        public Exception constructError(string messageToAppend)
        {
            return new Exception(StringUtils.msg("No progress beyond {} records after {} rounds. {}",
                processedCount, rounds, messageToAppend));
        }
    }
}