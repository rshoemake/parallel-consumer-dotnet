using System;

namespace io.confluent.parallelconsumer.@internal
{
    public class RateLimiter
    {
        public TimeSpan Rate { get; private set; } = TimeSpan.FromSeconds(1);
        private long lastFireMs = 0;

        public RateLimiter()
        {
        }

        public RateLimiter(int seconds)
        {
            Rate = TimeSpan.FromSeconds(seconds);
        }

        public void PerformIfNotLimited(Action action)
        {
            if (IsOkToCallAction())
            {
                lastFireMs = DateTimeOffset.Now.ToUnixTimeMilliseconds();
                action();
            }
        }

        public bool CouldPerform()
        {
            return IsOkToCallAction();
        }

        private bool IsOkToCallAction()
        {
            long elapsed = GetElapsedMs();
            return lastFireMs == 0 || elapsed > Rate.TotalMilliseconds;
        }

        private long GetElapsedMs()
        {
            long now = DateTimeOffset.Now.ToUnixTimeMilliseconds();
            long elapsed = now - lastFireMs;
            return elapsed;
        }

        public TimeSpan GetElapsedDuration()
        {
            return TimeSpan.FromMilliseconds(GetElapsedMs());
        }
    }
}