using System;
using System.Diagnostics;
using System.Threading;

namespace io.confluent.parallelconsumer.internal
{
    public class DynamicLoadFactor
    {
        private static readonly int DEFAULT_INITIAL_LOADING_FACTOR = 2;

        private readonly long startTimeMs = DateTimeOffset.Now.ToUnixTimeMilliseconds();

        private readonly TimeSpan coolDown = TimeSpan.FromSeconds(2);

        private readonly TimeSpan warmUp = TimeSpan.FromSeconds(2);

        private readonly int stepUpFactorBy = 1;

        public int MaxFactor { get; }

        public int CurrentFactor { get; private set; } = DEFAULT_INITIAL_LOADING_FACTOR;

        private int lastSteppedFactor;

        private DateTimeOffset lastStepTime = DateTimeOffset.MinValue;

        public DynamicLoadFactor(int maxFactor)
        {
            MaxFactor = maxFactor;
            lastSteppedFactor = CurrentFactor;
        }

        public bool MaybeStepUp()
        {
            if (CouldStep())
            {
                return DoStep();
            }
            return false;
        }

        private bool DoStep()
        {
            if (IsMaxReached())
            {
                return false;
            }
            else
            {
                CurrentFactor += stepUpFactorBy;
                int delta = CurrentFactor - lastSteppedFactor;
                Debug.WriteLine($"Stepped up load factor by {delta} from {lastSteppedFactor} to {CurrentFactor}");

                lastSteppedFactor = CurrentFactor;
                lastStepTime = DateTimeOffset.Now;
                return true;
            }
        }

        private bool CouldStep()
        {
            bool warmUpPeriodOver = IsWarmUpPeriodOver();
            bool noCoolDown = IsNotCoolingDown();
            return warmUpPeriodOver && noCoolDown;
        }

        private bool IsNotCoolingDown()
        {
            var now = DateTimeOffset.Now;
            TimeSpan elapsed = now - lastStepTime;
            bool coolDownElapsed = elapsed.CompareTo(coolDown) > 0;
            return coolDownElapsed;
        }

        public bool IsWarmUpPeriodOver()
        {
            long now = DateTimeOffset.Now.ToUnixTimeMilliseconds();
            long elapsed = now - startTimeMs;
            return elapsed > warmUp.TotalMilliseconds;
        }

        public bool IsMaxReached()
        {
            return CurrentFactor >= MaxFactor;
        }
    }
}