using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace io.confluent.parallelconsumer.@internal
{
    public class PausableWorkManager<K, V> : WorkManager<K, V>
    {
        private readonly Optional<CountdownEvent> optionalCountdownEvent;

        public PausableWorkManager(PCModule<K, V> module, DynamicLoadFactor dynamicExtraLoadFactor, CountdownEvent countdownEvent)
            : base(module, dynamicExtraLoadFactor)
        {
            optionalCountdownEvent = Optional.Of(countdownEvent);
        }

        public override List<WorkContainer<K, V>> GetWorkIfAvailable(int requestedMaxWorkToRetrieve)
        {
            var workContainers = base.GetWorkIfAvailable(requestedMaxWorkToRetrieve);
            if (workContainers.Count > 0)
            {
                optionalCountdownEvent.IfPresent(AwaitCountdownEvent);
            }
            return workContainers;
        }

        private void AwaitCountdownEvent(CountdownEvent countdownEvent)
        {
            try
            {
                countdownEvent.Wait(TimeSpan.FromSeconds(60));
            }
            catch (ThreadInterruptedException ex)
            {
                log.Debug("Expected the exception on rebalance, continue..", ex);
            }
        }
    }
}