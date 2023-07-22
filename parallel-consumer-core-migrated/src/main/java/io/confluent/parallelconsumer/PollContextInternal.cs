using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace io.confluent.parallelconsumer
{
    /**
     * Internal only view on the {@link PollContext}.
     */
    public class PollContextInternal<K, V>
    {
        private readonly PollContext<K, V> pollContext;

        /**
         * Used when running in {@link ParallelConsumerOptions.CommitMode#isUsingTransactionCommitMode()} then the produce
         * lock will be passed around here. It needs to be unlocked when work has been put back in the inbox.
         */
        public Optional<ProducerManager<K, V>.ProducingLock> ProducingLock { get; set; }

        public PollContextInternal(List<WorkContainer<K, V>> workContainers)
        {
            this.pollContext = new PollContext<K, V>(workContainers);
        }

        /**
         * @return a stream of {@link WorkContainer}s
         */
        public Stream<WorkContainer<K, V>> StreamWorkContainers()
        {
            return pollContext.StreamInternal().Map(rci => rci.WorkContainer);
        }

        /**
         * @return a flat {@link List} of {@link WorkContainer}s, which wrap the {@link ConsumerRecord}s in this result set
         */
        public List<WorkContainer<K, V>> GetWorkContainers()
        {
            return StreamWorkContainers().Collect(Collectors.ToList());
        }
    }
}