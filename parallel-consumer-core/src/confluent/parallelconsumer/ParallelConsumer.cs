using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using Confluent.Kafka;

namespace io.confluent.parallelconsumer
{
    /**
     * Asynchronous / concurrent message consumer for Kafka.
     * <p>
     * Currently, there is no direct implementation, only the {@link ParallelStreamProcessor} version (see
     * {@link AbstractParallelEoSStreamProcessor}), but there may be in the future.
     *
     * @param <K> key consume / produce key type
     * @param <V> value consume / produce value type
     * @see AbstractParallelEoSStreamProcessor
     */
    public interface ParallelConsumer<K, V> : DrainingCloseable
    {
        /**
         * @return true if the system has either closed, or has crashed
         */
        bool isClosedOrFailed();

        /**
         * @see KafkaConsumer#subscribe(Collection)
         */
        void subscribe(Collection<string> topics);

        /**
         * @see KafkaConsumer#subscribe(Pattern)
         */
        void subscribe(Regex pattern);

        /**
         * @see KafkaConsumer#subscribe(Collection, ConsumerRebalanceListener)
         */
        void subscribe(Collection<string> topics, ConsumerRebalanceListener callback);

        /**
         * @see KafkaConsumer#subscribe(Pattern, ConsumerRebalanceListener)
         */
        void subscribe(Regex pattern, ConsumerRebalanceListener callback);

        /**
         * Pause this consumer (i.e. stop processing of messages).
         * <p>
         * This operation only has an effect if the consumer is currently running. In all other cases calling this method
         * will be silent a no-op.
         * <p>
         * Once the consumer is paused, the system will stop submitting work to the processing pool. Already submitted in
         * flight work however will be finished. This includes work that is currently being processed inside a user function
         * as well as work that has already been submitted to the processing pool but has not been picked up by a free
         * worker yet.
         * <p>
         * General remarks:
         * <ul>
         * <li>A paused consumer may still keep polling for new work until internal buffers are filled.</li>
         * <li>This operation does not actively pause the subscription on the underlying Kafka Broker (compared to
         * {@link KafkaConsumer#pause KafkaConsumer#pause}).</li>
         * <li>Pending offset commits will still be performed when the consumer is paused.</li>
         * </p>
         */
        void pauseIfRunning();

        /**
         * Resume this consumer (i.e. continue processing of messages).
         * <p>
         * This operation only has an effect if the consumer is currently paused. In all other cases calling this method
         * will be a silent no-op.
         * </p>
         */
        void resumeIfPaused();

        /**
         * A simple tuple structure.
         *
         * @param <L>
         * @param <R>
         */
        class Tuple<L, R>
        {
            private readonly L left;
            private readonly R right;

            public Tuple(L left, R right)
            {
                this.left = left;
                this.right = right;
            }

            public static Tuple<LL, RR> pairOf<LL, RR>(LL l, RR r)
            {
                return new Tuple<LL, RR>(l, r);
            }
        }
    }
}