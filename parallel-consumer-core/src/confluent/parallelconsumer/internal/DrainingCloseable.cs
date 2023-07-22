using System;
using System.IO;
using System.Threading.Tasks;

namespace io.confluent.parallelconsumer.internal
{
    public interface DrainingCloseable : IDisposable
    {
        TimeSpan DEFAULT_TIMEOUT = TimeSpan.FromSeconds(30); // can increase if debugging

        enum DrainingMode
        {
            /**
             * Stop downloading more messages from the Broker, but finish processing what has already been queued.
             */
            DRAIN,
            /**
             * Stop downloading more messages, and stop procesing more messages in the queue, but finish processing messages
             * already being processed locally.
             */
            DONT_DRAIN
        }

        /**
         * Close the consumer, without draining. Uses a reasonable default timeout.
         *
         * @see #DEFAULT_TIMEOUT
         * @see #close(TimeSpan, DrainingMode)
         */
        default void Close()
        {
            CloseDontDrainFirst();
        }

        /**
         * @see DrainingMode#DRAIN
         */
        default void CloseDrainFirst()
        {
            CloseDrainFirst(DEFAULT_TIMEOUT);
        }

        /**
         * @see DrainingMode#DONT_DRAIN
         */
        default void CloseDontDrainFirst()
        {
            CloseDontDrainFirst(DEFAULT_TIMEOUT);
        }

        /**
         * @see DrainingMode#DRAIN
         */
        default void CloseDrainFirst(TimeSpan timeout)
        {
            Close(timeout, DrainingMode.DRAIN);
        }

        /**
         * @see DrainingMode#DONT_DRAIN
         */
        default void CloseDontDrainFirst(TimeSpan timeout)
        {
            Close(timeout, DrainingMode.DONT_DRAIN);
        }

        /**
         * Close the consumer.
         *
         * @param timeout      how long to wait before giving up
         * @param drainingMode wait for messages already consumed from the broker to be processed before closing
         */
        void Close(TimeSpan timeout, DrainingMode drainingMode);

        /**
         * Of the records consumed from the broker, how many do we have remaining in our local queues
         *
         * @return the number of consumed but outstanding records to process
         */
        long WorkRemaining();
    }
}