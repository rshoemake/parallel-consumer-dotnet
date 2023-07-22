namespace Confluent.ParallelConsumer.Internal
{
    /// <summary>
    /// The run state of the controller.
    /// </summary>
    public enum State
    {
        UNUSED,
        RUNNING,
        /// <summary>
        /// When paused, the system will stop submitting work to the processing pool. Polling for new work however may
        /// continue until internal buffers have been filled sufficiently and the auto-throttling takes effect. In flight
        /// work will not be affected by transitioning to this state (i.e. processing will finish without any interrupts
        /// being sent).
        /// </summary>
        PAUSED,
        /// <summary>
        /// When draining, the system will stop polling for more records, but will attempt to process all already downloaded
        /// records. Note that if you choose to close without draining, records already processed will still be committed
        /// first before closing.
        /// </summary>
        DRAINING,
        CLOSING,
        CLOSED
    }
}