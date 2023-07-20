using System;

namespace io.confluent.parallelconsumer
{
    /**
     * Internal only view of the {@link RecordContext} class.
     */
    public class RecordContextInternal<K, V>
    {
        public RecordContext<K, V> RecordContext { get; }

        public RecordContextInternal(WorkContainer<K, V> wc)
        {
            this.RecordContext = new RecordContext<K, V>(wc);
        }

        public WorkContainer<K, V> GetWorkContainer()
        {
            return RecordContext.GetWorkContainer();
        }
    }
}