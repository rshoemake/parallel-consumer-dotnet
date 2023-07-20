using System;
using System.Collections.Generic;

namespace Confluent.ParallelConsumer
{
    public interface IParallelStreamProcessor<K, V> : IParallelConsumer<K, V>, IDisposable
    {
        void Poll(Action<PollContext<K, V>> usersVoidConsumptionFunction);
        void PollAndProduceMany(Func<PollContext<K, V>, List<ProducerRecord<K, V>>> userFunction, Action<ConsumeProduceResult<K, V, K, V>> callback);
        void PollAndProduceMany(Func<PollContext<K, V>, List<ProducerRecord<K, V>>> userFunction);
        void PollAndProduce(Func<PollContext<K, V>, ProducerRecord<K, V>> userFunction);
        void PollAndProduce(Func<PollContext<K, V>, ProducerRecord<K, V>> userFunction, Action<ConsumeProduceResult<K, V, K, V>> callback);

        class ConsumeProduceResult<K, V, KK, VV>
        {
            public PollContext<K, V> In { get; }
            public ProducerRecord<KK, VV> Out { get; }
            public RecordMetadata Meta { get; }

            public ConsumeProduceResult(PollContext<K, V> inContext, ProducerRecord<KK, VV> outRecord, RecordMetadata metadata)
            {
                In = inContext;
                Out = outRecord;
                Meta = metadata;
            }
        }
    }
}