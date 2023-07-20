using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace io.confluent.parallelconsumer
{
    public interface JStreamParallelStreamProcessor<K, V> : DrainingCloseable
    {
        static JStreamParallelStreamProcessor<K, V> CreateJStreamEosStreamProcessor(ParallelConsumerOptions<K, V> options)
        {
            return new JStreamParallelEoSStreamProcessor<K, V>(options);
        }

        Stream<ParallelStreamProcessor.ConsumeProduceResult<K, V, K, V>> PollProduceAndStream(
            Func<PollContext<K, V>, List<ProducerRecord<K, V>>> userFunction);
    }
}