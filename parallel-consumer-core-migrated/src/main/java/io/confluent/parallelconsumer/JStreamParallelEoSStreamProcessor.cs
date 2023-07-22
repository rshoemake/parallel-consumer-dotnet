using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace io.confluent.parallelconsumer
{
    public class JStreamParallelEoSStreamProcessor<K, V> : ParallelEoSStreamProcessor<K, V>, JStreamParallelStreamProcessor<K, V>
    {
        private readonly IEnumerable<ConsumeProduceResult<K, V, K, V>> stream;
        private readonly ConcurrentQueue<ConsumeProduceResult<K, V, K, V>> userProcessResultsStream;

        public JStreamParallelEoSStreamProcessor(ParallelConsumerOptions<K, V> parallelConsumerOptions) : base(parallelConsumerOptions)
        {
            userProcessResultsStream = new ConcurrentQueue<ConsumeProduceResult<K, V, K, V>>();
            stream = SetupStreamFromQueue(userProcessResultsStream);
        }

        public IEnumerable<ConsumeProduceResult<K, V, K, V>> PollProduceAndStream(Func<PollContext<K, V>, List<ProducerRecord<K, V>>> userFunction)
        {
            base.PollAndProduceMany(userFunction, result =>
            {
                Console.WriteLine($"Wrapper callback applied, sending result to stream. Input: {result}");
                userProcessResultsStream.Enqueue(result);
            });

            return stream;
        }

        private IEnumerable<T> SetupStreamFromQueue<T>(ConcurrentQueue<T> queue)
        {
            while (!queue.IsEmpty)
            {
                if (queue.TryDequeue(out var item))
                {
                    yield return item;
                }
            }
        }
    }
}