using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.ParallelConsumer.Internal;
using Confluent.ParallelConsumer.Internal.ProducerManager;
using Microsoft.Extensions.Logging;

namespace Confluent.ParallelConsumer
{
    public class ParallelEoSStreamProcessor<K, V> : AbstractParallelEoSStreamProcessor<K, V>, IParallelStreamProcessor<K, V>
    {
        public ParallelEoSStreamProcessor(ParallelConsumerOptions<K, V> newOptions, PCModule<K, V> module) : base(newOptions, module)
        {
        }

        public ParallelEoSStreamProcessor(ParallelConsumerOptions<K, V> newOptions) : base(newOptions)
        {
        }

        public void Poll(Action<PollContext<K, V>> usersVoidConsumptionFunction)
        {
            Func<PollContextInternal<K, V>, List<object>> wrappedUserFunc = (context) =>
            {
                Log.Trace("asyncPoll - Consumed a consumerRecord ({0}), executing void function...", context);

                CarefullyRun(usersVoidConsumptionFunction, context.PollContext);

                Log.Trace("asyncPoll - user function finished ok.");
                return new List<object>(); // user function returns no produce records, so we satisfy our api
            };
            Action<object> voidCallBack = (ignore) => Log.Trace("Void callback applied.");
            SupervisorLoop(wrappedUserFunc, voidCallBack);
        }

        public void PollAndProduceMany(Func<PollContext<K, V>, List<ProducerRecord<K, V>>> userFunction, Action<ConsumeProduceResult<K, V, K, V>> callback)
        {
            if (!Options.IsProducerSupplied)
            {
                throw new ArgumentException("To use the produce flows you must supply a Producer in the options");
            }

            // wrap user func to add produce function
            Func<PollContextInternal<K, V>, List<ConsumeProduceResult<K, V, K, V>>> producingUserFunctionWrapper =
                context => ProcessAndProduceResults(userFunction, context);

            SupervisorLoop(producingUserFunctionWrapper, callback);
        }

        private List<ConsumeProduceResult<K, V, K, V>> ProcessAndProduceResults(Func<PollContext<K, V>, List<ProducerRecord<K, V>>> userFunction, PollContextInternal<K, V> context)
        {
            ProducerManager<K, V> pm = base.GetProducerManager().Value;

            // if running strict with no processing during commit - get the produce lock first
            if (Options.IsUsingTransactionCommitMode && !Options.IsAllowEagerProcessingDuringTransactionCommit)
            {
                try
                {
                    ProducerManager<K, V>.ProducingLock produceLock = pm.BeginProducing(context);
                    context.ProducingLock = produceLock;
                }
                catch (TimeoutException e)
                {
                    throw new Exception($"Timeout trying to early acquire produce lock to send record in {CommitMode.PeriodicTransactionalProducer} mode - could not START record processing phase", e);
                }
            }

            // run the user function, which is expected to return records to be sent
            List<ProducerRecord<K, V>> recordListToProduce = CarefullyRun(userFunction, context.PollContext);

            if (recordListToProduce.Count == 0)
            {
                Log.Debug("No result returned from function to send.");
                return new List<ConsumeProduceResult<K, V, K, V>>();
            }
            Log.Trace("asyncPoll and Stream - Consumed a record ({0}), and returning a derivative result record to be produced: {1}", context, recordListToProduce);

            List<ConsumeProduceResult<K, V, K, V>> results = new List<ConsumeProduceResult<K, V, K, V>>();
            Log.Trace("Producing {0} messages in result...", recordListToProduce.Count);

            // by having the produce lock span the block on acks, means starting a commit cycle blocks until ack wait is finished
            if (Options.IsUsingTransactionCommitMode && Options.IsAllowEagerProcessingDuringTransactionCommit)
            {
                try
                {
                    ProducerManager<K, V>.ProducingLock produceLock = pm.BeginProducing(context);
                    context.ProducingLock = produceLock;
                }
                catch (TimeoutException e)
                {
                    throw new Exception($"Timeout trying to late acquire produce lock to send record in {CommitMode.PeriodicTransactionalProducer} mode", e);
                }
            }

            // wait for all acks to complete, see PR #356 for a fully async version which doesn't need to block here
            try
            {
                var futures = pm.ProduceMessages(recordListToProduce);

                TimeUtils.Time(() =>
                {
                    foreach (var futureTuple in futures)
                    {
                        Future<RecordMetadata> futureSend = futureTuple.Value;

                        var recordMetadata = futureSend.Get(Options.SendTimeout.TotalMilliseconds);

                        var result = new ConsumeProduceResult<K, V, K, V>(context.PollContext, futureTuple.Key, recordMetadata);
                        results.Add(result);
                    }
                    return null; // return from timer function
                });
            }
            catch (Exception e)
            {
                throw new Exception("Error while waiting for produce results", e);
            }
            return results;
        }

        public void PollAndProduceMany(Func<PollContext<K, V>, List<ProducerRecord<K, V>>> userFunction)
        {
            PollAndProduceMany(userFunction, consumerRecord =>
            {
                // no op call back
                Log.Trace("No-op user callback");
            });
        }

        public void PollAndProduce(Func<PollContext<K, V>, ProducerRecord<K, V>> userFunction)
        {
            PollAndProduce(userFunction, consumerRecord =>
            {
                // no op call back
                Log.Trace("No-op user callback");
            });
        }

        public void PollAndProduce(Func<PollContext<K, V>, ProducerRecord<K, V>> userFunction, Action<ConsumeProduceResult<K, V, K, V>> callback)
        {
            PollAndProduceMany(consumerRecord => new List<ProducerRecord<K, V>> { userFunction(consumerRecord) }, callback);
        }
    }
}