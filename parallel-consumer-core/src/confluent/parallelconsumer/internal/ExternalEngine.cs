using System.Collections.Generic;
using System.Threading.Tasks;
using Confluent.ParallelConsumer;
using Confluent.ParallelConsumer.Internal;
using Microsoft.Extensions.Logging;

namespace Confluent.ParallelConsumer.Internal
{
    internal abstract class ExternalEngine<K, V> : AbstractParallelEoSStreamProcessor<K, V>
    {
        protected ExternalEngine(ParallelConsumerOptions<K, V> newOptions) : base(newOptions)
        {
            Validate(options);
        }

        private void Validate(ParallelConsumerOptions<K, V> options)
        {
            if (options.UsingTransactionCommitMode)
            {
                throw new System.InvalidOperationException($"External engines (such as Vert.x and Reactor) do not support transactions / EoS ({ParallelConsumerOptions<K, V>.CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER})");
            }
        }

        protected override int GetTargetOutForProcessing()
        {
            return Options.TargetAmountOfRecordsInFlight;
        }

        protected override void CheckPipelinePressure()
        {
            // no-op - as calculateQuantityToRequest does not use a pressure system, unlike the core module
        }

        protected override Task OnUserFunctionSuccessAsync(WorkContainer<K, V> wc, List<object> resultsFromUserFunction)
        {
            if (IsAsyncFutureWork(resultsFromUserFunction))
            {
                Logger.LogDebug("Reactor creation function success, user's function success");
                return Task.CompletedTask;
            }
            else
            {
                return base.OnUserFunctionSuccessAsync(wc, resultsFromUserFunction);
            }
        }

        protected override void AddToMailBoxOnUserFunctionSuccess(PollContextInternal<K, V> context, WorkContainer<K, V> wc, List<object> resultsFromUserFunction)
        {
            if (IsAsyncFutureWork(resultsFromUserFunction))
            {
                Logger.LogDebug("User function success but not adding vertx vertical to mailbox yet");
            }
            else
            {
                base.AddToMailBoxOnUserFunctionSuccess(context, wc, resultsFromUserFunction);
            }
        }

        protected abstract bool IsAsyncFutureWork(List<object> resultsFromUserFunction);
    }
}