using System;
using Xunit;
using Moq;
using Confluent.Kafka;
using Confluent.ParallelConsumer;
using Confluent.ParallelConsumer.Internal;
using Confluent.ParallelConsumer.State;

namespace Confluent.ParallelConsumer.Tests.State
{
    public class WorkContainerTest
    {
        [Fact]
        public void Basics()
        {
            var workContainer = new ModelUtils(new PCModuleTestEnv()).CreateWorkFor(0);
            Assert.True(workContainer.GetDelayUntilRetryDue().IsNegative);
        }

        [Fact]
        public void RetryDelayProvider()
        {
            int uniqueMultiplier = 7;

            Func<RecordContext<string, string>, TimeSpan> retryDelayProvider = context =>
            {
                var numberOfFailedAttempts = context.NumberOfFailedAttempts;
                return TimeSpan.FromSeconds(numberOfFailedAttempts * uniqueMultiplier);
            };

            var opts = new ParallelConsumerOptions<string, string>().WithRetryDelayProvider(retryDelayProvider);
            var module = new PCModuleTestEnv(opts);

            var wc = new WorkContainer<string, string>(0,
                Mock.Of<ConsumeResult<string, string>>(),
                module);

            int numberOfFailures = 3;
            wc.OnUserFunctionFailure(new FakeRuntimeException(""));
            wc.OnUserFunctionFailure(new FakeRuntimeException(""));
            wc.OnUserFunctionFailure(new FakeRuntimeException(""));

            var retryDelayConfig = wc.RetryDelayConfig;

            Assert.Equal(numberOfFailures * uniqueMultiplier, retryDelayConfig.TotalSeconds);
        }
    }
}