using System;
using Xunit;
using Confluent.Kafka;
using Confluent.ParallelConsumer;

namespace ParallelConsumer.Tests
{
    public class ParallelConsumerOptionsTest
    {
        [Fact]
        public void SetTimeBetweenCommits()
        {
            var newFreq = TimeSpan.FromMilliseconds(100);
            var options = ParallelConsumerOptions<string, string>.Builder()
                .CommitInterval(newFreq)
                .Consumer(new LongPollingMockConsumer<string, string>(AutoOffsetReset.Earliest))
                .Build();

            Assert.Equal(newFreq, options.CommitInterval);

            var pc = new ParallelEoSStreamProcessor<string, string>(options);

            Assert.Equal(newFreq, pc.TimeBetweenCommits);

            var testFreq = TimeSpan.FromMilliseconds(9);
            pc.TimeBetweenCommits = testFreq;

            Assert.Equal(testFreq, pc.TimeBetweenCommits);
            Assert.Equal(testFreq, options.CommitInterval);
        }
    }
}