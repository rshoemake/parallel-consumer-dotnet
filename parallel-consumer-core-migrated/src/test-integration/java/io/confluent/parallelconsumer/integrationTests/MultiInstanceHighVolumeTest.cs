using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using Confluent.Kafka;
using Xunit;
using Xunit.Abstractions;

namespace ParallelConsumer.IntegrationTests
{
    public class MultiInstanceHighVolumeTest : BrokerIntegrationTest<string, string>
    {
        public List<string> consumedKeys = new List<string>();
        public List<string> producedKeysAcknowledged = new List<string>();
        public AtomicInteger processedCount = new AtomicInteger(0);
        public AtomicInteger producedCount = new AtomicInteger(0);

        int maxPoll = 500; // 500 is the kafka default

        CommitMode commitMode = CommitMode.PERIODIC_CONSUMER_SYNC;
        ProcessingOrder order = ProcessingOrder.KEY;

        public MultiInstanceHighVolumeTest(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task MultiInstance()
        {
            numPartitions = 12;
            string inputTopicName = await SetupTopicAsync(GetType().Name + "-input");

            int expectedMessageCount = 30_000_00;
            _output.WriteLine($"Producing {expectedMessageCount} messages before starting test");

            List<string> expectedKeys = await GetKcu().ProduceMessagesAsync(inputTopicName, expectedMessageCount);

            // setup
            ParallelEoSStreamProcessor<string, string> pcOne = BuildPc(inputTopicName, maxPoll, order, commitMode);
            ParallelEoSStreamProcessor<string, string> pcTwo = BuildPc(inputTopicName, maxPoll, order, commitMode);
            ParallelEoSStreamProcessor<string, string> pcThree = BuildPc(inputTopicName, maxPoll, order, commitMode);

            // run
            var consumedByOne = new ConcurrentBag<ConsumerRecord<string, string>>();
            var consumedByTwo = new ConcurrentBag<ConsumerRecord<string, string>>();
            var consumedByThree = new ConcurrentBag<ConsumerRecord<string, string>>();
            List<Task> tasks = new List<Task>();
            tasks.Add(RunAsync(expectedMessageCount / 3, pcOne, consumedByOne));
            tasks.Add(RunAsync(expectedMessageCount / 3, pcTwo, consumedByTwo));
            tasks.Add(RunAsync(expectedMessageCount / 3, pcThree, consumedByThree));

            // wait for all pre-produced messages to be processed and produced
            var failureMessage = $"All keys sent to input-topic should be processed and produced, within time (expected: {expectedMessageCount} commit: {commitMode} order: {order} max poll: {maxPoll})";
            try
            {
                await Wait.AtMostAsync(TimeSpan.FromSeconds(60))
                    .FailFast("PC died - check logs", () => pcThree.IsClosedOrFailed())
                    .Alias(failureMessage)
                    .PollInterval(TimeSpan.FromSeconds(1))
                    .Until(() =>
                    {
                        _output.WriteLine($"Processed-count: {processedCount.Get()}, Produced-count: {producedCount.Get()}");
                        Assert.Equal(expectedKeys.Count, consumedKeys.Count);
                        Assert.Equal(expectedKeys.Count, producedKeysAcknowledged.Count);
                    });
            }
            catch (ConditionTimeoutException e)
            {
                Assert.True(false, failureMessage + "\n" + e.Message);
            }

            Assert.Equal(expectedMessageCount, processedCount.Get());

            // sanity
            Assert.Equal(expectedMessageCount, processedCount.Get());

            await Task.WhenAll(tasks);

            pcOne.Dispose();
            pcTwo.Dispose();
            pcThree.Dispose();
        }

        private ParallelEoSStreamProcessor<string, string> BuildPc(string inputTopicName, int maxPoll, ProcessingOrder order, CommitMode commitMode)
        {
            var pc = GetKcu().BuildPc(order, commitMode, maxPoll);
            pc.Subscribe(new List<string> { inputTopicName });
            return pc;
        }

        private async Task RunAsync(int expectedMessageCount, ParallelEoSStreamProcessor<string, string> pc, ConcurrentBag<ConsumerRecord<string, string>> consumed)
        {
            var bar = ProgressBarUtils.GetNewMessagesBar(_output, expectedMessageCount);
            bar.SetExtraMessage("#" + pc.Id);
            pc.SetMyId("id: " + pc.Id);
            await pc.PollAsync(record =>
            {
                ProcessRecord(bar, record.Message, consumed);
            });
            bar.Dispose();
        }

        private void ProcessRecord(ProgressBar bar, Message<string, string> record, ConcurrentBag<ConsumerRecord<string, string>> consumed)
        {
            bar.Tick();
            consumedKeys.Add(record.Key);
            processedCount.IncrementAndGet();
            consumed.Add(record);
        }
    }
}