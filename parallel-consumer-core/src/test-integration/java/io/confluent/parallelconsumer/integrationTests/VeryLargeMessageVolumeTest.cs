using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Xunit;
using Xunit.Abstractions;
using System.Linq;
using System.Threading;
using System.Diagnostics;

namespace ParallelConsumer.IntegrationTests
{
    public class VeryLargeMessageVolumeTest : BrokerIntegrationTest<string, string>
    {
        private const int HIGH_MAX_POLL_RECORDS_CONFIG = 10_000;

        private readonly List<string> consumedKeys = new List<string>();
        private readonly List<string> producedKeysAcknowledged = new List<string>();
        private readonly AtomicInteger processedCount = new AtomicInteger(0);
        private readonly AtomicInteger producedCount = new AtomicInteger(0);

        public VeryLargeMessageVolumeTest(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void ShouldNotThrowBitSetTooLongException()
        {
            RunTest(HIGH_MAX_POLL_RECORDS_CONFIG, ParallelConsumerOptions.CommitMode.PeriodicConsumerAsync, ParallelConsumerOptions.ProcessingOrder.Key);
        }

        private void RunTest(int maxPoll, ParallelConsumerOptions.CommitMode commitMode, ParallelConsumerOptions.ProcessingOrder order)
        {
            string inputName = SetupTopic(GetType().Name + "-input-" + RandomUtils.Next());
            string outputName = SetupTopic(GetType().Name + "-output-" + RandomUtils.Next());

            // pre-produce messages to input-topic
            List<string> expectedKeys = new List<string>();
            long expectedMessageCount = 1_000_000;
            Output.WriteLine($"Producing {expectedMessageCount} messages before starting test");
            List<Task<DeliveryResult<string, string>>> sends = new List<Task<DeliveryResult<string, string>>>();
            using (var kafkaProducer = GetKcu().CreateNewProducer(false))
            {
                for (int i = 0; i < expectedMessageCount; i++)
                {
                    string key = "key-" + i;
                    Task<DeliveryResult<string, string>> send = kafkaProducer.ProduceAsync(inputName, new Message<string, string> { Key = key, Value = "value-" + i });
                    sends.Add(send);
                    expectedKeys.Add(key);
                }
                Output.WriteLine("Finished sending test data");
            }
            // make sure we finish sending before next stage
            Output.WriteLine("Waiting for broker acks");
            Task.WaitAll(sends.ToArray());
            Assert.Equal(expectedMessageCount, sends.Count);

            // run parallel-consumer
            Output.WriteLine("Starting test");
            using (var newProducer = GetKcu().CreateNewProducer(commitMode == ParallelConsumerOptions.CommitMode.PeriodicTransactionalProducer))
            using (var newConsumer = GetKcu().CreateNewConsumer(true, new Dictionary<string, string> { { ConsumerConfig.MaxPollRecordsConfig, maxPoll.ToString() } }))
            {
                var pc = new ParallelEoSStreamProcessor<string, string>(ParallelConsumerOptions<string, string>.Builder()
                    .Ordering(order)
                    .Consumer(newConsumer)
                    .Producer(newProducer)
                    .CommitMode(commitMode)
                    .MaxConcurrency(1000)
                    .Build());
                pc.Subscribe(new List<string> { inputName });

                // sanity
                TopicPartition tp = new TopicPartition(inputName, 0);
                var beginOffsets = newConsumer.GetWatermarkOffsets(tp);
                var endOffsets = newConsumer.GetWatermarkOffsets(tp);
                Assert.Equal(expectedMessageCount, endOffsets[tp].Offset);
                Assert.Equal(0L, beginOffsets[tp].Offset);

                var bar = new ProgressBar(ProgressBarUtils.GetNewMessagesBar(expectedMessageCount), ProgressBarStyle.ASCII);
                pc.PollAndProduce(record =>
                {
                    bar.Tick();
                    consumedKeys.Add(record.Key);
                    processedCount.IncrementAndGet();
                    return new Message<string, string> { Key = record.Key, Value = "data" };
                }, consumeProduceResult =>
                {
                    producedCount.IncrementAndGet();
                    producedKeysAcknowledged.Add(consumeProduceResult.In.Key);
                });

                // wait for all pre-produced messages to be processed and produced
                var failureMessage = $"All keys sent to input-topic should be processed and produced, within time (expected: {expectedMessageCount} commit: {commitMode} order: {order} max poll: {maxPoll})";
                try
                {
                    var stopwatch = Stopwatch.StartNew();
                    while (stopwatch.Elapsed < TimeSpan.FromSeconds(120))
                    {
                        if (pc.IsClosedOrFailed())
                        {
                            throw new Exception("PC died - check logs");
                        }

                        Thread.Sleep(1000);

                        lock (consumedKeys)
                        {
                            if (consumedKeys.Count == expectedKeys.Count && producedKeysAcknowledged.Count == expectedKeys.Count)
                            {
                                break;
                            }
                        }
                    }

                    Assert.Equal(expectedKeys.Count, consumedKeys.Count);
                    Assert.Equal(expectedKeys.Count, producedKeysAcknowledged.Count);
                }
                catch (Exception e)
                {
                    throw new Exception(failureMessage, e);
                }

                bar.Dispose();

                pc.CloseDrainFirst();

                Assert.Equal(processedCount.Value, producedCount.Value);

                // sanity
                Assert.Equal(expectedMessageCount, processedCount.Value);
                Assert.Equal(expectedKeys.Count, producedKeysAcknowledged.Count);
            }
        }
    }
}