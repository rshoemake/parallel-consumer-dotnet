using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.ParallelConsumer;
using Confluent.ParallelConsumer.Options;
using Lombok;
using Microsoft.Extensions.Logging;
using Xunit;
using Xunit.Abstractions;

namespace io.confluent.parallelconsumer.integrationTests
{
    public class LoadTest : DbTest
    {
        private static int total = 4000;

        [SneakyThrows]
        public void SetupTestData()
        {
            SetupTopic();
            PublishMessages(total / 100, total, topic);
        }

        [SneakyThrows]
        [Fact]
        public void TimedNormalKafkaConsumerTest()
        {
            SetupTestData();
            GetKcu().GetConsumer().Subscribe(new List<string> { topic });
            ReadRecordsPlainConsumer(total, topic);
        }

        [SneakyThrows]
        [Fact]
        public void AsyncConsumeAndProcess()
        {
            SetupTestData();
            var newConsumer = GetKcu().CreateNewConsumer();
            var tx = true;
            var options = new ParallelConsumerOptions<string, string>()
            {
                Ordering = ProcessingOrder.Key,
                CommitMode = CommitMode.PeriodicTransactionalProducer,
                Producer = GetKcu().CreateNewProducer(tx),
                Consumer = newConsumer,
                MaxConcurrency = 3
            };
            var async = new ParallelEoSStreamProcessor<string, string>(options);
            async.Subscribe(new Regex(topic));

            var msgCount = new AtomicInteger(0);
            var pb = ProgressBarUtils.GetNewMessagesBar(log, total);

            using (pb)
            {
                async.Poll(r =>
                {
                    SleepABit();
                    msgCount.GetAndIncrement();
                });

                await().AtMost(TimeSpan.FromSeconds(60)).Until(() =>
                {
                    pb.StepTo(msgCount.Get());
                    return msgCount.Get() >= total;
                });
            }
            async.Close();
        }

        private void SleepABit()
        {
            var simulatedCPUMessageProcessingDelay = new Random().Next(0, 5);
            Thread.Sleep(simulatedCPUMessageProcessingDelay);
        }

        private void ReadRecordsPlainConsumer(int total, string topic)
        {
            log.Info("Starting to read back");
            var allRecords = new List<ConsumeResult<string, string>>();
            var count = new AtomicInteger();
            Time(() =>
            {
                var pb = ProgressBarUtils.GetNewMessagesBar(log, total);
                Task.Run(() =>
                {
                    while (allRecords.Count < total)
                    {
                        var poll = GetKcu().GetConsumer().Consume(TimeSpan.FromMilliseconds(500));
                        var records = poll.Topic == topic ? new List<ConsumeResult<string, string>> { poll } : new List<ConsumeResult<string, string>>();
                        records.ForEach(x =>
                        {
                            SleepABit();
                            pb.Step();
                        });
                        allRecords.AddRange(records);
                        count.GetAndAdd(records.Count);
                    }
                });

                using (pb)
                {
                    await().AtMost(TimeSpan.FromSeconds(60)).UntilAsserted(() =>
                    {
                        Assert.Equal(total, count.Get());
                    });
                }
            });

            Assert.Equal(total, allRecords.Count);
        }

        [SneakyThrows]
        private void PublishMessages(int keyRange, int total, string topic)
        {
            var keys = Enumerable.Range(0, keyRange).ToList();
            var integers = Enumerable.Range(0, total).ToList();
            var futureMetadataResultsFromPublishing = new LinkedList<Task<DeliveryResult<string, string>>>();
            log.Info("Start publishing...");
            Time(() =>
            {
                foreach (var x in ProgressBarUtils.Wrap(integers, "Publishing async"))
                {
                    var key = keys[new Random().Next(0, keys.Count)].ToString();
                    var messageSizeInBytes = 500;
                    var value = new string(Enumerable.Repeat("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789", messageSizeInBytes).Select(s => s[new Random().Next(s.Length)]).ToArray());
                    var producerRecord = new Message<string, string> { Key = key, Value = value };
                    try
                    {
                        var meta = GetKcu().GetProducer().ProduceAsync(topic, producerRecord);
                        futureMetadataResultsFromPublishing.Add(meta);
                    }
                    catch (Exception e)
                    {
                        throw new Exception(e.Message);
                    }
                }
            });

            var usedPartitions = new HashSet<int>();
            foreach (var meta in ProgressBarUtils.Wrap(futureMetadataResultsFromPublishing, "Joining"))
            {
                var recordMetadata = meta.GetAwaiter().GetResult();
                var partition = recordMetadata.Partition;
                usedPartitions.Add(partition);
            }

            if (numPartitions > 100000)
            {
                Assert.Equal(numPartitions, usedPartitions.Distinct().Count());
            }
        }
    }
}