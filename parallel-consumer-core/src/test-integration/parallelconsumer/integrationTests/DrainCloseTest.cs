using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.ParallelConsumer;
using Confluent.ParallelConsumer.Options;
using Lombok;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using static Confluent.ParallelConsumer.ProcessingOrder;

namespace ParallelConsumer.IntegrationTests
{
    [TestClass]
    public class DrainCloseTest : BrokerIntegrationTest<string, string>
    {
        private Consumer<string, string> consumer;
        private ParallelConsumerOptions<string, string> pcOpts;
        private ParallelEoSStreamProcessor<string, string> pc;

        [TestInitialize]
        public void SetUp()
        {
            SetupTopic();
            consumer = GetKcu().CreateNewConsumer(KafkaClientUtils.GroupOption.NEW_GROUP);

            pcOpts = ParallelConsumerOptions<string, string>.Builder()
                .Consumer(consumer)
                .Ordering(PARTITION)
                .Build();

            pc = new ParallelEoSStreamProcessor<string, string>(pcOpts);

            pc.Subscribe(new HashSet<string> { topic });
        }

        [TestMethod]
        public async Task StopPollingAfterStateIsSetToDraining()
        {
            var recordsToProduce = 2L; // 1 in process + 1 waiting in shard queue
            var recordsToProduceAfterClose = 10L;

            var count = new AtomicLong();
            var latch = new CountdownEvent(1);

            GetKcu().ProduceMessages(topic, recordsToProduce);
            pc.Poll(recordContexts =>
            {
                count.GetAndIncrement();
                try
                {
                    latch.Wait();
                }
                catch (Exception e)
                {
                    throw new RuntimeException(e);
                }
                log.Debug("Processed record, count now {} - offset: {}", count, recordContexts.Offset());
            });
            await Task.Run(() => { while (count.Get() != 1L) { } });

            new Thread(() => pc.CloseDrainFirst(TimeSpan.FromSeconds(30))).Start();
            Thread.Sleep(2000);

            GetKcu().ProduceMessages(topic, recordsToProduceAfterClose);
            Thread.Sleep(5000);

            latch.Signal();

            await Task.Run(() => { while (!pc.IsClosedOrFailed() && count.Get() != recordsToProduce + recordsToProduceAfterClose) { } });
            Assert.AreEqual(recordsToProduce, count.Get());
            log.Debug("Test finished");
        }
    }
}