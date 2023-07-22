using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Confluent.Kafka;
using Confluent.ParallelConsumer;
using Microsoft.Extensions.Logging;
using Moq;
using Xunit;

namespace io.confluent.parallelconsumer
{
    public class JStreamParallelEoSStreamProcessorTest : ParallelEoSStreamProcessorTestBase
    {
        JStreamParallelEoSStreamProcessor<string, string> streaming;

        public override void SetupData()
        {
            base.PrimeFirstRecord();
        }

        protected override ParallelEoSStreamProcessor<string, string> InitAsyncConsumer(ParallelConsumerOptions<string, string> options)
        {
            streaming = new JStreamParallelEoSStreamProcessor<string, string>(options);

            return streaming;
        }

        [Fact]
        public void TestStream()
        {
            var latch = new CountdownEvent(1);
            var streamedResults = streaming.PollProduceAndStream((record) =>
            {
                var mock = new Mock<ProducerRecord<string, string>>();
                Log.LogInformation("Consumed and produced record ({0}), and returning a derivative result to produce to output topic: {1}", record, mock);
                MyRecordProcessingAction(record.SingleConsumerRecord);
                latch.Signal();
                return new List<ProducerRecord<string, string>> { mock.Object };
            });

            latch.Wait();

            AwaitForSomeLoopCycles(2);

            Mock.Get(MyRecordProcessingAction).Verify(x => x(It.IsAny<ConsumerRecord<string, string>>()), Times.Once);

            Assert.Equal(1, streamedResults.Count());
        }

        [Fact]
        public void TestConsumeAndProduce()
        {
            var latch = new CountdownEvent(1);
            var stream = streaming.PollProduceAndStream((record) =>
            {
                var apply = MyRecordProcessingAction(record.SingleConsumerRecord);
                var result = new ProducerRecord<string, string>(OUTPUT_TOPIC, "akey", apply);
                Log.LogInformation("Consumed a record ({0}), and returning a derivative result record to be produced: {1}", record, result);
                latch.Signal();
                return new List<ProducerRecord<string, string>> { result };
            });

            latch.Wait();

            ResumeControlLoop();

            AwaitForSomeLoopCycles(1);

            Mock.Get(MyRecordProcessingAction).Verify(x => x(It.IsAny<ConsumerRecord<string, string>>()), Times.Once);

            var myResultStream = stream.Select(x =>
            {
                if (x != null)
                {
                    var left = x.In.SingleConsumerRecord;
                    Log.LogInformation("{0}:{1}:{2}:{3}", left.Key, left.Value, x.Out, x.Meta);
                }
                else
                {
                    Log.LogInformation("null");
                }
                return x;
            });

            Assert.Equal(1, myResultStream.Count());
        }

        [Fact]
        public void TestFlatMapProduce()
        {
            var latch = new CountdownEvent(1);
            var myResultStream = streaming.PollProduceAndStream((record) =>
            {
                var apply1 = MyRecordProcessingAction(record.SingleConsumerRecord);
                var apply2 = MyRecordProcessingAction(record.SingleConsumerRecord);

                var list = new List<ProducerRecord<string, string>>
                {
                    new ProducerRecord<string, string>(OUTPUT_TOPIC, "key", apply1),
                    new ProducerRecord<string, string>(OUTPUT_TOPIC, "key", apply2)
                };

                latch.Signal();
                return list;
            });

            latch.Wait();

            AwaitForSomeLoopCycles(1);

            Mock.Get(MyRecordProcessingAction).Verify(x => x(It.IsAny<ConsumerRecord<string, string>>()), Times.Exactly(2));

            Assert.Equal(2, myResultStream.Count());
        }
    }
}