using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.ParallelConsumer;
using Confluent.ParallelConsumer.OffsetTracking;
using Confluent.ParallelConsumer.Options;
using Confluent.ParallelConsumer.State;
using Microsoft.Extensions.Logging;
using Xunit;

namespace ParallelConsumer.IntegrationTests
{
    public class TransactionMarkersTest : BrokerIntegrationTest<string, string>
    {
        private const int LIMIT = 1;
        private readonly AtomicInteger receivedRecordCount = new AtomicInteger();
        private IProducer<string, string> txProducer;
        private IProducer<string, string> txProducerTwo;
        private IProducer<string, string> txProducerThree;
        private IProducer<string, string> normalProducer;
        private IConsumer<string, string> consumer;
        private ParallelEoSStreamProcessor<string, string> pc;

        protected override void Setup()
        {
            base.Setup();
            consumer = GetKcu().GetConsumer();
            txProducer = GetKcu().CreateAndInitNewTransactionalProducer();
            txProducerTwo = GetKcu().CreateAndInitNewTransactionalProducer();
            txProducerThree = GetKcu().CreateAndInitNewTransactionalProducer();
            normalProducer = GetKcu().CreateNewProducer(ProducerMode.NotTransactional);
            pc = new ParallelEoSStreamProcessor<string, string>(ParallelConsumerOptions<string, string>.Builder()
                .Consumer(consumer)
                .Ordering(ProcessingOrder.Partition)
                .Build());
            pc.Subscribe(new HashSet<string> { Topic });
        }

        protected override void Close()
        {
            pc.Close();
        }

        [Fact]
        public void Single()
        {
            SendOneTransaction();
            SendRecordsNonTransactionally(1);
            RunPcAndBlockRecordsOverLimitIndex();
            WaitForRecordsToBeReceived();
            pc.Close();
        }

        [Fact]
        public void DoubleTransaction()
        {
            SendOneTransaction();
            SendOneTransaction();
            RunPcAndBlockRecordsOverLimitIndex();
            WaitForRecordsToBeReceived();
            pc.Close();
        }

        private void WaitForRecordsToBeReceived()
        {
            int expected = 2;
            WaitForRecordsToBeReceived(expected);
        }

        private void WaitForRecordsToBeReceived(int expected)
        {
            await Task.Run(() =>
            {
                while (receivedRecordCount.Get() < expected)
                {
                    Thread.Sleep(100);
                }
            });
        }

        private void RunPcAndBlockRecordsOverLimitIndex()
        {
            int blockOver = LIMIT;
            RunPcAndBlockRecordsOverLimitIndex(blockOver);
        }

        private void RunPcAndBlockRecordsOverLimitIndex(int blockOver)
        {
            pc.Poll(recordContexts =>
            {
                int index = receivedRecordCount.IncrementAndGet();
                if (index > blockOver)
                {
                    Thread.Sleep(Timeout.Infinite);
                }
            });
        }

        private void SendOneTransaction()
        {
            txProducer.InitTransactions(TimeSpan.FromSeconds(10));
            txProducer.BeginTransaction();
            txProducer.Produce(new TopicPartition(Topic, Partition.Any), new Message<string, string> { Key = "", Value = "" });
            txProducer.CommitTransaction(TimeSpan.FromSeconds(10));
        }

        private void SendRecordsNonTransactionally(int count)
        {
            for (int i = 0; i < count; i++)
            {
                normalProducer.Produce(new TopicPartition(Topic, Partition.Any), new Message<string, string> { Key = "", Value = "" });
            }
        }

        [Fact]
        public void Several()
        {
            SendSeveralTransaction();
            SendRecordsNonTransactionally(10);
            RunPcAndBlockRecordsOverLimitIndex();
            WaitForRecordsToBeReceived();
            pc.Close();
        }

        private void SendSeveralTransaction()
        {
            for (int i = 0; i < 10; i++)
            {
                SendOneTransaction();
            }
        }

        [Fact]
        public void DontBlockFirstRecords()
        {
            SendSeveralTransaction();
            SendRecordsNonTransactionally(10);
            RunPcAndBlockRecordsOverLimitIndex(3);
            WaitForRecordsToBeReceived();
            pc.Close();
        }

        [Fact]
        public void DontBlockAnyRecords()
        {
            SendSeveralTransaction();
            SendRecordsNonTransactionally(10);
            RunPcAndBlockRecordsOverLimitIndex(int.MaxValue);
            WaitForRecordsToBeReceived();
            pc.Close();
        }

        [Fact]
        public void OverLappingTransactions()
        {
            int numberOfBaseRecords = 3;
            StartAndOneRecord(txProducer);
            StartAndOneRecord(txProducerTwo);
            StartAndOneRecord(txProducerThree);
            CommitTx(txProducer);
            CommitTx(txProducerTwo);
            CommitTx(txProducerThree);
            SendRecordsNonTransactionally(2);
            RunPcAndBlockRecordsOverLimitIndex(numberOfBaseRecords);
            WaitForRecordsToBeReceived(numberOfBaseRecords);
            pc.Close();
        }

        private void CommitTx(IProducer<string, string> txProducer)
        {
            txProducer.CommitTransaction(TimeSpan.FromSeconds(10));
        }

        private void StartAndOneRecord(IProducer<string, string> txProducer)
        {
            txProducer.InitTransactions(TimeSpan.FromSeconds(10));
            txProducer.BeginTransaction();
            txProducer.Produce(new TopicPartition(Topic, Partition.Any), new Message<string, string> { Key = "", Value = "" });
        }
    }
}