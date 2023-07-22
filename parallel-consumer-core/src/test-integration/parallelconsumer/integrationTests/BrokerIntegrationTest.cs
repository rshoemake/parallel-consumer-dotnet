using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Confluent.Kafka;
using NUnit.Framework;

namespace ParallelConsumer.IntegrationTests
{
    [TestFixture]
    public abstract class BrokerIntegrationTest<K, V>
    {
        static BrokerIntegrationTest()
        {
            Environment.SetEnvironmentVariable("flogger.backend_factory", "com.google.common.flogger.backend.slf4j.Slf4jBackendFactory#getInstance");
        }

        protected int numPartitions = 1;
        protected int partitionNumber = 0;

        protected string topic;

        protected static KafkaContainer kafkaContainer = CreateKafkaContainer(null);

        protected static KafkaContainer CreateKafkaContainer(string logSegmentSize)
        {
            KafkaContainer baseContainer = new KafkaContainer("confluentinc/cp-kafka:7.3.0")
                .WithEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
                .WithEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
                .WithEnv("KAFKA_TRANSACTION_STATE_LOG_NUM_PARTITIONS", "1")
                .WithEnv("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "500")
                .WithReuse(true);

            if (!string.IsNullOrWhiteSpace(logSegmentSize))
            {
                baseContainer = baseContainer.WithEnv("KAFKA_LOG_SEGMENT_BYTES", logSegmentSize);
            }

            return baseContainer;
        }

        protected static void FollowKafkaLogs()
        {
            if (log.isDebugEnabled())
            {
                FilteredTestContainerSlf4jLogConsumer logConsumer = new FilteredTestContainerSlf4jLogConsumer(log);
                kafkaContainer.FollowOutput(logConsumer);
            }
        }

        [OneTimeSetUp]
        protected static void BeforeAll()
        {
            kafkaContainer.Start();
            FollowKafkaLogs();
        }

        protected KafkaClientUtils kcu = new KafkaClientUtils(kafkaContainer);

        [SetUp]
        protected void Open()
        {
            kcu.Open();
        }

        [TearDown]
        protected void Close()
        {
            kcu.Close();
        }

        protected void SetupTopic()
        {
            string name = typeof(LoadTest).Name;
            SetupTopic(name);
        }

        protected string SetupTopic(string name)
        {
            Assert.IsTrue(kafkaContainer.IsRunning); // sanity

            topic = name + "-" + new Random().Next();

            EnsureTopic(topic, numPartitions);

            return topic;
        }

        protected CreateTopicsResult EnsureTopic(string topic, int numPartitions)
        {
            NewTopic e1 = new NewTopic(topic, numPartitions, (short)1);
            CreateTopicsResult topics = kcu.Admin.CreateTopicsAsync(new List<NewTopic> { e1 }).Result;
            try
            {
                topics.All().GetAwaiter().GetResult();
            }
            catch (ExecutionException)
            {
                // fine
            }
            catch (Exception e)
            {
                throw new Exception(e.Message);
            }
            return topics;
        }

        protected List<string> ProduceMessages(int quantity)
        {
            return ProduceMessages(quantity, "");
        }

        protected List<string> ProduceMessages(int quantity, string prefix)
        {
            return kcu.ProduceMessagesAsync(topic, quantity, prefix).Result;
        }
    }
}