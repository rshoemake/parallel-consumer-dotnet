using Confluent.Kafka;
using Confluent.ParallelConsumer;
using NUnit.Framework;
using System.Collections.Generic;

namespace CustomConsumersTest
{
    [TestFixture]
    public class CustomConsumersTest : BrokerIntegrationTest
    {
        [Test]
        public void ExtendedConsumer()
        {
            var properties = GetKcu().SetupConsumerProps(GetType().Name);
            var client = new CustomConsumer<string, string>(properties);

            var options = new ParallelConsumerOptions<string, string>
            {
                Consumer = client
            };

            var pc = new ParallelEoSStreamProcessor<string, string>(options);
        }

        public class CustomConsumer<K, V> : Consumer<K, V>
        {
            public string CustomField { get; set; } = "custom";

            public CustomConsumer(Dictionary<string, object> config) : base(config)
            {
            }
        }
    }
}