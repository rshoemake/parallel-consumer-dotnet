using Confluent.ParallelConsumer;
using Confluent.ParallelConsumer.State;
using Moq;
using Xunit;

namespace Confluent.ParallelConsumer.Tests.State
{
    public class ShardKeyTest
    {
        [Fact]
        public void NullKey()
        {
            var cr = new Mock<ConsumerRecord<string, string>>();
            cr.Setup(x => x.Partition).Returns(0);
            cr.Setup(x => x.Topic).Returns("atopic");
            cr.Setup(x => x.Key).Returns((string)null);

            var wc = new Mock<WorkContainer<string, string>>();
            wc.Setup(x => x.Cr).Returns(cr.Object);

            ShardKey.Of(wc.Object, ParallelConsumerOptions.ProcessingOrder.Key);
        }

        [Fact]
        public void KeyTest()
        {
            var ordering = ParallelConsumerOptions.ProcessingOrder.Key;
            var topicOne = "t1";
            var topicOneP0 = new TopicPartition("t1", 0);
            var keyOne = "k1";

            var reck1 = new ConsumerRecord<string, string>(topicOne, 0, 0, keyOne, "v");
            var key1 = ShardKey.Of(reck1, ordering);
            var anotherInstanceWithSameInputs = ShardKey.Of(reck1, ordering);
            Assert.Equal(key1, anotherInstanceWithSameInputs);

            var reck2 = new ConsumerRecord<string, string>(topicOne, 0, 0, "k2", "v");
            var of3 = ShardKey.Of(reck2, ordering);
            Assert.NotEqual(key1, of3);

            var reck3 = new ConsumerRecord<string, string>("t2", 0, 0, keyOne, "v");
            Assert.NotEqual(key1, ShardKey.Of(reck3, ordering));

            var keyOrderedKey = new ShardKey.KeyOrderedKey(topicOneP0, keyOne);
            var keyOrderedKeyTwo = new ShardKey.KeyOrderedKey(topicOneP0, keyOne);
            Assert.Equal(keyOrderedKey, keyOrderedKeyTwo);

            var reck4 = new ConsumerRecord<string, string>(topicOne, 1, 0, keyOne, "v");
            var of4 = ShardKey.Of(reck2, ordering);
            Assert.NotEqual(key1, of3);
        }
    }
}