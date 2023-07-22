using System.Collections.Concurrent;
using Xunit;

namespace io.confluent.parallelconsumer.sanity
{
    public class QueueingSanityTests
    {
        [Fact]
        public void Test()
        {
            ConcurrentLinkedDeque<int> q = new ConcurrentLinkedDeque<int>();

            Assert.True(q.Add(1));
            Assert.True(q.Add(2));

            Assert.Equal(1, q.Poll());
            Assert.Equal(2, q.Poll());
        }
    }
}