using System;
using System.Collections.Generic;
using Xunit;

namespace io.confluent.parallelconsumer.state
{
    public class ShardManagerTest
    {
        ModelUtils mu = new ModelUtils();

        [Fact]
        void RetryQueueOrdering()
        {
            PCModuleTestEnv module = mu.GetModule();
            ShardManager<string, string> sm = new ShardManager<string, string>(module, module.WorkManager());
            SortedSet<WorkContainer<string, string>> retryQueue = sm.GetRetryQueue();

            WorkContainer<string, string> w0 = mu.CreateWorkFor(0);
            WorkContainer<string, string> w1 = mu.CreateWorkFor(1);
            WorkContainer<string, string> w2 = mu.CreateWorkFor(2);
            WorkContainer<string, string> w3 = mu.CreateWorkFor(3);

            const int ZERO = 0;
            Assert.Equal(ZERO, sm.GetRetryQueueWorkContainerComparator().Compare(w0, w0));

            retryQueue.Add(w0);
            retryQueue.Add(w1);
            retryQueue.Add(w2);
            retryQueue.Add(w3);

            Assert.Equal(4, retryQueue.Count);

            Assert.NotEqual(w0, w1);
            Assert.NotEqual(w1, w2);

            bool removed = retryQueue.Remove(w1);
            Assert.True(removed);
            Assert.Equal(3, retryQueue.Count);

            Assert.DoesNotContain(w1, retryQueue);

            Assert.Contains(w0, retryQueue);
            Assert.Contains(w2, retryQueue);
        }
    }
}