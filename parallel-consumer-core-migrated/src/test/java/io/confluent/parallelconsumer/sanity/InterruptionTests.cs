using System;
using System.Threading;
using Xunit;

namespace io.confluent.parallelconsumer.sanity
{
    public class InterruptionTests
    {
        /**
         * Verify behaviour of 0 vs 1 timeout on {@link Object@wait}. Original test timeout of 5ms was too small, sometimes
         * (1/4000) runs it would timeout. 1/117,000 it failed at 50ms. 1 second didn't observe failure within ~250,000 runs
         * in Intellij (run until fail).
         */
        [Timeout(1)]
        [Fact]
        public void WaitOnZeroCausesInfiniteWait()
        {
            object lockObj = new object();
            try
            {
                lock (lockObj)
                {
                    Monitor.Wait(lockObj, 1);
                    // Monitor.Wait(lockObj, 0); // zero causes it to wait forever
                }
            }
            catch (ThreadInterruptedException e)
            {
                Console.WriteLine(e.StackTrace);
            }
        }
    }
}