using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Confluent.Csid.Utils
{
    public static class LatchTestUtils
    {
        private static readonly TimeSpan DefaultTimeout = TimeSpan.FromSeconds(10);

        public static void AwaitLatch(List<CountdownEvent> latches, int latchIndex)
        {
            Console.WriteLine($"Waiting on latch {latchIndex}");
            AwaitLatch(latches[latchIndex]);
            Console.WriteLine($"Wait on latch {latchIndex} finished.");
        }

        public static void AwaitLatch(CountdownEvent latch)
        {
            AwaitLatch(latch, (int)DefaultTimeout.TotalSeconds);
        }

        public static void AwaitLatch(CountdownEvent latch, int seconds)
        {
            Console.WriteLine($"Waiting on latch with timeout {seconds}s");
            DateTime start = DateTime.Now;
            bool latchReachedZero = false;
            while (DateTime.Now < start.AddSeconds(seconds))
            {
                try
                {
                    latchReachedZero = latch.Wait(seconds * 1000);
                }
                catch (ThreadInterruptedException e)
                {
                    Console.WriteLine("Latch await interrupted");
                }
                if (latchReachedZero)
                    break;
                else
                    Console.WriteLine($"Latch wait aborted, but timeout ({seconds}s) not reached - will try to wait again remaining {seconds - (int)DateTime.Now.Subtract(start).TotalSeconds}");
            }
            if (latchReachedZero)
            {
                Console.WriteLine("Latch was released (#countdown)");
            }
            else
            {
                throw new TimeoutException($"Latch await timeout ({seconds} seconds) - {latch.CurrentCount} count remaining");
            }
        }

        public static void Release(List<CountdownEvent> locks, int lockIndex)
        {
            Console.WriteLine($"Releasing {lockIndex}...");
            locks[lockIndex].Signal();
        }

        public static List<CountdownEvent> ConstructLatches(int numberOfLatches)
        {
            var result = new List<CountdownEvent>(numberOfLatches);
            for (int i = 0; i < numberOfLatches; i++)
            {
                result.Add(new CountdownEvent(1));
            }
            return result;
        }

        public static void Release(CountdownEvent latch)
        {
            Console.WriteLine("Latch countdown");
            latch.Signal();
        }
    }
}