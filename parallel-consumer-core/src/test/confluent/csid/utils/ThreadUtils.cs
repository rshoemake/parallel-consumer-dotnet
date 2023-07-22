using System;
using System.Threading;

namespace io.confluent.csid.utils
{
    public static class ThreadUtils
    {
        public static void SleepQuietly(int ms)
        {
            Console.WriteLine($"Sleeping for {ms}");
            Thread.Sleep(ms);
            Console.WriteLine($"Woke up (slept for {ms})");
        }

        public static void SleepLog(int ms)
        {
            try
            {
                Thread.Sleep(ms);
            }
            catch (ThreadInterruptedException e)
            {
                Console.WriteLine($"Sleep of {ms} interrupted");
            }
        }

        public static void SleepQuietly(long ms)
        {
            SleepQuietly((int)ms);
        }

        public static void SleepSecondsLog(int seconds)
        {
            SleepLog(seconds * 1000);
        }
    }
}