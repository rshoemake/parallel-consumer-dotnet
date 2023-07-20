using System;
using System.Threading;
using System.Threading.Tasks;

namespace io.confluent.csid.utils
{
    public class BlockedThreadAsserter
    {
        private readonly AtomicBoolean methodReturned = new AtomicBoolean(false);

        public bool FunctionHasCompleted()
        {
            return methodReturned.Get();
        }

        public void AssertFunctionBlocks(Action functionExpectedToBlock)
        {
            AssertFunctionBlocks(functionExpectedToBlock, TimeSpan.FromSeconds(1));
        }

        public void AssertFunctionBlocks(Action functionExpectedToBlock, TimeSpan blockedForAtLeast)
        {
            Thread blocked = new Thread(() =>
            {
                try
                {
                    Console.WriteLine($"Running function expected to block for at least {blockedForAtLeast}...");
                    functionExpectedToBlock();
                    Console.WriteLine("Blocked function finished.");
                }
                catch (Exception e)
                {
                    Console.WriteLine("Error in blocking function");
                    Console.WriteLine(e);
                }
                methodReturned.Set(true);
            });
            blocked.Start();

            await()
                .PollDelay(blockedForAtLeast) // makes sure it is still blocked after 1 second
                .AtMost(blockedForAtLeast.Add(TimeSpan.FromSeconds(1)))
                .Until(() => Truth.AssertWithMessage("Thread should be sleeping/blocked and not have returned")
                    .That(methodReturned.Get())
                    .IsFalse());
        }

        private readonly ScheduledExecutorService scheduledExecutorService = Executors.NewSingleThreadScheduledExecutor();

        public void AssertUnblocksAfter(Action functionExpectedToBlock, Action unblockingFunction, TimeSpan unblocksAfter)
        {
            AtomicBoolean unblockerHasRun = new AtomicBoolean(false);
            scheduledExecutorService.Schedule(() =>
            {
                Console.WriteLine("Running unblocking function - blocked function should return ONLY after this (which will be tested)");
                try
                {
                    unblockingFunction();
                }
                catch (Exception e)
                {
                    Console.WriteLine("Error in unlocking function");
                    Console.WriteLine(e);
                }
                unblockerHasRun.Set(true);
                Console.WriteLine("Blocked function returned");
            },
            (long)unblocksAfter.TotalMilliseconds,
            TimeUnit.MILLISECONDS);

            var time = TimeUtils.TimeWithMeta(() =>
            {
                Console.WriteLine($"Running function expected to block for at least {unblocksAfter}...");
                try
                {
                    functionExpectedToBlock();
                }
                catch (Exception e)
                {
                    Console.WriteLine("Error in blocking function");
                    Console.WriteLine(e);
                }
                Console.WriteLine("Unblocking function finished returned");
                return typeof(void);
            });
            Console.WriteLine($"Function unblocked after {time.GetElapsed()}");

            this.methodReturned.Set(true);

            Truth.AssertThat(time.GetElapsed()).IsAtLeast(unblocksAfter);
            Truth.AssertWithMessage("Unblocking function should complete OK (if false, may not have run at all - or that the expected function to block did NOT block)")
                .That(unblockerHasRun.Get()).IsTrue();
        }

        public void AssertUnblocksAfter(Action functionExpectedToBlock, Action unblockingFunction)
        {
            AssertUnblocksAfter(functionExpectedToBlock, unblockingFunction, TimeSpan.FromSeconds(1));
        }

        public void AwaitReturnFully()
        {
            Console.WriteLine("Waiting for blocked method to fully finish...");
            await().UntilTrue(this.methodReturned);
            Console.WriteLine("Waiting on blocked method to fully finish is complete.");
        }
    }
}