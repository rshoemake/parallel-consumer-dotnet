using System;
using System.Diagnostics;
using System.Threading.Tasks;

namespace Confluent.Csid.Utils
{
    public static class TimeUtils
    {
        public static DateTime GetClock()
        {
            return DateTime.UtcNow;
        }

        public static async Task<RESULT> Time<RESULT>(Func<Task<RESULT>> func)
        {
            return await TimeWithMeta(func).ConfigureAwait(false);
        }

        public static async Task<TimeResult<RESULT>> TimeWithMeta<RESULT>(Func<Task<RESULT>> func)
        {
            var stopwatch = Stopwatch.StartNew();
            var timer = new TimeResult<RESULT> { StartMs = stopwatch.ElapsedMilliseconds };
            RESULT result = await func().ConfigureAwait(false);
            timer.Result = result;
            stopwatch.Stop();
            timer.EndMs = stopwatch.ElapsedMilliseconds;
            Console.WriteLine($"Function took {stopwatch.Elapsed}");
            return timer;
        }

        public class TimeResult<RESULT>
        {
            public long StartMs { get; set; }
            public long EndMs { get; set; }
            public RESULT Result { get; set; }

            public TimeSpan Elapsed => TimeSpan.FromMilliseconds(EndMs - StartMs);
        }
    }
}