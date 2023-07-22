using System;
using System.Collections.Generic;
using System.Linq;

namespace io.confluent.parallelconsumer.integrationTests.utils
{
    public class GenUtils
    {
        public static readonly Instant randomSeedInstant = new Calendar.Builder().SetTimeZone(TimeZone.GetTimeZone("UTC")).SetDate(1982, 6, 10).Build().ToInstant();
        public static readonly long randomSeed = randomSeedInstant.ToEpochMilli();

        public List<T> CreateSomeStuff<T>(int quantity, Func<int, T> constructor)
        {
            return Enumerable.Range(0, quantity)
                .Select(constructor)
                .ToList();
        }
    }
}