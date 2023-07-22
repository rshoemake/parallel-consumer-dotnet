using System;
using System.Collections.Generic;
using System.Linq;

namespace io.confluent.csid.utils
{
    public class Range : IEnumerable<long>
    {
        private readonly long start;
        private readonly long limit;

        public Range(int start, long max)
        {
            this.start = start;
            this.limit = max;
        }

        public Range(long limit)
        {
            this.start = 0L;
            this.limit = limit;
        }

        public static Range range(long max)
        {
            return new Range(max);
        }

        public static Range range(int start, long max)
        {
            return new Range(start, max);
        }

        public static List<int> listOfIntegers(int max)
        {
            return Range.range(max).listAsIntegers();
        }

        public IEnumerator<long> GetEnumerator()
        {
            long max = limit;
            long current = start;

            while (current < max)
            {
                yield return current++;
            }
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public List<int> listAsIntegers()
        {
            return Enumerable.Range((int)start, (int)(limit - start)).ToList();
        }

        public IEnumerable<long> toStream()
        {
            return Enumerable.Range((int)start, (int)(limit - start)).Select(i => (long)i);
        }
    }
}