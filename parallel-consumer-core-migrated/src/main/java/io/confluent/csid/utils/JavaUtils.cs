using System;
using System.Collections.Generic;
using System.Linq;

namespace Confluent.Csid.Utils
{
    public static class CSharpUtils
    {
        public static Optional<T> GetLast<T>(List<T> someList)
        {
            if (someList.Count == 0) return Optional<T>.Empty;
            return Optional<T>.Of(someList[someList.Count - 1]);
        }

        public static Optional<T> GetFirst<T>(List<T> someList)
        {
            return someList.Count == 0 ? Optional<T>.Empty : Optional<T>.Of(someList[0]);
        }

        public static Optional<T> GetOnlyOne<T>(Dictionary<string, T> stringMapMap)
        {
            if (stringMapMap.Count == 0) return Optional<T>.Empty;
            var values = stringMapMap.Values;
            if (values.Count > 1) throw new InternalRuntimeException("More than one element");
            return Optional<T>.Of(values.First());
        }

        public static Duration Max(Duration left, Duration right)
        {
            long expectedDurationOfClose = Math.Max(left.ToMillis(), right.ToMillis());
            return Duration.OfMillis(expectedDurationOfClose);
        }

        public static bool IsGreaterThan(Duration compare, Duration to)
        {
            return compare.CompareTo(to) > 0;
        }

        public static Dictionary<K, V2> Remap<K, V1, V2>(Dictionary<K, V1> map, Func<V1, V2> function)
        {
            return map
                .Select(entry => new KeyValuePair<K, V2>(entry.Key, function(entry.Value)))
                .ToDictionary(pair => pair.Key, pair => pair.Value);
        }

        public static List<string> GetRandom(List<string> list, int quantity)
        {
            if (list.Count < quantity)
            {
                throw new ArgumentException("List size is less than quantity");
            }

            return CreateRandomIntStream(list.Count)
                .Take(quantity)
                .Select(index => list[index])
                .ToList();
        }

        private static IEnumerable<int> CreateRandomIntStream(int range)
        {
            var random = new Random();
            while (true)
            {
                yield return random.Next(range);
            }
        }

        public static Collector<T, List<T>, SortedSet<T>> ToSortedSet<T>()
        {
            return Collector.Of(
                () => new List<T>(),
                (list, item) => list.Add(item),
                (list1, list2) => { list1.AddRange(list2); return list1; },
                list => new SortedSet<T>(list)
            );
        }
    }
}