using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Linq;
using Xunit;

namespace io.confluent.csid.utils
{
    public class LoopingResumingIteratorTest
    {
        [Fact]
        public void NoFrom()
        {
            var map = new Dictionary<int, string>
            {
                { 0, "a" },
                { 1, "b" },
                { 2, "c" },
                { 3, "d" }
            };
            var iterator = new LoopingResumingIterator<int, string>(map);

            var results = new List<KeyValuePair<int, string>>();
            for (var x = iterator.Next(); x.HasValue; x = iterator.Next())
            {
                results.Add(x.Value);
            }
            Assert.Equal(new List<int> { 0, 1, 2, 3 }, results.Select(x => x.Key));
        }

        [Fact]
        public void FromInMiddle()
        {
            var map = new Dictionary<int, string>
            {
                { 0, "a" },
                { 1, "b" },
                { 2, "c" },
                { 3, "d" }
            };
            var iterator = LoopingResumingIterator<int, string>.Build(2, map);
            var results = new List<KeyValuePair<int, string>>();
            for (var x = iterator.Next(); x.HasValue; x = iterator.Next())
            {
                results.Add(x.Value);
            }
            Assert.Equal(new List<int> { 2, 3, 0, 1 }, results.Select(x => x.Key));
        }

        [Fact]
        public void FromIsEnd()
        {
            var map = new Dictionary<int, string>
            {
                { 0, "a" },
                { 1, "b" },
                { 2, "c" },
                { 3, "d" }
            };
            var iterator = LoopingResumingIterator<int, string>.Build(3, map);
            var results = new List<KeyValuePair<int, string>>();
            for (var x = iterator.Next(); x.HasValue; x = iterator.Next())
            {
                results.Add(x.Value);
            }
            Assert.Equal(new List<int> { 3, 0, 1, 2 }, results.Select(x => x.Key));
        }

        [Fact]
        public void FromBeginningFirstElement()
        {
            var map = new Dictionary<int, string>
            {
                { 0, "a" },
                { 1, "b" },
                { 2, "c" },
                { 3, "d" }
            };
            var iterator = LoopingResumingIterator<int, string>.Build(0, map);
            var results = new List<KeyValuePair<int, string>>();
            for (var x = iterator.Next(); x.HasValue; x = iterator.Next())
            {
                results.Add(x.Value);
            }
            Assert.Equal(new List<int> { 0, 1, 2, 3 }, results.Select(x => x.Key));
        }

        [Fact]
        public void FromDoesntExist()
        {
            var map = new Dictionary<int, string>
            {
                { 0, "a" },
                { 1, "b" },
                { 2, "c" },
                { 3, "d" }
            };
            var iterator = LoopingResumingIterator<int, string>.Build(88, map);
            var results = new List<KeyValuePair<int, string>>();
            for (var x = iterator.Next(); x.HasValue; x = iterator.Next())
            {
                results.Add(x.Value);
            }
            Assert.Equal(new List<int> { 0, 1, 2, 3 }, results.Select(x => x.Key));
        }

        [Fact]
        public void LoopsCorrectly()
        {
            var map = new Dictionary<int, string>
            {
                { 0, "a" },
                { 1, "b" },
                { 2, "c" }
            };
            var entries = LoopingResumingIterator<int, string>.Build(null, map);
            var results = new List<KeyValuePair<int, string>>();
            var iterator = entries;
            for (var x = iterator.Next(); x.HasValue; x = iterator.Next())
            {
                results.Add(x.Value);
            }
            Assert.Equal(new List<int> { 0, 1, 2 }, results.Select(x => x.Key));

            entries = LoopingResumingIterator<int, string>.Build(2, map);
            results = new List<KeyValuePair<int, string>>();
            iterator = entries;
            for (var x = iterator.Next(); x.HasValue; x = iterator.Next())
            {
                results.Add(x.Value);
            }
            Assert.Equal(new List<int> { 2, 0, 1 }, results.Select(x => x.Key));

            Assert.False(entries.Next().HasValue);
        }

        [Fact]
        public void LoopsCorrectlyWithStartingObjectIndexZero()
        {
            var map = new Dictionary<int, string>
            {
                { 0, "a" },
                { 1, "b" },
                { 2, "c" }
            };
            var entries = LoopingResumingIterator<int, string>.Build(0, map);
            var results = new List<KeyValuePair<int, string>>();
            var iterator = entries;
            for (var x = iterator.Next(); x.HasValue; x = iterator.Next())
            {
                results.Add(x.Value);
            }
            Assert.Equal(new List<int> { 0, 1, 2 }, results.Select(x => x.Key));

            Assert.False(entries.Next().HasValue);
        }

        [Fact]
        public void EmptyInitialStartingKey()
        {
            var map = new Dictionary<int, string>
            {
                { 0, "a" },
                { 1, "b" }
            };

            var iterator = LoopingResumingIterator<int, string>.Build(null, map);

            var next = iterator.Next().Value;
            Assert.Equal(0, next.Key);
            Assert.Equal("a", next.Value);
        }

        [Fact]
        public void DemoOfNoSuchElementIssue()
        {
            const int INDEX_TO_REMOVE = 4;
            var map = new ConcurrentDictionary<int, string>();
            map.TryAdd(0, "a");
            map.TryAdd(1, "b");
            map.TryAdd(2, "c");
            map.TryAdd(3, "d");
            map.TryAdd(INDEX_TO_REMOVE, "e");
            map.TryAdd(5, "f");
            map.TryAdd(6, "g");
            map.TryAdd(7, "h");
            map.TryAdd(8, "i");
            map.TryAdd(9, "j");

            var results = new List<Optional<KeyValuePair<int, string>>>();

            var iterator = LoopingResumingIterator<int, string>.Build(INDEX_TO_REMOVE, map);

            for (var x = iterator.Next(); x.HasValue; x = iterator.Next())
            {
                results.Add(x);
                if (x.Value.Key == INDEX_TO_REMOVE)
                {
                    map.TryRemove(INDEX_TO_REMOVE, out _);
                }
            }

            var expected = new Dictionary<int, string>
            {
                { 4, "e" },
                { 5, "f" },
                { 6, "g" },
                { 7, "h" },
                { 8, "i" },
                { 9, "j" },
                { 0, "a" },
                { 1, "b" },
                { 2, "c" },
                { 3, "d" }
            };
            var expectedAsList = expected.Select(x => Optional.Of(x)).ToList();

            Assert.Equal(expectedAsList, results);

            Assert.False(iterator.Next().HasValue);

            map.Clear();

            Assert.False(iterator.Next().HasValue);
        }
    }
}