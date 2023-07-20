using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace io.confluent.parallelconsumer.sanity
{
    [TestClass]
    public class StreamTest
    {
        //    [TestMethod]
        public void Test()
        {
            var s = Enumerable.Range(0, int.MaxValue).Select(_ => new Random().NextDouble());
            foreach (var x in s)
            {
                Console.WriteLine(x.ToString());
            }
        }

        [TestMethod]
        public void TestStreamSpliterators()
        {
            int max = 10;

            var i = new Iterator<string>()
            {
                Count = 0,
                HasNext = () => Count < max,
                Next = () =>
                {
                    Count++;
                    return Count + " " + new Random().NextDouble();
                }
            };

            var spliterator = Spliterators.Spliterator(i, 0, Spliterator.NONNULL);

            var stream = StreamSupport.Stream(spliterator, false);

            var collect = stream
                .Select(x =>
                {
                    Console.WriteLine(x);
                    return x.ToUpper();
                })
                .ToList();

            Assert.AreEqual(max, collect.Count);
        }

        private class Iterator<T> : IEnumerator<T>
        {
            public int Count { get; set; }
            public Func<bool> HasNext { get; set; }
            public Func<T> Next { get; set; }

            public T Current => Next();

            object IEnumerator.Current => Current;

            public void Dispose()
            {
            }

            public bool MoveNext()
            {
                return HasNext();
            }

            public void Reset()
            {
                throw new NotImplementedException();
            }
        }
    }
}