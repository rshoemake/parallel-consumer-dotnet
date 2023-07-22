using System;
using Xunit;

namespace Confluent.Csid.Utils
{
    public class SuppliersTest
    {
        [Fact]
        public void TestMemoize()
        {
            UnderlyingSupplier underlyingSupplier = new UnderlyingSupplier();
            Func<int> memoizedSupplier = Suppliers.Memoize(underlyingSupplier);
            Assert.Equal(0, underlyingSupplier.Calls); // the underlying supplier hasn't executed yet
            Assert.Equal(5, memoizedSupplier());

            Assert.Equal(1, underlyingSupplier.Calls);
            Assert.Equal(5, memoizedSupplier());

            Assert.Equal(1, underlyingSupplier.Calls); // it still should only have executed once due to memoization
        }

        [Fact]
        public void TestMemoizeNullSupplier()
        {
            Assert.Throws<ArgumentNullException>(() => Suppliers.Memoize<int>(null));
        }

        [Fact]
        public void TestMemoizeSupplierReturnsNull()
        {
            Assert.Throws<ArgumentNullException>(() =>
            {
                Func<object> supplier = Suppliers.Memoize(() => null);
                supplier();
            });
        }

        [Fact]
        public void TestMemoizeExceptionThrown()
        {
            Func<int> memoizedSupplier = Suppliers.Memoize(new ThrowingSupplier());
            Assert.Throws<InvalidOperationException>(() => memoizedSupplier());
        }

        private class UnderlyingSupplier : Func<int>
        {
            public int Calls { get; private set; }

            public override int Invoke()
            {
                Calls++;
                return Calls * 5;
            }
        }

        private class ThrowingSupplier : Func<int>
        {
            public override int Invoke()
            {
                throw new InvalidOperationException();
            }
        }
    }
}