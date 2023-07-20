using System;
using System.Threading;

namespace Confluent.Csid.Utils
{
    public static class Suppliers
    {
        public static Func<T> Memoize<T>(Func<T> delegateFunc)
        {
            if (delegateFunc == null)
                throw new ArgumentNullException(nameof(delegateFunc));

            var value = new AtomicReference<T>();
            return () =>
            {
                var val = value.Get();
                if (val == null)
                {
                    lock (value)
                    {
                        val = value.Get();
                        if (val == null)
                        {
                            val = delegateFunc.Invoke();
                            value.Set(val);
                        }
                    }
                }
                return val;
            };
        }
    }

    internal class AtomicReference<T>
    {
        private T _value;

        public T Get()
        {
            return _value;
        }

        public void Set(T value)
        {
            _value = value;
        }
    }
}