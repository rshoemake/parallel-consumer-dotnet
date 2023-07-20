using System;

namespace Confluent.Csid.Utils
{
    public static class MathUtils
    {
        public static short ToShortExact(long value)
        {
            short shortCast = (short)value;
            if (shortCast != value)
            {
                throw new ArithmeticException("short overflow");
            }
            return shortCast;
        }
    }
}