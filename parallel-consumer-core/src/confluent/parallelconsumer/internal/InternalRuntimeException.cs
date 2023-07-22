using System;

namespace io.confluent.parallelconsumer.@internal
{
    /**
     * Internal {@link RuntimeException}
     *
     * @author Antony Stubbs
     * @see InternalException
     */
    [StandardException]
    public class InternalRuntimeException : RuntimeException
    {
        /**
         * @see StringUtils#msg(String, Object...)
         */
        public static InternalRuntimeException msg(string message, params object[] vars)
        {
            return new InternalRuntimeException(StringUtils.msg(message, vars));
        }

        /**
         * @see StringUtils#msg(String, Object...)
         */
        public InternalRuntimeException(string message, Throwable e, params object[] args)
        {
            this(StringUtils.msg(message, args), e);
        }
    }
}