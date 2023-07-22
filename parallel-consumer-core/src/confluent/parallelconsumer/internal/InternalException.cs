using System;

namespace io.confluent.parallelconsumer.@internal
{
    /**
     * Generic Parallel Consumer parent exception.
     *
     * @author Antony Stubbs
     * @see InternalRuntimeException RuntimeException version
     */
    [StandardException]
    public class InternalException : Exception
    {
    }
}