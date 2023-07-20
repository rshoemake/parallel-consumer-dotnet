using System;

namespace io.confluent.parallelconsumer
{
    /**
     * Used for testing error handling - easier to identify than a plain exception.
     *
     * @author Antony Stubbs
     */
    [StandardException]
    public class FakeRuntimeException : PCRetriableException
    {
    }
}