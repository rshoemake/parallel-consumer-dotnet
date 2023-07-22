using System;

namespace io.confluent.parallelconsumer
{
    /**
     * This exception is only used when there is an exception thrown from code provided by the user.
     */
    [StandardException]
    public class ExceptionInUserFunctionException : ParallelConsumerException
    {
    }
}