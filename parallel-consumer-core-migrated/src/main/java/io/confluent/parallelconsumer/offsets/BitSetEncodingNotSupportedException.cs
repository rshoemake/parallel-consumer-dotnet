using System;

namespace io.confluent.parallelconsumer.offsets
{
    /**
     * Thrown under situations where the {@link BitSetEncoder} would not be able to encode the given data.
     *
     * @author Antony Stubbs
     */
    [StandardException]
    public class BitSetEncodingNotSupportedException : EncodingNotSupportedException
    {
    }
}