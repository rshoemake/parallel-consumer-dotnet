using System;

namespace Confluent.ParallelConsumer.Offsets
{
    /*
     * Error decoding offsets
     *
     * TODO should extend java.lang.Error ?
     *
     * author Antony Stubbs
     */
    public class OffsetDecodingError : InternalException
    {
    }
}