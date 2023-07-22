using System;

namespace Confluent.ParallelConsumer.Offsets
{
    /// <summary>
    /// Parent of the exceptions for when the <see cref="OffsetEncoder"/> cannot encode the given data.
    /// </summary>
    public class EncodingNotSupportedException : InternalException
    {
    }
}