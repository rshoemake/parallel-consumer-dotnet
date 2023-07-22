using System;

namespace Confluent.ParallelConsumer.Offsets
{
    /// <summary>
    /// Throw when for whatever reason, no encoding of the offsets is possible.
    /// </summary>
    public class NoEncodingPossibleException : InternalException
    {
    }
}