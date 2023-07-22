using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace OffsetRunLength
{
    public static class OffsetRunLength
    {
        public static List<int> RunLengthEncode(string input)
        {
            var length = new AtomicInteger();
            var previous = new AtomicBoolean(false);
            var encoding = new List<int>();
            input.ToCharArray().ToList().ForEach(bit =>
            {
                var current = bit switch
                {
                    'o' => false,
                    'x' => true,
                    _ => throw new ArgumentException(bit + " in " + input)
                };
                if (previous.Get() == current)
                {
                    length.GetAndIncrement();
                }
                else
                {
                    previous.Set(current);
                    encoding.Add(length.Get());
                    length.Set(1);
                }
            });
            encoding.Add(length.Get());
            return encoding;
        }

        public static string RunLengthDecodeToString(List<int> input)
        {
            var sb = new StringBuilder(input.Count);
            var current = false;
            foreach (var i in input)
            {
                for (var x = 0; x < i; x++)
                {
                    if (current)
                    {
                        sb.Append('x');
                    }
                    else
                    {
                        sb.Append('o');
                    }
                }
                current = !current;
            }
            return sb.ToString();
        }

        public static HighestOffsetAndIncompletes RunLengthDecodeToIncompletes(OffsetEncoding encoding, long baseOffset, ByteBuffer input)
        {
            input.Rewind();
            var v1ShortBuffer = input.AsShortBuffer();
            var v2IntegerBuffer = input.AsIntBuffer();

            var incompletes = new SortedSet<long>();

            long highestSeenOffset = (baseOffset > 0) ? (baseOffset - 1) : 0L;

            Func<bool> hasRemainingTest = () =>
            {
                return encoding.Version switch
                {
                    OffsetEncoding.VersionEnum.V1 => v1ShortBuffer.HasRemaining(),
                    OffsetEncoding.VersionEnum.V2 => v2IntegerBuffer.HasRemaining(),
                    _ => throw new ArgumentException("Invalid encoding version")
                };
            };

            if (log.IsTraceEnabled())
            {
                var runlengths = new List<Number>();
                try
                {
                    while (hasRemainingTest())
                    {
                        var runLength = encoding.Version switch
                        {
                            OffsetEncoding.VersionEnum.V1 => v1ShortBuffer.Get(),
                            OffsetEncoding.VersionEnum.V2 => v2IntegerBuffer.Get(),
                            _ => throw new ArgumentException("Invalid encoding version")
                        };
                        runlengths.Add(runLength);
                    }
                }
                catch (BufferUnderflowException u)
                {
                    log.Error("Error decoding offsets", u);
                }
                log.Debug("Unrolled runlengths: {}", runlengths);
                v1ShortBuffer.Rewind();
                v2IntegerBuffer.Rewind();
            }

            var currentRunLengthIsComplete = false;
            var currentOffset = baseOffset;
            while (hasRemainingTest())
            {
                try
                {
                    var runLength = encoding.Version switch
                    {
                        OffsetEncoding.VersionEnum.V1 => v1ShortBuffer.Get(),
                        OffsetEncoding.VersionEnum.V2 => v2IntegerBuffer.Get(),
                        _ => throw new ArgumentException("Invalid encoding version")
                    };

                    if (currentRunLengthIsComplete)
                    {
                        log.Trace("Ignoring {} completed offset(s) (offset:{})", runLength, currentOffset);
                        currentOffset += runLength.LongValue();
                        highestSeenOffset = currentOffset - 1;
                    }
                    else
                    {
                        log.Trace("Adding {} incomplete offset(s) (starting with offset:{})", runLength, currentOffset);
                        for (var relativeOffset = 0; relativeOffset < runLength.LongValue(); relativeOffset++)
                        {
                            incompletes.Add(currentOffset);
                            highestSeenOffset = currentOffset;
                            currentOffset++;
                        }
                    }
                    log.Trace("Highest seen: {}", highestSeenOffset);
                }
                catch (BufferUnderflowException u)
                {
                    log.Error("Error decoding offsets", u);
                    throw u;
                }
                currentRunLengthIsComplete = !currentRunLengthIsComplete;
            }
            return HighestOffsetAndIncompletes.Of(highestSeenOffset, incompletes);
        }

        public static List<int> RunLengthDeserialise(ByteBuffer input)
        {
            input.Rewind();
            var shortBuffer = input.AsShortBuffer();

            var results = new List<int>(shortBuffer.Capacity());
            while (shortBuffer.HasRemaining())
            {
                results.Add((int)shortBuffer.Get());
            }
            return results;
        }
    }
}