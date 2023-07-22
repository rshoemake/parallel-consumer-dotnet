using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Confluent.ParallelConsumer.Offsets
{
    public class RunLengthEncoder : OffsetEncoder
    {
        private int currentRunLengthSize = 0;
        private bool previousRunLengthState = false;
        public List<int> RunLengthEncodingIntegers { get; }
        private Optional<byte[]> encodedBytes = Optional.empty();

        private static readonly Version DEFAULT_VERSION = Version.v2;

        public RunLengthEncoder(OffsetSimultaneousEncoder offsetSimultaneousEncoder, Version newVersion) : base(offsetSimultaneousEncoder, newVersion)
        {
            RunLengthEncodingIntegers = new List<int>();
        }

        protected override OffsetEncoding GetEncodingType()
        {
            return Version switch
            {
                Version.v1 => OffsetEncoding.RunLength,
                Version.v2 => OffsetEncoding.RunLengthV2,
                _ => throw new NotSupportedException()
            };
        }

        protected override OffsetEncoding GetEncodingTypeCompressed()
        {
            return Version switch
            {
                Version.v1 => OffsetEncoding.RunLengthCompressed,
                Version.v2 => OffsetEncoding.RunLengthV2Compressed,
                _ => throw new NotSupportedException()
            };
        }

        public override void EncodeIncompleteOffset(long relativeOffset)
        {
            EncodeRunLength(false, relativeOffset);
        }

        public override void EncodeCompletedOffset(long relativeOffset)
        {
            EncodeRunLength(true, relativeOffset);
        }

        public override byte[] Serialise()
        {
            AddTail();

            int entryWidth = Version switch
            {
                Version.v1 => sizeof(short),
                Version.v2 => sizeof(int),
                _ => throw new NotSupportedException()
            };

            ByteBuffer runLengthEncodedByteBuffer = ByteBuffer.Allocate(RunLengthEncodingIntegers.Count * entryWidth);

            foreach (int runLength in RunLengthEncodingIntegers)
            {
                switch (Version)
                {
                    case Version.v1:
                        try
                        {
                            runLengthEncodedByteBuffer.PutShort(MathUtils.ToShortExact(runLength));
                        }
                        catch (ArithmeticException e)
                        {
                            throw new RunLengthV1EncodingNotSupported($"Run-length too long for Short ({runLength} vs Short max of {short.MaxValue})");
                        }
                        break;
                    case Version.v2:
                        runLengthEncodedByteBuffer.PutInt(runLength);
                        break;
                }
            }

            byte[] array = runLengthEncodedByteBuffer.Array();
            encodedBytes = Optional.Of(array);
            return array;
        }

        private void AddTail()
        {
            RunLengthEncodingIntegers.Add(currentRunLengthSize);
        }

        public override int GetEncodedSize()
        {
            return encodedBytes.Get().Length;
        }

        protected override byte[] GetEncodedBytes()
        {
            return encodedBytes.Get();
        }

        private long previousRangeIndex = KAFKA_OFFSET_ABSENCE;

        private void EncodeRunLength(bool currentIsComplete, long relativeOffset)
        {
            long delta = relativeOffset - previousRangeIndex;
            bool currentOffsetMatchesOurRunLengthState = previousRunLengthState == currentIsComplete;
            if (currentOffsetMatchesOurRunLengthState)
            {
                switch (Version)
                {
                    case Version.v1:
                        try
                        {
                            int deltaAsInt = Math.ToIntExact(delta);
                            int newRunLength = Math.AddExact(currentRunLengthSize, deltaAsInt);
                            currentRunLengthSize = MathUtils.ToShortExact(newRunLength);
                        }
                        catch (ArithmeticException e)
                        {
                            throw new RunLengthV1EncodingNotSupported($"Run-length too big for Short ({currentRunLengthSize + delta} vs max of {short.MaxValue})");
                        }
                        break;
                    case Version.v2:
                        try
                        {
                            currentRunLengthSize = Math.ToIntExact(Math.AddExact(currentRunLengthSize, delta));
                        }
                        catch (ArithmeticException e)
                        {
                            throw new RunLengthV2EncodingNotSupported($"Run-length too big for Integer ({currentRunLengthSize} vs max of {int.MaxValue})");
                        }
                        break;
                }
            }
            else
            {
                previousRunLengthState = currentIsComplete;
                RunLengthEncodingIntegers.Add(currentRunLengthSize);
                currentRunLengthSize = 1;
            }
            previousRangeIndex = relativeOffset;
        }

        public List<long> CalculateSucceededActualOffsets(long originalBaseOffset)
        {
            List<long> successfulOffsets = new List<long>();
            bool succeeded = false;
            long offsetPosition = originalBaseOffset;
            foreach (int run in RunLengthEncodingIntegers)
            {
                if (succeeded)
                {
                    foreach (long integer in Range.Range(run))
                    {
                        long newGoodOffset = offsetPosition + integer;
                        successfulOffsets.Add(newGoodOffset);
                    }
                }
                offsetPosition += run;
                succeeded = !succeeded;
            }
            return successfulOffsets;
        }
    }
}