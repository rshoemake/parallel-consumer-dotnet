using System;
using System.Collections.Generic;
using System.Text;

namespace OffsetCodecTestUtils
{
    public static class OffsetCodecTestUtils
    {
        /**
         * x is complete
         * <p>
         * o is incomplete
         */
        public static string IncompletesToBitmapString(long finalOffsetForPartition, long highestSeen, HashSet<long> incompletes)
        {
            StringBuilder runLengthString = new StringBuilder();
            long lowWaterMark = finalOffsetForPartition;
            long end = highestSeen - lowWaterMark;
            for (long relativeOffset = 0; relativeOffset < end; relativeOffset++)
            {
                long offset = lowWaterMark + relativeOffset;
                if (incompletes.Contains(offset))
                {
                    runLengthString.Append("o");
                }
                else
                {
                    runLengthString.Append("x");
                }
            }
            return runLengthString.ToString();
        }

        public static string IncompletesToBitmapString(long finalOffsetForPartition, PartitionState state)
        {
            return IncompletesToBitmapString(finalOffsetForPartition, state.OffsetHighestSeen, state.IncompleteOffsetsBelowHighestSucceeded);
        }

        /**
         * x is complete
         * <p>
         * o is incomplete
         */
        public static SortedSet<long> BitmapStringToIncomplete(long baseOffset, string inputBitmapString)
        {
            SortedSet<long> incompleteOffsets = new SortedSet<long>();

            long longLength = inputBitmapString.Length;
            for (long index = 0; index < longLength; index++)
            {
                char bit = inputBitmapString[(int)index];
                if (bit == 'o')
                {
                    incompleteOffsets.Add(baseOffset + index);
                }
                else if (bit == 'x')
                {
                    Console.WriteLine("Dropping completed offset");
                }
                else
                {
                    throw new ArgumentException("Invalid encoding - unexpected char: " + bit);
                }
            }

            return incompleteOffsets;
        }
    }
}