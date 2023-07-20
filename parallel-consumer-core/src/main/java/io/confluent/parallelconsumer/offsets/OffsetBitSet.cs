using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;

namespace io.confluent.parallelconsumer.offsets
{
    /**
     * Deserialization tools for {@link BitSetEncoder}.
     * <p>
     * todo unify or refactor with {@link BitSetEncoder}. Why was it ever separate?
     *
     * @author Antony Stubbs
     * @see BitSetEncoder
     */
    public class OffsetBitSet
    {
        private static string DeserialiseBitSetWrap(ByteBuffer wrap, OffsetEncoding.Version version)
        {
            wrap.Rewind();

            int originalBitsetSize = version switch
            {
                OffsetEncoding.Version.v1 => (int)wrap.GetShort(),
                OffsetEncoding.Version.v2 => wrap.GetInt(),
                _ => throw new ArgumentException("Invalid version")
            };

            ByteBuffer slice = wrap.Slice();
            return DeserialiseBitSet(originalBitsetSize, slice);
        }

        private static string DeserialiseBitSet(int originalBitsetSize, ByteBuffer s)
        {
            BitSet bitSet = BitSet.ValueOf(s);

            StringBuilder result = new StringBuilder(bitSet.Size());
            foreach (long offset in Range(originalBitsetSize))
            {
                // range will already have been checked at initialization
                if (bitSet.Get((int)offset))
                {
                    result.Append('x');
                }
                else
                {
                    result.Append('o');
                }
            }

            return result.ToString();
        }

        private static HighestOffsetAndIncompletes DeserialiseBitSetWrapToIncompletes(OffsetEncoding encoding, long baseOffset, ByteBuffer wrap)
        {
            wrap.Rewind();
            int originalBitsetSize = encoding switch
            {
                OffsetEncoding.BitSet => wrap.GetShort(),
                OffsetEncoding.BitSetV2 => wrap.GetInt(),
                _ => throw new ArgumentException("Invalid encoding")
            };
            ByteBuffer slice = wrap.Slice();
            SortedSet<long> incompletes = DeserialiseBitSetToIncompletes(baseOffset, originalBitsetSize, slice);
            long highestSeenOffset = baseOffset + originalBitsetSize - 1;
            return HighestOffsetAndIncompletes.Of(highestSeenOffset, incompletes);
        }

        private static SortedSet<long> DeserialiseBitSetToIncompletes(long baseOffset, int originalBitsetSize, ByteBuffer inputBuffer)
        {
            BitSet bitSet = BitSet.ValueOf(inputBuffer);
            SortedSet<long> incompletes = new SortedSet<long>();
            foreach (long relativeOffsetLong in Range(originalBitsetSize))
            {
                // range will already have been checked at initialization
                int relativeOffset = (int)relativeOffsetLong;
                long offset = baseOffset + relativeOffset;
                if (bitSet.Get(relativeOffset))
                {
                    log.Trace("Ignoring completed offset {}", relativeOffset);
                }
                else
                {
                    incompletes.Add(offset);
                }
            }
            return incompletes;
        }
    }
}