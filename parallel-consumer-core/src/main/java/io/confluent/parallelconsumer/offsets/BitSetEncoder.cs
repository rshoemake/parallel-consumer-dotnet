using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace io.confluent.parallelconsumer.offsets
{
    /*-
     * Copyright (C) 2020-2022 Confluent, Inc.
     */

    using io.confluent.csid.utils;
    using io.confluent.csid.utils.StringUtils;
    using io.confluent.parallelconsumer.internal;
    using io.confluent.parallelconsumer.state;
    using lombok.extern.slf4j.Slf4j;

    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;

    using static io.confluent.parallelconsumer.offsets.OffsetEncoding;

    /**
     * Encodes a range of offsets, from an incompletes collection into a BitSet.
     * <p>
     * Highly efficient when the completion status is random.
     * <p>
     * Highly inefficient when the completion status is in large blocks ({@link RunLengthEncoder} is much better)
     * <p>
     * Because our system works on manipulating INCOMPLETE offsets, it doesn't matter if the offset range we're encoding is
     * Sequential or not. Because as records are always in commit order, if we've seen a range of offsets, we know we've
     * seen all that exist (within said range). So if offset 8 is missing from the partition, we will encode it as having
     * been completed (when in fact it doesn't exist), because we only compare against known incompletes, and assume all
     * others are complete. See {@link PartitionState#incompleteOffsets} for more discussion on this.
     * <p>
     * So, when we deserialize, the INCOMPLETES collection is then restored, and that's what's used to compare to see if a
     * record should be skipped or not. So if record 8 is recorded as completed, it will be absent from the restored
     * INCOMPLETES list, and we are assured we will never see record 8.
     *
     * @author Antony Stubbs
     * @see PartitionState#incompleteOffsets
     * @see RunLengthEncoder
     * @see OffsetBitSet
     */
    [ToString(callSuper = true)]
    [Slf4j]
    public class BitSetEncoder : OffsetEncoder
    {

        private static readonly Version DEFAULT_VERSION = Version.v2;

        /**
         * {@link BitSet} only supports {@link Integer#MAX_VALUE) bits
         */
        public static readonly int MAX_LENGTH_ENCODABLE = int.MaxValue;

        public BitSet bitSet { get; }

        private readonly long originalLength;

        private Optional<byte[]> encodedBytes = Optional.empty();

        /**
         * @param length the difference between the highest and lowest offset to be encoded
         */
        public BitSetEncoder(long length, OffsetSimultaneousEncoder offsetSimultaneousEncoder, Version newVersion) : base(offsetSimultaneousEncoder, newVersion)
        {

            // prep bit set buffer, range check above
            try
            {
                bitSet = new BitSet((int)length);
            }
            catch (ArithmeticException e)
            {
                throw new BitSetEncodingNotSupportedException("BitSet only supports " + MAX_LENGTH_ENCODABLE + " bits, but " + length + " were requested", e);
            }

            this.originalLength = length;
        }

        private ByteBuffer constructWrappedByteBuffer(long length, Version newVersion)
        {
            return newVersion switch
            {
                Version.v1 => initV1(length),
                Version.v2 => initV2(length),
                _ => throw new ArgumentException("Invalid version")
            };
        }

        /**
         * Switch from encoding bitset length as a short to an integer (Short.MAX_VALUE size of 32,000 was too short).
         * <p>
         * Integer.MAX_VALUE is the most we can use, as {@link BitSet} only supports {@link Integer#MAX_VALUE} bits.
         */
        // TODO refactor inivtV2 and V1 together, passing in the Short or Integer
        private ByteBuffer initV2(long bitsetEntriesRequired)
        {
            if (bitsetEntriesRequired > MAX_LENGTH_ENCODABLE)
            {
                // need to upgrade to using Integer for the bitset length, but can't change serialisation format in-place
                throw new BitSetEncodingNotSupportedException(StringUtils.msg("BitSet V2 too long to encode, as length overflows Integer.MAX_VALUE. Length: {}. (max: {})", bitsetEntriesRequired, MAX_LENGTH_ENCODABLE));
            }

            int bytesRequiredForEntries = (int)(Math.Ceiling((double)bitsetEntriesRequired / 8));
            int lengthEntryWidth = sizeof(int);
            int wrappedBufferLength = lengthEntryWidth + bytesRequiredForEntries + 1;
            ByteBuffer wrappedBitSetBytesBuffer = ByteBuffer.Allocate(wrappedBufferLength);

            // bitset doesn't serialise it's set capacity, so we have to as the unused capacity actually means something
            wrappedBitSetBytesBuffer.PutInt((int)bitsetEntriesRequired);

            return wrappedBitSetBytesBuffer;
        }

        /**
         * This was a bit "short" sighted of me.... Encodes the capacity of the bitset as a short, which is only ~32,000
         * bits ({@link Short#MAX_VALUE}).
         */
        private ByteBuffer initV1(long bitsetEntriesRequired)
        {
            if (bitsetEntriesRequired > short.MaxValue)
            {
                // need to upgrade to using Integer for the bitset length, but can't change serialisation format in-place
                throw new BitSetEncodingNotSupportedException("Input too long to encode for BitSet V1, length overflows Short.MAX_VALUE: " + bitsetEntriesRequired + ". (max: " + short.MaxValue + ")");
            }

            int bytesRequiredForEntries = (int)(Math.Ceiling((double)bitsetEntriesRequired / 8));
            int lengthEntryWidth = sizeof(short);
            int wrappedBufferLength = lengthEntryWidth + bytesRequiredForEntries + 1;
            ByteBuffer wrappedBitSetBytesBuffer = ByteBuffer.Allocate(wrappedBufferLength);

            // bitset doesn't serialise it's set capacity, so we have to as the unused capacity actually means something
            wrappedBitSetBytesBuffer.PutShort(MathUtils.ToShortExact(bitsetEntriesRequired));

            return wrappedBitSetBytesBuffer;
        }

        protected override OffsetEncoding getEncodingType()
        {
            return version switch
            {
                Version.v1 => BitSet,
                Version.v2 => BitSetV2,
                _ => throw new ArgumentException("Invalid version")
            };
        }

        protected override OffsetEncoding getEncodingTypeCompressed()
        {
            return version switch
            {
                Version.v1 => BitSetCompressed,
                Version.v2 => BitSetV2Compressed,
                _ => throw new ArgumentException("Invalid version")
            };
        }

        public override void encodeIncompleteOffset(long relativeOffset)
        {
            // noop - bitset defaults to 0's (`unset`)
        }

        public override void encodeCompletedOffset(long relativeOffset)
        {
            // range will already have been checked at initialization
            bitSet.Set((int)relativeOffset);
        }

        public override byte[] serialise()
        {
            byte[] bitSetArray = this.bitSet.ToByteArray();
            ByteBuffer wrappedBitSetBytesBuffer = constructWrappedByteBuffer(originalLength, version);

            if (wrappedBitSetBytesBuffer.Remaining() < bitSetArray.Length)
                throw new InternalRuntimeException("Not enough space in byte array");

            try
            {
                wrappedBitSetBytesBuffer.Put(bitSetArray);
            }
            catch (BufferOverflowException e)
            {
                throw new InternalRuntimeException("Error copying bitset into byte wrapper", e);
            }

            byte[] array = wrappedBitSetBytesBuffer.Array();
            this.encodedBytes = Optional.Of(array);
            return array;
        }

        public override int getEncodedSize()
        {
            return this.encodedBytes.Get().Length;
        }

        protected override byte[] getEncodedBytes()
        {
            return this.encodedBytes.Get();
        }
    }
}