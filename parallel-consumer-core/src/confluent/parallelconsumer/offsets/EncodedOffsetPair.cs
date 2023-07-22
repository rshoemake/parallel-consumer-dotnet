using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ParallelConsumerOffsets
{
    internal class EncodedOffsetPair : IComparable<EncodedOffsetPair>
    {
        public static readonly Comparer<EncodedOffsetPair> SIZE_COMPARATOR = Comparer<EncodedOffsetPair>.Create((x, y) => x.Data.Capacity.CompareTo(y.Data.Capacity));

        public OffsetEncoding Encoding { get; }
        public ByteBuffer Data { get; }

        public EncodedOffsetPair(OffsetEncoding encoding, ByteBuffer data)
        {
            Encoding = encoding;
            Data = data;
        }

        public int CompareTo(EncodedOffsetPair other)
        {
            return SIZE_COMPARATOR.Compare(this, other);
        }

        public override string ToString()
        {
            return $"\n{{ {Encoding.Name}, \t\t\tsize={Data.Capacity} }}";
        }

        public byte[] ReadDataArrayForDebug()
        {
            return CopyBytesOutOfBufferForDebug(Data);
        }

        private static byte[] CopyBytesOutOfBufferForDebug(ByteBuffer bbData)
        {
            bbData.Position(0);
            byte[] bytes = new byte[bbData.Remaining()];
            bbData.Get(bytes, 0, bbData.Limit());
            return bytes;
        }

        public static EncodedOffsetPair Unwrap(byte[] input)
        {
            ByteBuffer wrap = ByteBuffer.Wrap(input).AsReadOnlyBuffer();
            byte magic = wrap.Get();
            OffsetEncoding decode = Decode(magic);
            ByteBuffer slice = wrap.Slice();

            return new EncodedOffsetPair(decode, slice);
        }

        public string GetDecodedString()
        {
            string binaryArrayString = Encoding switch
            {
                OffsetEncoding.ByteArray => DeserialiseByteArrayToBitMapString(Data),
                OffsetEncoding.ByteArrayCompressed => DeserialiseByteArrayToBitMapString(DecompressZstd(Data)),
                OffsetEncoding.BitSet => DeserialiseBitSetWrap(Data, Version.V1),
                OffsetEncoding.BitSetCompressed => DeserialiseBitSetWrap(DecompressZstd(Data), Version.V1),
                OffsetEncoding.RunLength => RunLengthDecodeToString(RunLengthDeserialise(Data)),
                OffsetEncoding.RunLengthCompressed => RunLengthDecodeToString(RunLengthDeserialise(DecompressZstd(Data))),
                OffsetEncoding.BitSetV2 => DeserialiseBitSetWrap(Data, Version.V2),
                OffsetEncoding.BitSetV2Compressed => DeserialiseBitSetWrap(Data, Version.V2),
                OffsetEncoding.RunLengthV2 => DeserialiseBitSetWrap(Data, Version.V2),
                OffsetEncoding.RunLengthV2Compressed => DeserialiseBitSetWrap(Data, Version.V2),
                _ => throw new InternalRuntimeException("Invalid state")
            };
            return binaryArrayString;
        }

        public HighestOffsetAndIncompletes GetDecodedIncompletes(long baseOffset)
        {
            return GetDecodedIncompletes(baseOffset, ParallelConsumerOptions.InvalidOffsetMetadataHandlingPolicy.FAIL);
        }

        public HighestOffsetAndIncompletes GetDecodedIncompletes(long baseOffset, ParallelConsumerOptions.InvalidOffsetMetadataHandlingPolicy errorPolicy)
        {
            HighestOffsetAndIncompletes binaryArrayString = Encoding switch
            {
                OffsetEncoding.BitSet => DeserialiseBitSetWrapToIncompletes(Encoding, baseOffset, Data),
                OffsetEncoding.BitSetCompressed => DeserialiseBitSetWrapToIncompletes(OffsetEncoding.BitSet, baseOffset, DecompressZstd(Data)),
                OffsetEncoding.RunLength => RunLengthDecodeToIncompletes(Encoding, baseOffset, Data),
                OffsetEncoding.RunLengthCompressed => RunLengthDecodeToIncompletes(OffsetEncoding.RunLength, baseOffset, DecompressZstd(Data)),
                OffsetEncoding.BitSetV2 => DeserialiseBitSetWrapToIncompletes(Encoding, baseOffset, Data),
                OffsetEncoding.BitSetV2Compressed => DeserialiseBitSetWrapToIncompletes(OffsetEncoding.BitSetV2, baseOffset, DecompressZstd(Data)),
                OffsetEncoding.RunLengthV2 => RunLengthDecodeToIncompletes(Encoding, baseOffset, Data),
                OffsetEncoding.RunLengthV2Compressed => RunLengthDecodeToIncompletes(OffsetEncoding.RunLengthV2, baseOffset, DecompressZstd(Data)),
                OffsetEncoding.KafkaStreams or OffsetEncoding.KafkaStreamsV2 =>
                    errorPolicy == ParallelConsumerOptions.InvalidOffsetMetadataHandlingPolicy.IGNORE ?
                        HighestOffsetAndIncompletes.Of(baseOffset) :
                        throw new KafkaStreamsEncodingNotSupported(),
                _ => throw new NotSupportedException($"Encoding ({Encoding.Description}) not supported")
            };
            return binaryArrayString;
        }
    }
}