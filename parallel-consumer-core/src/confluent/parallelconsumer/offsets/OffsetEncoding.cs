using System;
using System.Collections.Generic;
using System.Linq;

namespace OffsetEncoding
{
    public enum Version
    {
        v1, v2
    }

    public enum OffsetEncoding
    {
        ByteArray(v1, (byte)'L'),
        ByteArrayCompressed(v1, (byte)'î'),
        BitSet(v1, (byte)'l'),
        BitSetCompressed(v1, (byte)'a'),
        RunLength(v1, (byte)'n'),
        RunLengthCompressed(v1, (byte)'J'),
        BitSetV2(v2, (byte)'o'),
        BitSetV2Compressed(v2, (byte)'s'),
        RunLengthV2(v2, (byte)'e'),
        RunLengthV2Compressed(v2, (byte)'p'),
        KafkaStreams(v1, (byte)1),
        KafkaStreamsV2(v2, (byte)2);

        public Version version;
        public byte magicByte;

        private static readonly Dictionary<byte, OffsetEncoding> magicMap = Enum.GetValues(typeof(OffsetEncoding))
            .Cast<OffsetEncoding>()
            .ToDictionary(x => x.magicByte, x => x);

        private OffsetEncoding(Version version, byte magicByte)
        {
            this.version = version;
            this.magicByte = magicByte;
        }

        public static OffsetEncoding Decode(byte magic)
        {
            if (magicMap.TryGetValue(magic, out var encoding))
            {
                return encoding;
            }
            else
            {
                throw new Exception("Unexpected magic: " + magic);
            }
        }

        public string Description()
        {
            return $"{Enum.GetName(typeof(OffsetEncoding), this)}:{version}";
        }
    }
}