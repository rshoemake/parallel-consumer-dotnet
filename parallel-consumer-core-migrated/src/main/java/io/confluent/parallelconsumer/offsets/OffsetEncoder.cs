using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO.Compression;

namespace OffsetEncoder
{
    public abstract class OffsetEncoder
    {
        protected readonly OffsetEncoding.Version version;
        private readonly OffsetSimultaneousEncoder offsetSimultaneousEncoder;

        protected OffsetEncoder(OffsetSimultaneousEncoder offsetSimultaneousEncoder, OffsetEncoding.Version version)
        {
            this.offsetSimultaneousEncoder = offsetSimultaneousEncoder;
            this.version = version;
        }

        protected abstract OffsetEncoding GetEncodingType();

        protected abstract OffsetEncoding GetEncodingTypeCompressed();

        protected abstract void EncodeIncompleteOffset(long relativeOffset);

        protected abstract void EncodeCompletedOffset(long relativeOffset);

        protected abstract byte[] Serialise();

        protected abstract int GetEncodedSize();

        protected bool QuiteSmall()
        {
            return GetEncodedSize() < OffsetSimultaneousEncoder.LARGE_ENCODED_SIZE_THRESHOLD_BYTES;
        }

        protected byte[] Compress()
        {
            return CompressZstd(GetEncodedBytes());
        }

        protected void Register()
        {
            byte[] bytes = Serialise();
            OffsetEncoding encodingType = GetEncodingType();
            Register(encodingType, bytes);
        }

        private void Register(OffsetEncoding type, byte[] bytes)
        {
            Console.WriteLine("Registering {0}, with size {1}", type, bytes.Length);
            EncodedOffsetPair encodedPair = new EncodedOffsetPair(type, new MemoryStream(bytes));
            offsetSimultaneousEncoder.sortedEncodings.Add(encodedPair);
            offsetSimultaneousEncoder.encodingMap.Add(type, bytes);
        }

        protected void RegisterCompressed()
        {
            byte[] compressed = Compress();
            OffsetEncoding encodingType = GetEncodingTypeCompressed();
            Register(encodingType, compressed);
        }

        protected abstract byte[] GetEncodedBytes();

        private byte[] CompressZstd(byte[] input)
        {
            using (MemoryStream output = new MemoryStream())
            {
                using (ZstdStream compressor = new ZstdStream(output, CompressionMode.Compress))
                {
                    compressor.Write(input, 0, input.Length);
                }
                return output.ToArray();
            }
        }
    }
}