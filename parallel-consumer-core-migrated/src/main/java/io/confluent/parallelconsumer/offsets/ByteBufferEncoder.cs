using System;
using System.Linq;
using System.Text;

namespace Confluent.ParallelConsumer.Offsets
{
    public class ByteBufferEncoder : OffsetEncoder
    {
        private readonly ByteBuffer bytesBuffer;

        public ByteBufferEncoder(long length, OffsetSimultaneousEncoder offsetSimultaneousEncoder) : base(offsetSimultaneousEncoder, OffsetEncoding.Version.v1)
        {
            var safeCast = (int)length;
            this.bytesBuffer = ByteBuffer.Allocate(1 + safeCast);
        }

        protected override OffsetEncoding GetEncodingType()
        {
            return OffsetEncoding.ByteArray;
        }

        protected override OffsetEncoding GetEncodingTypeCompressed()
        {
            return OffsetEncoding.ByteArrayCompressed;
        }

        public override void EncodeIncompleteOffset(long relativeOffset)
        {
            this.bytesBuffer.Put((byte)0);
        }

        public override void EncodeCompletedOffset(long relativeOffset)
        {
            this.bytesBuffer.Put((byte)1);
        }

        public override byte[] Serialise()
        {
            return this.bytesBuffer.Array();
        }

        public override int GetEncodedSize()
        {
            return this.bytesBuffer.Capacity();
        }

        protected override byte[] GetEncodedBytes()
        {
            return this.bytesBuffer.Array();
        }
    }
}