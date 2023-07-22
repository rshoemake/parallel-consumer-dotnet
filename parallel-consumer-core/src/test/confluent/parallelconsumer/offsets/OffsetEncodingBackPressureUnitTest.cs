using System;
using System.Collections.Generic;
using System.Linq;
using NUnit.Framework;
using TLinkowski.UniLists;
using TLinkowski.UniSets;

namespace io.confluent.parallelconsumer.offsets
{
    [TestFixture]
    public class BitSetEncodingTest
    {
        [Test]
        public void Basic()
        {
            var incompletes = UniSets.Of(0, 4, 6, 7, 8, 10).Select(x => (long)x).ToTreeSet();
            var completes = UniLists.Of(1, 2, 3, 5, 9).Select(x => (long)x).ToTreeSet();
            var offsetSimultaneousEncoder = new OffsetSimultaneousEncoder(-1, 0L, incompletes);
            int length = 11;
            var bs = new BitSetEncoder(length, offsetSimultaneousEncoder, OffsetEncoding.Version.v2);

            bs.EncodeIncompleteOffset(0);
            bs.EncodeCompletedOffset(1);
            bs.EncodeCompletedOffset(2);
            bs.EncodeCompletedOffset(3);
            bs.EncodeIncompleteOffset(4);
            bs.EncodeCompletedOffset(5);
            bs.EncodeIncompleteOffset(6);
            bs.EncodeIncompleteOffset(7);
            bs.EncodeIncompleteOffset(8);
            bs.EncodeCompletedOffset(9);
            bs.EncodeIncompleteOffset(10);

            // before serialisation
            {
                Assert.That(bs.GetBitSet().ToArray(), Is.EqualTo(new[] { 1, 2, 3, 5, 9 }));
            }

            // after serialisation
            {
                byte[] raw = bs.Serialise();

                byte[] wrapped = offsetSimultaneousEncoder.PackEncoding(new EncodedOffsetPair(OffsetEncoding.BitSetV2, ByteBuffer.Wrap(raw)));

                var result = OffsetMapCodecManager.DecodeCompressedOffsets(0, wrapped);

                Assert.That(result.HighestSeenOffset, Is.EqualTo(10L));

                Assert.That(result.IncompleteOffsets, Is.EquivalentTo(incompletes));
            }
        }
    }
}