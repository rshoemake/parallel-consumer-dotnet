using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using NUnit.Framework;

namespace io.confluent.parallelconsumer.state
{
    [TestFixture]
    public class PartitionStateCommittedOffsetTest
    {
        private ModelUtils mu = new ModelUtils(new PCModuleTestEnv());

        private TopicPartition tp = new TopicPartition("topic", 0);

        private long unexpectedlyHighOffset = 20L;

        private long previouslyCommittedOffset = 11L;

        private readonly long highestSeenOffset = 101L;

        private List<long> trackedIncompletes = new List<long> { 11L, 15L, 20L, 60L, 80L, 95L, 96L, 97L, 98L, 100L };

        private List<long> expectedTruncatedIncompletes;

        private HighestOffsetAndIncompletes offsetData;

        private PartitionState<string, string> state;

        [SetUp]
        public void SetUp()
        {
            expectedTruncatedIncompletes = trackedIncompletes.Where(offset => offset >= unexpectedlyHighOffset).ToList();
            offsetData = new HighestOffsetAndIncompletes(Optional.Of(highestSeenOffset), new SortedSet<long>(trackedIncompletes));
            state = new PartitionState<string, string>(0, mu.GetModule(), tp, offsetData);
        }

        [Test]
        public void ConcurrentSkipListMapSanityCheck()
        {
            ConcurrentSkipListMap<long, bool> incompletes = new ConcurrentSkipListMap<long, bool>();
            incompletes.TryAdd(2L, true);
            incompletes.TryAdd(3L, true);
            incompletes.TryAdd(4L, true);
            incompletes.TryAdd(5L, true);
            incompletes.TryAdd(6L, true);

            ConcurrentSkipListMap<long, bool> polled = new ConcurrentSkipListMap<long, bool>();
            polled.TryAdd(3L, true);
            polled.TryAdd(5L, true);

            long lowPoll = polled.FirstKey;
            long highPoll = polled.LastKey;

            var polledRange = incompletes.Keys.SubSet(lowPoll, true, highPoll, true);

            foreach (var x in polledRange)
            {
                if (polled.ContainsKey(x))
                {
                    Console.WriteLine("Found: " + x);
                }
                else
                {
                    Console.WriteLine("Not found, dropping: " + x);
                    incompletes.TryRemove(x, out _);
                }
            }

            Assert.That(incompletes.Keys, Is.EquivalentTo(new List<long> { 2L, 3L, 5L, 6L }));
        }

        [Test]
        public void CompactedTopic()
        {
            HashSet<long> missingOffsets = new HashSet<long> { 80L, 95L, 97L };
            long slightlyLowerRange = highestSeenOffset - 2L;
            List<long> polledOffsetsWithCompactedRemoved = Enumerable.Range((int)previouslyCommittedOffset, (int)(slightlyLowerRange - previouslyCommittedOffset + 1))
                .Where(offset => !missingOffsets.Contains(offset))
                .ToList();

            PolledTestBatch polledTestBatchWithoutMissingOffsets = new PolledTestBatch(mu, tp, polledOffsetsWithCompactedRemoved);

            AddPollToState(state, polledTestBatchWithoutMissingOffsets);

            OffsetAndMetadata offsetAndMetadata = state.CreateOffsetAndMetadata();

            Assert.That(offsetAndMetadata.Offset, Is.EqualTo(previouslyCommittedOffset));

            var incompletesWithoutMissingOffsets = trackedIncompletes.Where(offset => !missingOffsets.Contains(offset)).ToList();
            Assert.That(state.GetAllIncompleteOffsets(), Is.EquivalentTo(incompletesWithoutMissingOffsets));
        }

        [Test]
        public void CommittedOffsetLower()
        {
            long randomlyChosenStepBackwards = 5L;
            long unexpectedLowerOffset = previouslyCommittedOffset - randomlyChosenStepBackwards;

            PolledTestBatch polledTestBatch = new PolledTestBatch(mu, tp, unexpectedLowerOffset, highestSeenOffset);

            AddPollToState(state, polledTestBatch);

            OffsetAndMetadata offsetAndMetadata = state.CreateOffsetAndMetadata();

            Assert.That(offsetAndMetadata.Offset, Is.EqualTo(unexpectedLowerOffset));
            Assert.That(state.GetAllIncompleteOffsets(), Is.EquivalentTo(Enumerable.Range((int)unexpectedLowerOffset, (int)(highestSeenOffset - unexpectedLowerOffset + 1)).ToList()));
        }

        private void AddPollToState(PartitionState<string, string> state, PolledTestBatch polledTestBatch)
        {
            state.MaybeRegisterNewPollBatchAsWork(polledTestBatch.PolledRecordBatch.Records(state.GetTp()));
        }

        [Test]
        public void BootstrapPollOffsetHigherDueToRetentionOrCompaction()
        {
            PolledTestBatch polledTestBatch = new PolledTestBatch(mu, tp, unexpectedlyHighOffset, highestSeenOffset);

            AddPollToState(state, polledTestBatch);

            Assert.That(state.GetOffsetToCommit(), Is.EqualTo(unexpectedlyHighOffset));
            OffsetAndMetadata offsetAndMetadata = state.CreateOffsetAndMetadata();

            Assert.That(offsetAndMetadata.Offset, Is.EqualTo(unexpectedlyHighOffset));
            Assert.That(state.GetAllIncompleteOffsets(), Is.EquivalentTo(expectedTruncatedIncompletes));
        }
    }
}