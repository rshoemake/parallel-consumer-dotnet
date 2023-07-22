using System;
using System.Collections.Generic;
using System.Linq;
using Google.Common.Truth;
using Google.Common.Truth.Optional;
using Google.Common.Truth.Subjects;
using IO.Confluent.ParallelConsumer.Model;
using IO.Stubbs.Truth.Generator;

namespace IO.Confluent.ParallelConsumer.Truth
{
    [ToString]
    [UserManagedSubject(typeof(CommitHistory))]
    public class CommitHistorySubject : Subject
    {
        private readonly CommitHistory actual;

        protected CommitHistorySubject(FailureMetadata metadata, CommitHistory actual) : base(metadata, actual)
        {
            this.actual = actual;
        }

        [SubjectFactoryMethod]
        public static Factory<CommitHistorySubject, CommitHistory> CommitHistories()
        {
            return (metadata, actual) => new CommitHistorySubject(metadata, actual);
        }

        public static CommitHistorySubject AssertTruth(CommitHistory actual)
        {
            return AssertThat(actual);
        }

        public static CommitHistorySubject AssertThat(CommitHistory actual)
        {
            return Truth.AssertAbout(CommitHistories()).That(actual);
        }

        public void AtLeastOffset(long needleCommit)
        {
            Optional<long> highestCommitOpt = this.actual.HighestCommit();
            Check("highestCommit()").About(OptionalSubject.Optionals())
                .That(highestCommitOpt)
                .IsPresent();
            Check("highestCommit().atLeastOffset()")
                .That(highestCommitOpt.Value)
                .IsAtLeast(needleCommit);
        }

        public void Offset(long quantity)
        {
            Check("getOffsetHistory()").That(actual.GetOffsetHistory()).Contains(quantity);
        }

        public void Anything()
        {
            Check("commits()").That(actual.GetOffsetHistory()).IsNotEmpty();
        }

        public void Nothing()
        {
            Check("commits()").That(actual.GetOffsetHistory()).IsEmpty();
        }

        public void IsEmpty()
        {
            Nothing();
        }

        public void EncodedIncomplete(params int[] expectedEncodedOffsetsArray)
        {
            ISet<long> incompleteOffsets = actual.GetEncodedSucceeded().GetIncompleteOffsets();
            Check("encodedSucceeded()")
                .That(incompleteOffsets)
                .ContainsExactlyElementsIn(expectedEncodedOffsetsArray
                    .Select(x => (long)x)
                    .ToList());
        }

        public void EncodingEmpty()
        {
            Check("encodedMetadata()").That(actual.GetEncoding()).IsEmpty();
        }
    }
}