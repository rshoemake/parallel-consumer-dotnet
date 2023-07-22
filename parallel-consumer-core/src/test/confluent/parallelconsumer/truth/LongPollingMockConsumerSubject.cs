using System;
using System.Collections.Generic;
using System.Linq;
using Google.Truth;
using Google.Truth.Extensions;
using Kafka.Common;
using Kafka.Consumers;
using Kafka.Models;
using Kafka.Truth.CommitHistory;

namespace Kafka.Truth.LongPollingMockConsumer
{
    public class LongPollingMockConsumerSubject<K, V> : Subject
    {
        private readonly LongPollingMockConsumer<K, V> _actual;

        protected LongPollingMockConsumerSubject(FailureMetadata metadata, LongPollingMockConsumer<K, V> actual) : base(metadata, actual)
        {
            _actual = actual;
        }

        [SubjectFactoryMethod]
        public static LongPollingMockConsumerSubject<K, V> MockConsumers()
        {
            return new LongPollingMockConsumerSubject<K, V>();
        }

        public static LongPollingMockConsumerSubject<K, V> AssertTruth(LongPollingMockConsumer<K, V> actual)
        {
            return AssertThat(actual);
        }

        public static LongPollingMockConsumerSubject<K, V> AssertThat(LongPollingMockConsumer<K, V> actual)
        {
            var factory = LongPollingMockConsumerSubject<K, V>.MockConsumers();
            return Assert.About(factory).That(actual);
        }

        public CommitHistorySubject HasCommittedToPartition(TopicPartition tp)
        {
            IsNotNull();
            var allCommits = _actual.GetCommitHistoryInt();
            var historyForCommitsToPartition = allCommits
                .Where(aCommitInstance => aCommitInstance.ContainsKey(tp))
                .Select(aCommitInstance => aCommitInstance[tp])
                .ToList();
            var commitHistory = new CommitHistory(historyForCommitsToPartition);
            return Check("GetCommitHistory({0})", tp).About(CommitHistories.CommitHistories()).That(commitHistory);
        }

        public CommitHistorySubject HasCommittedToAnyPartition()
        {
            IsNotNull();
            var allCommits = _actual.GetCommitHistoryInt();
            var historyForCommitsToPartition = allCommits
                .SelectMany(aCommitInstance => aCommitInstance.Values)
                .ToList();
            var commitHistory = new CommitHistory(historyForCommitsToPartition);
            return Check("GetCommitHistory()").About(CommitHistories.CommitHistories()).That(commitHistory);
        }
    }
}