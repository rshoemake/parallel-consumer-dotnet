using System;
using FluentAssertions;
using Google.Truth;
using Xunit;

namespace io.confluent.parallelconsumer.truth
{
    public class ConsumerRecordsSubjectTests
    {
        [Fact]
        public void HasHeadOffsetAtLeastInAnyTopicPartition_ShouldPass_WhenHeadOffsetIsAtLeastTarget()
        {
            // Arrange
            var consumerRecords = new ConsumerRecords<object, object>();
            consumerRecords.Add(new ConsumerRecord<object, object>("topic", 0, 0, null, null));
            var subject = new ConsumerRecordsSubject(new FailureMetadata(), consumerRecords);

            // Act
            Action act = () => subject.HasHeadOffsetAtLeastInAnyTopicPartition(1);

            // Assert
            act.Should().NotThrow();
        }

        [Fact]
        public void HasHeadOffsetAtLeastInAnyTopicPartition_ShouldFail_WhenHeadOffsetIsLessThanTarget()
        {
            // Arrange
            var consumerRecords = new ConsumerRecords<object, object>();
            consumerRecords.Add(new ConsumerRecord<object, object>("topic", 0, 0, null, null));
            var subject = new ConsumerRecordsSubject(new FailureMetadata(), consumerRecords);

            // Act
            Action act = () => subject.HasHeadOffsetAtLeastInAnyTopicPartition(2);

            // Assert
            act.Should().Throw<Exception>().WithMessage("headOffset < 2");
        }

        [Fact]
        public void HasHeadOffsetAtMostInAnyTopicPartition_ShouldPass_WhenHeadOffsetIsAtMostTarget()
        {
            // Arrange
            var consumerRecords = new ConsumerRecords<object, object>();
            consumerRecords.Add(new ConsumerRecord<object, object>("topic", 0, 0, null, null));
            var subject = new ConsumerRecordsSubject(new FailureMetadata(), consumerRecords);

            // Act
            Action act = () => subject.HasHeadOffsetAtMostInAnyTopicPartition(1);

            // Assert
            act.Should().NotThrow();
        }

        [Fact]
        public void HasHeadOffsetAtMostInAnyTopicPartition_ShouldFail_WhenHeadOffsetIsGreaterThanTarget()
        {
            // Arrange
            var consumerRecords = new ConsumerRecords<object, object>();
            consumerRecords.Add(new ConsumerRecord<object, object>("topic", 0, 0, null, null));
            var subject = new ConsumerRecordsSubject(new FailureMetadata(), consumerRecords);

            // Act
            Action act = () => subject.HasHeadOffsetAtMostInAnyTopicPartition(0);

            // Assert
            act.Should().Throw<Exception>().WithMessage("headOffset > 0");
        }
    }
}