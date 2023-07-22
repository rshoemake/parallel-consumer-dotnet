using FluentAssertions;
using Google.Common.Truth;
using System;

namespace io.confluent.parallelconsumer.truth
{
    public class ProducerManagerSubject : ProducerManagerParentSubject, IUserManagedMiddleSubject
    {
        protected ProducerManagerSubject(FailureMetadata failureMetadata, ProducerManager actual) : base(failureMetadata, actual)
        {
        }

        public static ProducerManagerSubject ProducerManagers(FailureMetadata failureMetadata, ProducerManager actual)
        {
            return new ProducerManagerSubject(failureMetadata, actual);
        }

        public void TransactionNotOpen()
        {
            Actual.ProducerWrapper.IsTransactionOpen.Should().BeFalse();
        }

        public void TransactionOpen()
        {
            Actual.ProducerWrapper.IsTransactionOpen.Should().BeTrue();
        }

        public void StateIs(ProducerWrapper.ProducerState targetState)
        {
            var producerWrap = Actual.ProducerWrapper;
            var producerState = producerWrap.ProducerState;
            producerState.Should().Be(targetState);
        }
    }
}