using System;
using System.Collections.Generic;
using System.Threading;
using Confluent.Kafka;
using Moq;
using NodaTime;
using ParallelConsumer.Internal;
using ParallelConsumer.State;

namespace ParallelConsumer.Internal
{
    public class PCModuleTestEnv : PCModule<string, string>
    {
        private ModelUtils mu = new ModelUtils(this);
        private Optional<CountDownLatch> workManagerController;
        private WorkManager<string, string> workManager;
        private readonly DynamicLoadFactor limitedDynamicLoadFactor = new LimitedDynamicExtraLoadFactor();

        protected override DynamicLoadFactor DynamicExtraLoadFactor()
        {
            return limitedDynamicLoadFactor;
        }

        public PCModuleTestEnv(ParallelConsumerOptions<string, string> optionsInstance, CountDownLatch latch) : base(optionsInstance)
        {
            this.workManagerController = Optional.Of(latch);
        }

        public PCModuleTestEnv(ParallelConsumerOptions<string, string> optionsInstance) : base(optionsInstance)
        {
            ParallelConsumerOptions<string, string> overrideOptions = EnhanceOptions(optionsInstance);

            // overwrite super's with new instance
            base.optionsInstance = overrideOptions;
            this.workManagerController = Optional.Empty();
        }

        private ParallelConsumerOptions<string, string> EnhanceOptions(ParallelConsumerOptions<string, string> optionsInstance)
        {
            var copy = options.ToBuilder();

            if (optionsInstance.Consumer == null)
            {
                Consumer<string, string> mockConsumer = new Mock<Consumer<string, string>>().Object;
                Mock.Get(mockConsumer).Setup(x => x.GroupMetadata).Returns(mu.ConsumerGroupMeta());
                copy.Consumer(mockConsumer);
            }

            var overrideOptions = copy
                .Producer(new Mock<Producer<string, string>>().Object)
                .Build();

            return overrideOptions;
        }

        public PCModuleTestEnv() : this(ParallelConsumerOptions<string, string>.Builder().Build())
        {
        }

        protected override ProducerWrapper<string, string> ProducerWrap()
        {
            return MockProducerWrapTransactional();
        }

        private ProducerWrapper<string, string> mockProduceWrap;

        private ProducerWrapper<string, string> MockProducerWrapTransactional()
        {
            if (mockProduceWrap == null)
            {
                mockProduceWrap = new Mock<ProducerWrapper<string, string>>(options, true, Producer()).Object;
            }
            return mockProduceWrap;
        }

        public override WorkManager<string, string> WorkManager()
        {
            if (this.workManager == null)
            {
                this.workManager = this.workManagerController.IsPresent ?
                    new PausableWorkManager<string, string>(this, DynamicExtraLoadFactor(), workManagerController.Get())
                    : base.WorkManager();
            }
            return workManager;
        }

        protected override ConsumerManager<string, string> ConsumerManager()
        {
            ConsumerManager<string, string> consumerManager = base.ConsumerManager();

            // force update to set cache, otherwise maybe never called (fake consumer)
            consumerManager.UpdateMetadataCache();

            return consumerManager;
        }

        private readonly MutableClock mutableClock = NodaTime.SystemClock.Instance as MutableClock;

        public override Clock Clock()
        {
            return mutableClock;
        }
    }
}