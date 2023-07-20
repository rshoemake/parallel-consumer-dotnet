using System;
using Confluent.Kafka;
using Confluent.ParallelConsumer;
using Confluent.ParallelConsumer.Internal;
using Confluent.ParallelConsumer.State;

namespace IO.Confluent.ParallelConsumer.Internal
{
    public class PCModule<K, V>
    {
        protected ParallelConsumerOptions<K, V> optionsInstance;

        public AbstractParallelEoSStreamProcessor<K, V> ParallelEoSStreamProcessor { get; set; }

        public PCModule(ParallelConsumerOptions<K, V> options)
        {
            this.optionsInstance = options;
        }

        public ParallelConsumerOptions<K, V> Options()
        {
            return optionsInstance;
        }

        private ProducerWrapper<K, V> producerWrapper;

        protected ProducerWrapper<K, V> ProducerWrap()
        {
            if (this.producerWrapper == null)
            {
                this.producerWrapper = new ProducerWrapper<K, V>(Options());
            }
            return producerWrapper;
        }

        private ProducerManager<K, V> producerManager;

        protected ProducerManager<K, V> ProducerManager()
        {
            if (producerManager == null)
            {
                this.producerManager = new ProducerManager<K, V>(ProducerWrap(), ConsumerManager(), WorkManager(), Options());
            }
            return producerManager;
        }

        public IProducer<K, V> Producer()
        {
            return optionsInstance.Producer;
        }

        public IConsumer<K, V> Consumer()
        {
            return optionsInstance.Consumer;
        }

        private ConsumerManager<K, V> consumerManager;

        protected ConsumerManager<K, V> ConsumerManager()
        {
            if (consumerManager == null)
            {
                consumerManager = new ConsumerManager<K, V>(optionsInstance.Consumer);
            }
            return consumerManager;
        }

        public WorkManager<K, V> WorkManager { get; set; }

        protected WorkManager<K, V> WorkManager()
        {
            if (WorkManager == null)
            {
                WorkManager = new WorkManager<K, V>(this, DynamicExtraLoadFactor());
            }
            return WorkManager;
        }

        protected AbstractParallelEoSStreamProcessor<K, V> PC()
        {
            if (ParallelEoSStreamProcessor == null)
            {
                ParallelEoSStreamProcessor = new ParallelEoSStreamProcessor<K, V>(Options(), this);
            }
            return ParallelEoSStreamProcessor;
        }

        private DynamicLoadFactor dynamicLoadFactor = new DynamicLoadFactor();

        protected DynamicLoadFactor DynamicExtraLoadFactor()
        {
            return dynamicLoadFactor;
        }

        private BrokerPollSystem<K, V> brokerPollSystem;

        protected BrokerPollSystem<K, V> BrokerPoller(AbstractParallelEoSStreamProcessor<K, V> pc)
        {
            if (brokerPollSystem == null)
            {
                brokerPollSystem = new BrokerPollSystem<K, V>(ConsumerManager(), WorkManager(), pc, Options());
            }
            return brokerPollSystem;
        }

        public Clock Clock()
        {
            return TimeUtils.GetClock();
        }
    }
}