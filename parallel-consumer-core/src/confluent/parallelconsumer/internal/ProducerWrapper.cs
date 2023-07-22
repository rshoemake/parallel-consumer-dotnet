using System;
using System.Collections.Generic;
using System.Reflection;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Transactions;
using Confluent.Kafka.Types;
using Microsoft.Extensions.Logging;

namespace io.confluent.parallelconsumer.internal
{
    public class ProducerWrapper<K, V> : IProducer<K, V>
    {
        public enum ProducerState
        {
            INSTANTIATED,
            INIT,
            BEGIN,
            COMMIT,
            ABORT,
            CLOSE
        }

        private ProducerState _producerState = ProducerState.INSTANTIATED;

        private readonly ParallelConsumerOptions<K, V> _options;

        private readonly bool _producerIsConfiguredForTransactions;

        private FieldInfo _txManagerField;
        private MethodInfo _txManagerMethodIsCompleting;
        private MethodInfo _txManagerMethodIsReady;

        private readonly IProducer<K, V> _producer;

        public ProducerWrapper(ParallelConsumerOptions<K, V> options)
        {
            _options = options;
            _producer = options.Producer;
            _producerIsConfiguredForTransactions = DiscoverIfProducerIsConfiguredForTransactions();
        }

        public ProducerWrapper(ParallelConsumerOptions<K, V> options, IProducer<K, V> producer)
        {
            _options = options;
            _producer = producer;
            _producerIsConfiguredForTransactions = DiscoverIfProducerIsConfiguredForTransactions();
        }

        public ProducerState GetProducerState()
        {
            return _producerState;
        }

        public bool IsMockProducer()
        {
            return _producer is MockProducer<K, V>;
        }

        public bool IsConfiguredForTransactions()
        {
            return _producerIsConfiguredForTransactions;
        }

        private bool DiscoverIfProducerIsConfiguredForTransactions()
        {
            if (_producer is Producer<K, V>)
            {
                _txManagerField = _producer.GetType().GetField("transactionManager", BindingFlags.NonPublic | BindingFlags.Instance);

                bool producerIsConfiguredForTransactions = GetProducerIsTransactional();
                if (producerIsConfiguredForTransactions)
                {
                    TransactionManager transactionManager = GetTransactionManager();
                    _txManagerMethodIsCompleting = transactionManager.GetType().GetMethod("isCompleting", BindingFlags.NonPublic | BindingFlags.Instance);
                    _txManagerMethodIsReady = transactionManager.GetType().GetMethod("isReady", BindingFlags.NonPublic | BindingFlags.Instance);
                }
                return producerIsConfiguredForTransactions;
            }
            else if (_producer is MockProducer<K, V>)
            {
                return _options.UsingTransactionalProducer;
            }
            else
            {
                return false;
            }
        }

        private bool GetProducerIsTransactional()
        {
            if (_producer is MockProducer<K, V>)
            {
                return _options.UsingTransactionalProducer;
            }
            else
            {
                TransactionManager transactionManager = GetTransactionManager();
                if (transactionManager == null)
                {
                    return false;
                }
                else
                {
                    return transactionManager.IsTransactional;
                }
            }
        }

        private TransactionManager GetTransactionManager()
        {
            if (_txManagerField == null) return null;
            TransactionManager transactionManager = (TransactionManager)_txManagerField.GetValue(_producer);
            return transactionManager;
        }

        protected bool IsTransactionCompleting()
        {
            if (_producer is MockProducer<K, V>) return false;
            return (bool)_txManagerMethodIsCompleting.Invoke(GetTransactionManager(), null);
        }

        protected bool IsTransactionReady()
        {
            if (_producer is MockProducer<K, V>) return true;
            return (bool)_txManagerMethodIsReady.Invoke(GetTransactionManager(), null);
        }

        public void InitTransactions()
        {
            _producer.InitTransactions();
            _producerState = ProducerState.INIT;
        }

        public void BeginTransaction()
        {
            _producer.BeginTransaction();
            _producerState = ProducerState.BEGIN;
        }

        public void CommitTransaction()
        {
            _producer.CommitTransaction();
            _producerState = ProducerState.COMMIT;
        }

        public void AbortTransaction()
        {
            _producer.AbortTransaction();
            _producerState = ProducerState.ABORT;
        }

        public void SendOffsetsToTransaction(Dictionary<TopicPartition, OffsetAndMetadata> offsets, string consumerGroupId)
        {
            SendOffsetsToTransaction(offsets, new ConsumerGroupMetadata(consumerGroupId));
        }

        public void SendOffsetsToTransaction(Dictionary<TopicPartition, OffsetAndMetadata> offsets, ConsumerGroupMetadata groupMetadata)
        {
            _producer.SendOffsetsToTransaction(offsets, groupMetadata);
        }

        public void Close()
        {
            _producer.Close();
            _producerState = ProducerState.CLOSE;
        }

        public void Close(TimeSpan timeout)
        {
            _producer.Close(timeout);
            _producerState = ProducerState.CLOSE;
        }

        public void Flush(TimeSpan timeout)
        {
            _producer.Flush(timeout);
        }

        public void Flush(CancellationToken cancellationToken = default)
        {
            _producer.Flush(cancellationToken);
        }

        public int Poll(TimeSpan timeout)
        {
            return _producer.Poll(timeout);
        }

        public int Poll(int millisecondsTimeout)
        {
            return _producer.Poll(millisecondsTimeout);
        }

        public int Poll()
        {
            return _producer.Poll();
        }

        public int Poll(TimeSpan timeout, int maxBatchSize)
        {
            return _producer.Poll(timeout, maxBatchSize);
        }

        public int Poll(int millisecondsTimeout, int maxBatchSize)
        {
            return _producer.Poll(millisecondsTimeout, maxBatchSize);
        }

        public int Poll(int maxBatchSize)
        {
            return _producer.Poll(maxBatchSize);
        }

        public void Produce(TopicPartition topicPartition, Message<K, V> message, Action<DeliveryReport<K, V>> deliveryHandler = null)
        {
            _producer.Produce(topicPartition, message, deliveryHandler);
        }

        public void Produce(string topic, Message<K, V> message, Action<DeliveryReport<K, V>> deliveryHandler = null)
        {
            _producer.Produce(topic, message, deliveryHandler);
        }

        public void Produce(Message<K, V> message, Action<DeliveryReport<K, V>> deliveryHandler = null)
        {
            _producer.Produce(message, deliveryHandler);
        }

        public void Produce(IEnumerable<Message<K, V>> messages, Action<DeliveryReport<K, V>> deliveryHandler = null)
        {
            _producer.Produce(messages, deliveryHandler);
        }

        public void Produce(TopicPartition topicPartition, IEnumerable<Message<K, V>> messages, Action<DeliveryReport<K, V>> deliveryHandler = null)
        {
            _producer.Produce(topicPartition, messages, deliveryHandler);
        }

        public void Produce(string topic, IEnumerable<Message<K, V>> messages, Action<DeliveryReport<K, V>> deliveryHandler = null)
        {
            _producer.Produce(topic, messages, deliveryHandler);
        }

        public void Produce(IEnumerable<KeyValuePair<TopicPartition, Message<K, V>>> messages, Action<DeliveryReport<K, V>> deliveryHandler = null)
        {
            _producer.Produce(messages, deliveryHandler);
        }

        public void Produce(IEnumerable<KeyValuePair<string, Message<K, V>>> messages, Action<DeliveryReport<K, V>> deliveryHandler = null)
        {
            _producer.Produce(messages, deliveryHandler);
        }

        public void Produce(IEnumerable<Message<K, V>> messages, Action<DeliveryReport<K, V>> deliveryHandler = null, int? partition = null)
        {
            _producer.Produce(messages, deliveryHandler, partition);
        }

        public void Produce(IEnumerable<KeyValuePair<TopicPartition, Message<K, V>>> messages, Action<DeliveryReport<K, V>> deliveryHandler = null, int? partition = null)
        {
            _producer.Produce(messages, deliveryHandler, partition);
        }

        public void Produce(IEnumerable<KeyValuePair<string, Message<K, V>>> messages, Action<DeliveryReport<K, V>> deliveryHandler = null, int? partition = null)
        {
            _producer.Produce(messages, deliveryHandler, partition);
        }

        public void Produce(TopicPartition topicPartition, IEnumerable<Message<K, V>> messages, int? partition = null, Action<DeliveryReport<K, V>> deliveryHandler = null)
        {
            _producer.Produce(topicPartition, messages, partition, deliveryHandler);
        }

        public void Produce(string topic, IEnumerable<Message<K, V>> messages, int? partition = null, Action<DeliveryReport<K, V>> deliveryHandler = null)
        {
            _producer.Produce(topic, messages, partition, deliveryHandler);
        }

        public void Produce(IEnumerable<Message<K, V>> messages, int? partition = null, Action<DeliveryReport<K, V>> deliveryHandler = null)
        {
            _producer.Produce(messages, partition, deliveryHandler);
        }

        public void Produce(IEnumerable<KeyValuePair<TopicPartition, Message<K, V>>> messages, int? partition = null, Action<DeliveryReport<K, V>> deliveryHandler = null)
        {
            _producer.Produce(messages, partition, deliveryHandler);
        }

        public void Produce(IEnumerable<KeyValuePair<string, Message<K, V>>> messages, int? partition = null, Action<DeliveryReport<K, V>> deliveryHandler = null)
        {
            _producer.Produce(messages, partition, deliveryHandler);
        }

        public void Produce(TopicPartition topicPartition, Message<K, V> message, int? partition = null, Action<DeliveryReport<K, V>> deliveryHandler = null)
        {
            _producer.Produce(topicPartition, message, partition, deliveryHandler);
        }

        public void Produce(string topic, Message<K, V> message, int? partition = null, Action<DeliveryReport<K, V>> deliveryHandler = null)
        {
            _producer.Produce(topic, message, partition, deliveryHandler);
        }

        public void Produce(Message<K, V> message