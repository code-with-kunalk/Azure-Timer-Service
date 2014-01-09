using System;
using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;
using System.Runtime.Serialization;
using System.Configuration;

namespace AzureTimerService.Helper
{
    public class SbQueueStorageHelper<T> where T : ISerializable
    {
        private string _connectionString;
        private NamespaceManager _namespaceManager;
        private QueueClient _queueClient;
        private QueueDescription _queueDescription;
        private string _queueName;

        public SbQueueStorageHelper(string queueName)
        {
            _queueName = queueName;
            ConnectionString = ConfigurationManager.AppSettings["SbQueueConnectionString"];
        }

        public string ConnectionString
        {
            get
            {
                if (String.IsNullOrEmpty(_connectionString))
                    throw new Exception("Connection String is null.");
                return _connectionString;
            }
            set
            {
                if (String.IsNullOrEmpty(value)) return;
                _connectionString = value;
                _namespaceManager = NamespaceManager.CreateFromConnectionString(_connectionString);
                if (_namespaceManager == null) return;
                _queueDescription = InitializeQueueDescription(_queueName);
                if (!_namespaceManager.QueueExists(_queueDescription.Path))
                    _namespaceManager.CreateQueue(_queueDescription.Path);
                _queueClient = QueueClient.CreateFromConnectionString(_connectionString, _queueDescription.Path);
            }
        }

        public bool Enqueue(BrokeredMessage brokeredMessage)
        {
            _queueClient.Send(brokeredMessage);
            return true;
        }

        public BrokeredMessage Dequeue()
        {
            return _queueClient.Receive();
        }

        public bool Release(BrokeredMessage brokeredMessage)
        {
            brokeredMessage.Abandon();
            return true;
        }

        public bool Delete(BrokeredMessage brokeredMessage)
        {
            brokeredMessage.Complete();
            return true;
        }

        private QueueDescription InitializeQueueDescription(string queueName)
        {
            return new QueueDescription(queueName)
            {
                MaxSizeInMegabytes = 5120,
                DefaultMessageTimeToLive = TimeSpan.MaxValue,
                LockDuration = new TimeSpan(0, 2, 0),
                MaxDeliveryCount = Int32.MaxValue
            };
        }
    }
}