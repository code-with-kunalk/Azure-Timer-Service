using AzureTimerService.Entity;
using AzureTimerService.Logging;
using Microsoft.ServiceBus.Messaging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;

namespace AzureTimerService.Helper
{
    public class TimerJobManager<T> where T : ISerializable
    {
        private SbQueueStorageHelper<TimerJobMessage<T>> _sbQueueRepository;
        private TableStorageHelper<TimerJob> _timerJobRepository;
        private LoggingService _loggingService;

        public TimerJobManager()
        {
            _loggingService = new LoggingService();
            _loggingService.SourceClassName = "TimerManager";
            _sbQueueRepository = new SbQueueStorageHelper<TimerJobMessage<T>>("scheduledjobs");
            _timerJobRepository = new TableStorageHelper<TimerJob>();
            _timerJobRepository.TableName = "TimerJob";

        }

        public string EnqueueTimerJob(TimerJobMessage<T> timerJobMessage)
        {
            try
            {
                if (null == timerJobMessage) return String.Empty;
                var messageId = Guid.NewGuid().ToString();
                var brokeredMessage = new BrokeredMessage(Serializer.SerializeObject(timerJobMessage))
                {
                    MessageId = messageId,
                    TimeToLive = TimeSpan.MaxValue,
                    ScheduledEnqueueTimeUtc = timerJobMessage.ScheduledAppearanceOnInUTC.ToUniversalTime()
                };
                if (_sbQueueRepository.Enqueue(brokeredMessage))
                    return messageId;
            }
            catch (Exception ex)
            {
                _loggingService.LogException("TimerManager.AddTimerJob", LogLevel.Error, timerJobMessage.TimerJobId, ex, String.Format("ServiceName: {0}", timerJobMessage.ServiceName));
            }
            return String.Empty;
        }

        public bool InsertTimerJobEntity(TimerJobMessage<T> taskMessage, string messageId)
        {
            try
            {
                if (null == taskMessage) return false;
                TimerJob timerJobInstance = null;
                timerJobInstance = _timerJobRepository.Get(taskMessage.ServiceName, taskMessage.TimerJobId);
                if (null == timerJobInstance)
                {
                    timerJobInstance = new TimerJob();
                    timerJobInstance.ServiceName = taskMessage.ServiceName;
                    timerJobInstance.TimerJobId = taskMessage.TimerJobId;
                    timerJobInstance.CreatedOn = DateTime.UtcNow;
                    timerJobInstance.IsDelivered = false;
                    timerJobInstance.LastAppearedOnInUTC = DateTime.MinValue;
                }
                else
                    timerJobInstance.IsDelivered = true;
                timerJobInstance.BrokeredMessageId = messageId;
                timerJobInstance.IsActive = true;
                timerJobInstance.RecurrenceType = taskMessage.RecurrenceType;
                timerJobInstance.ScheduledAppearanceOnInUTC = taskMessage.ScheduledAppearanceOnInUTC;
                _timerJobRepository.Upsert(timerJobInstance.PartitionKey, timerJobInstance.RowKey, timerJobInstance);
                return true;
            }
            catch (Exception ex)
            {
                _loggingService.LogException("TimerManager.InsertTimerJob", LogLevel.Error, taskMessage.TimerJobId, ex, String.Format("ServiceName: {0}", taskMessage.ServiceName));
            }
            return false;
        }

        public BrokeredMessage RetrieveBrokeredMessage()
        {
            try
            {
                var brokeredMessage = _sbQueueRepository.Dequeue();
                return brokeredMessage;
            }
            catch (Exception ex)
            {
                _loggingService.LogException("TimerManager.RetrieveBrokeredMessage", LogLevel.Error, Guid.Empty.ToString(), ex, "");
            }
            return null;
        }

        public bool DeleteBrokeredMessage(BrokeredMessage brokeredMessage)
        {
            try
            {
                return _sbQueueRepository.Delete(brokeredMessage);
            }
            catch (Exception ex)
            {
                _loggingService.LogException("TimerManager.DeleteBrokeredMessage", LogLevel.Error, Guid.Empty.ToString(), ex, "");
            }
            return false;
        }

        public bool ReleaseBrokeredMessage(BrokeredMessage brokeredMessage)
        {
            try
            {
                return _sbQueueRepository.Release(brokeredMessage);
            }
            catch (Exception ex)
            {
                _loggingService.LogException("TimerManager.DeleteBrokeredMessage", LogLevel.Error, Guid.Empty.ToString(), ex, "");
            }
            return false;
        }

        public bool VerifyJob(TimerJobMessage<T> timerJobMessage)
        {
            if (null == timerJobMessage) return false;
            var timerJob = _timerJobRepository.Get(timerJobMessage.ServiceName, timerJobMessage.TimerJobId);
            if (null != timerJob)
                return timerJob.IsActive;
            return false;
        }

        public bool CancelTimerJob(string serviceName, string timerJobId)
        {
            try
            {
                var timerJob = _timerJobRepository.Get(serviceName, timerJobId);
                if (null != timerJob)
                {
                    timerJob.IsActive = false;
                    _timerJobRepository.Update(timerJob.PartitionKey, timerJob.RowKey, timerJob);
                    return true;
                }
            }
            catch (Exception ex)
            {
                LogException("TimerJobManager.CancelTimerJob", timerJobId, ex, String.Format("ServiceName: {0}", serviceName));
            }
            return false;
        }

        public bool MarkTimerJobProcessStatus(string serviceName, string timerJobId, bool isDelivered, bool isProcessed, string comments = "", DateTime lastAppearedOn = default(DateTime))
        {
            try
            {
                var timerJob = _timerJobRepository.Get(serviceName, timerJobId);
                if (null == timerJob) return false;
                timerJob.IsDelivered = isDelivered;
                timerJob.IsProcessed = isProcessed;
                if (!String.IsNullOrEmpty(comments)) timerJob.JobComments += comments + Environment.NewLine;
                if (lastAppearedOn != default(DateTime)) timerJob.LastAppearedOnInUTC = lastAppearedOn;
                _timerJobRepository.Update(timerJob.PartitionKey, timerJob.RowKey, timerJob);
                return true;
            }
            catch (Exception ex)
            {
                LogException("MarkTimerJobCompletion", timerJobId, ex, String.Format("ServiceName: {0}", serviceName));
            }
            return false;
        }

        public TimerJob RetrieveTimerJobDetails(string serviceName, string timerJobId)
        {
            try
            {
                return _timerJobRepository.Get(serviceName, timerJobId);
            }
            catch (Exception ex)
            {
                LogException("RetrieveTimerJobDetails", timerJobId, ex,
                    String.Format("ServiceName: {0}", serviceName));
            }
            return null;
        }

        public List<TimerJob> RetrieveAllTimerJobs(string serviceName)
        {
            try
            {
                return _timerJobRepository.GetList(serviceName);
            }
            catch (Exception ex)
            {
                LogException("RetrieveTimerJobDetails", serviceName, ex,
                    String.Format("ServiceName: {0}", serviceName));
            }
            return null;
        }

        #region Logging

        private void Log(string methodName, string loggingId, string message, string parameters)
        {
            _loggingService.TableName = "TimerJobLogs";
            _loggingService.Log(methodName, LogLevel.Verbose, loggingId, message, parameters);
            _loggingService.TableName = String.Empty;
        }

        private void LogException(string methodName, string loggingId, Exception ex, string parameters)
        {
            _loggingService.TableName = "TimerJobLogs";
            _loggingService.LogException(methodName, LogLevel.Error, loggingId, ex, parameters);
            _loggingService.TableName = String.Empty;
        }

        #endregion
    }
}
