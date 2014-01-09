using AzureTimerService.Entity;
using AzureTimerService.Helper;
using AzureTimerService.Logging;
using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Threading;

namespace AzureTimerService.Service
{
    /// <summary>
    /// Timer Job Service allows you to create a scheduler service. You can schedule a task and attach any custom properties that you might want to have to execute the task.
    /// It allows you to create the scheduled task, cancel the scheduled task, retrieve details of your scheduled task in XML format.
    /// </summary>
    /// <typeparam name="T">The custom object that contains the custom properties. The object has to implement ISerializable.</typeparam>
    public abstract class TimerJobService<T> where T : ISerializable
    {
        #region Instance Mapping

        private TimerJobManager<T> _timerJobManager;
        private LoggingService _loggingService;
        private bool _runTimerService;
        private bool _isTimerRunning;
        private int _timerJobSleepTime;

        public int TimerServiceSleepTime
        {
            get { return _timerJobSleepTime; }
            set { _timerJobSleepTime = value; }
        }

        public bool IsTimerRunning
        {
            get { return _isTimerRunning; }
            private set
            {
                _isTimerRunning = value;
                if (!_isTimerRunning && RunTimerService)
                    TimerJob();
            }
        }

        public bool RunTimerService
        {
            get { return _runTimerService; }
            set
            {
                _runTimerService = value;
                if (_runTimerService && !IsTimerRunning)
                {
                    IsTimerRunning = true;
                    TimerJob();
                }
                else if (!_runTimerService)
                    IsTimerRunning = false;
            }
        }

        /// <summary>
        /// Sets or Gets if the timer service is automatic. True by default.
        /// </summary>
        public bool IsServiceStartupTypeAutomatic { get; set; }


        #endregion

        #region Constructor

        public TimerJobService()
        {
            _loggingService = new LoggingService();
            _loggingService.SourceClassName = "TimerJobService";
            _loggingService.TableName = "TimerJobLogs";

            _timerJobManager = new TimerJobManager<T>();
        }

        #endregion

        #region Public Methods

        /// <summary>
        /// Creates a new Timer Job. 
        /// </summary>
        /// <param name="serviceName">Service Name - To support more than one service using the same timer service.</param>
        /// <param name="customObject">The object that gets delivered on the scheduled time. It has to implement ISerializable.</param>
        /// <param name="scheduledAppearanceOnInUTC">The time in UTC when you want to receive the object.</param>
        /// <param name="recurrenceType">Type of occurence of the timer job.</param>
        /// <param name="expiresOn">When do the timer job expires. Never if left blank.</param>
        /// <returns>Returns the timer job Id if created successfully. Empty string otherwise.</returns>
        public string CreateTimerJob(string serviceName, T customObject, DateTime scheduledAppearanceOnInUTC, RecurrenceType recurrenceType, DateTime expiresOn = default(DateTime))
        {
            return CreateTimerJob(serviceName, customObject, scheduledAppearanceOnInUTC, recurrenceType, expiresOn, String.Empty);
        }

        /// <summary>
        /// Cancels the specified timer job.
        /// </summary>
        /// <param name="serviceName">Service Name - To support more than one service using the same timer service.</param>
        /// <param name="timerJobId">Timer job Id to identify the timer job.</param>
        /// <returns>True the job is cancelled successfully.</returns>
        public bool CancelTimerJob(string serviceName, string timerJobId)
        {
            try
            {
                if (String.IsNullOrEmpty(timerJobId)) return false;
                return _timerJobManager.CancelTimerJob(serviceName, timerJobId);
            }
            catch (Exception ex)
            {
                LogException("TimerJobService.CancelTimerJob", timerJobId, ex, "");
            }
            return false;
        }

        /// <summary>
        /// Retrieves Timer Job details. 
        /// </summary>
        /// <param name="serviceName">Service name of the job under which the job is scheduled.</param>
        /// <param name="timerJobId">Id of the timer job whose details are to be retrieved.</param>
        /// <returns>Returns the details of Timer Job(s) in XML format.</returns>
        public string RetrieveTimerJobDetails(string serviceName, string timerJobId = "")
        {
            if (String.IsNullOrEmpty(serviceName)) return "Timer Job could not be null or empty.";

            if (String.IsNullOrEmpty(timerJobId))
            {
                var lstTimerJobDetails = _timerJobManager.RetrieveAllTimerJobs(serviceName);
                if (null != lstTimerJobDetails)
                {
                    var lstObj = new List<object>();
                    foreach (var item in lstTimerJobDetails)
                    {
                        var obj = new
                        {
                            ServiceName = item.ServiceName,
                            TimerJobId = item.TimerJobId,
                            CreatedOn = item.CreatedOn,
                            IsDelivered = item.IsDelivered,
                            IsActive = item.IsActive,
                            EventOccurredOn = item.LastAppearedOnInUTC,
                            IsProcessedOn = item.IsProcessed,
                            Comments = item.JobComments
                        };
                        lstObj.Add(obj);
                    }
                    if (null != lstObj && lstObj.Count > 0)
                        return lstObj.ToXML();
                }
            }
            else
            {
                var timerJob = _timerJobManager.RetrieveTimerJobDetails(serviceName, timerJobId);
                if (null != timerJob)
                {
                    var obj = new
                    {
                        ServiceName = timerJob.ServiceName,
                        TimerJobId = timerJob.TimerJobId,
                        CreatedOn = timerJob.CreatedOn,
                        IsDelivered = timerJob.IsDelivered,
                        IsActive = timerJob.IsActive,
                        EventOccurredOn = timerJob.LastAppearedOnInUTC,
                        IsProcessedOn = timerJob.IsProcessed,
                        Comments = timerJob.JobComments
                    };
                    return obj.ToXML();
                }
            }
            return String.Empty;
        }

        #endregion

        #region Private Members

        private string CreateTimerJob(string serviceName, T customObject, DateTime scheduledAppearanceOnInUTC, RecurrenceType recurrenceType, DateTime expiresOn = default(DateTime), string timerJobId = "")
        {
            if (object.Equals(customObject, default(T))) return "Custom object can't be null.";
            if (String.IsNullOrEmpty(timerJobId))
                timerJobId = Guid.NewGuid().ToString();
            try
            {
                if (expiresOn != default(DateTime) && expiresOn <= DateTime.UtcNow)
                {
                    Log("TimerJobService.CreateTimerJob", timerJobId,
                        "EOT has reached for the timer job",
                        "");
                    LogReceivedParameters("TimerJobService.CreateTimerJob", timerJobId, customObject);
                }
                else
                {
                    var timerJobMessage = new TimerJobMessage<T>();
                    timerJobMessage.ServiceName = serviceName;
                    timerJobMessage.CustomObject = customObject;
                    timerJobMessage.RecurrenceType = (int)recurrenceType;
                    timerJobMessage.ScheduledAppearanceOnInUTC = scheduledAppearanceOnInUTC;
                    timerJobMessage.TimerJobId = timerJobId;
                    timerJobMessage.ExpiresOn = expiresOn == default(DateTime) ? DateTime.MaxValue : expiresOn;
                    var brokeredMessageId = _timerJobManager.EnqueueTimerJob(timerJobMessage);
                    if (!String.IsNullOrEmpty(brokeredMessageId))
                        if (_timerJobManager.InsertTimerJobEntity(timerJobMessage, brokeredMessageId))
                        {
                            if (!IsTimerRunning && IsServiceStartupTypeAutomatic) TimerJob();
                            return timerJobId;
                        }
                }
            }
            catch (Exception ex)
            {
                LogException("TimerJobService.CreateTimerJob", timerJobId, ex, "");
            }
            return String.Empty;
        }

        private void TimerJob()
        {
            try
            {
                while (IsTimerRunning && RunTimerService)
                {
                    Thread.Sleep(TimerServiceSleepTime <= 0 ? 10000 : TimerServiceSleepTime);
                    var brokeredMessage = _timerJobManager.RetrieveBrokeredMessage();
                    if (null != brokeredMessage)
                    {
                        var messageAppearanceTime = DateTime.UtcNow;
                        var messageBody = brokeredMessage.GetBody<byte[]>();
                        var timerJobMessage = Serializer.DeserializeObject(messageBody) as TimerJobMessage<T>;
                        if (null != timerJobMessage)
                        {
                            bool isProcessed = false;
                            if (DateTime.UtcNow < timerJobMessage.ExpiresOn && _timerJobManager.VerifyJob(timerJobMessage))
                            {
                                var customObject = timerJobMessage.CustomObject;
                                if (null != customObject)
                                {
                                    _timerJobManager.DeleteBrokeredMessage(brokeredMessage);
                                    if (timerJobMessage.RecurrenceType != (int)RecurrenceType.OneTime)
                                    {
                                        var nextAppreanceOn = NextAppearanceOn(timerJobMessage);
                                        if (nextAppreanceOn < timerJobMessage.ExpiresOn && nextAppreanceOn != DateTime.MinValue)
                                        {
                                            for (int i = 3; ; )
                                            {
                                                if (String.IsNullOrEmpty(CreateTimerJob(timerJobMessage.ServiceName, customObject, nextAppreanceOn, (RecurrenceType)timerJobMessage.RecurrenceType, timerJobMessage.ExpiresOn, timerJobMessage.TimerJobId)))
                                                {
                                                    i--;
                                                    if (i > 0)
                                                        continue;
                                                    else
                                                        _timerJobManager.MarkTimerJobProcessStatus(timerJobMessage.ServiceName, timerJobMessage.TimerJobId, true, isProcessed,
                                                            "Next Timer could not be scheduled.");
                                                    break;
                                                }
                                                _timerJobManager.MarkTimerJobProcessStatus(timerJobMessage.ServiceName, timerJobMessage.TimerJobId, true, isProcessed,
                                                            String.Format("Next Timer scheduled on {0}.", nextAppreanceOn));
                                                break;
                                            }
                                        }
                                    }
                                    try
                                    {
                                        isProcessed = ProcessTimerJob(customObject);
                                    }
                                    catch
                                    {
                                        isProcessed = false;
                                    }
                                    _timerJobManager.MarkTimerJobProcessStatus(timerJobMessage.ServiceName,timerJobMessage.TimerJobId, true, isProcessed,
                                        String.Format("Timer Job processed on {0} with result as {1}.", DateTime.UtcNow, isProcessed),
                                        messageAppearanceTime);
                                }
                            }
                            else
                            {
                                _timerJobManager.DeleteBrokeredMessage(brokeredMessage);
                                _timerJobManager.MarkTimerJobProcessStatus(timerJobMessage.ServiceName, timerJobMessage.TimerJobId, false, isProcessed,
                                    "Either the task is not active or has reached/exceeded EOT", messageAppearanceTime);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                LogException("TimerJobService.TimerJob", Guid.Empty.ToString(), ex, "");
                IsTimerRunning = false;
            }
        }

        private DateTime NextAppearanceOn(TimerJobMessage<T> timerJobMessage)
        {
            switch (timerJobMessage.RecurrenceType)
            {
                case (int)RecurrenceType.Daily:
                    return timerJobMessage.ScheduledAppearanceOnInUTC.AddDays(1);
                case (int)RecurrenceType.Weekly:
                    return timerJobMessage.ScheduledAppearanceOnInUTC.AddDays(7);
                case (int)RecurrenceType.Monthly:
                    return timerJobMessage.ScheduledAppearanceOnInUTC.AddMonths(1);
            }
            return DateTime.MinValue;
        }

        #endregion

        protected abstract bool ProcessTimerJob(T customObject);

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

        private void LogReceivedParameters(string methodName, string loggingId, object obj)
        {
            _loggingService.TableName = "TimerJobLogs";
            _loggingService.LogReceivedParameters(methodName, loggingId, obj);
            _loggingService.TableName = String.Empty;
        }

        #endregion

    }
}
