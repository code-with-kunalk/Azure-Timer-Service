using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using System.Diagnostics;
using System.Reflection;

using System.Xml;
using System.Xml.Linq;
using AzureTimerService.Helper;

namespace AzureTimerService.Logging
{
    public class LoggingService
    {
        private TableStorageHelper<LogEntry> _loggingRepository;

        private string _tableName;
        private string _defaultGuid;

        /// <summary>
        /// Name of the Table.
        /// </summary>
        public string TableName
        {
            get
            {
                return String.IsNullOrEmpty(_tableName) ? "z" + DateTime.UtcNow.Date.ToString("yyyyMMdd") : _tableName;
            }
            set
            {
                _tableName = String.IsNullOrEmpty(value) ? "z" + DateTime.UtcNow.Date.ToString("yyyyMMdd") : value;
                _loggingRepository.TableName = _tableName;
            }
        }

        private string _sourceClassName;
        /// <summary>
        /// Name of the Source Class
        /// </summary>
        public string SourceClassName
        {
            get { return _sourceClassName; }
            set
            {
                if (!String.IsNullOrEmpty(value))
                    _sourceClassName = value;
            }
        }

        public LoggingService()
        {
            _loggingRepository = new TableStorageHelper<LogEntry>();
        }

        public bool Log(string methodName, LogLevel logLevel, string partitionKeyGuid, string messsage, string parameters)
        {
            try
            {
                if (logLevel == LogLevel.Critical)
                {
                    //send Email
                }
                _loggingRepository.Insert(new LogEntry()
                {
                    PartitionKey = partitionKeyGuid == Guid.Empty.ToString() ? _defaultGuid : partitionKeyGuid,
                    RowKey = (DateTime.MaxValue.Ticks - DateTime.UtcNow.Ticks).ToString("d19"),
                    SourceMethod = methodName,
                    LogLevel = (int)logLevel,
                    SourceClass = SourceClassName,
                    Message = messsage,
                    Parameters = parameters
                });
                return true;
            }
            catch (Exception)
            {
                return false;
            }
        }

        public bool DeleteLogs(string partitionKey)
        {
            return _loggingRepository.Delete(partitionKey);
        }

        public void LogException(string methodName, LogLevel logLevel, string partitionKeyGuid, Exception exception, string parameters)
        {
            exception = GetException(exception);
            if (logLevel == LogLevel.Critical)
            {
                //send mail
            }
            _loggingRepository.Insert(new LogEntry()
            {
                PartitionKey = partitionKeyGuid == Guid.Empty.ToString() ? _defaultGuid : partitionKeyGuid,
                RowKey = (DateTime.MaxValue.Ticks - DateTime.UtcNow.Ticks).ToString("d19"),
                SourceMethod = methodName,
                LogLevel = (int)logLevel,
                SourceClass = SourceClassName,
                Message = String.Format("Message: {0}\nStack Trace: {1}", exception.Message, exception.StackTrace),
                Parameters = parameters
            });
        }

        private Exception GetException(Exception exception)
        {
            if (exception.InnerException != null)
            {
                exception = GetException(exception.InnerException);
            }
            return exception;
        }

        public List<LogEntry> RetrieveAll()
        {
            return _loggingRepository.GetAll().ToList<LogEntry>();
        }

        public List<LogEntry> RetrieveAllPaginated(int start, int take)
        {
            return _loggingRepository.GetAllPaginated(start, take).ToList<LogEntry>();
        }

        public List<LogEntry> RetrievePartition(string partitionKey)
        {
            return _loggingRepository.GetList(partitionKey).ToList<LogEntry>();
        }

        public List<LogEntry> RetrievePartitionPaginated(string partitionKey, int start, int take)
        {
            return _loggingRepository.GetListPaginated(partitionKey, start, take).ToList<LogEntry>();
        }

        public List<string> GetLogTableNames()
        {
            return null;
        }

        public void LogReceivedParameters(string methodName, string partitionKey, object obj)
        {
            try
            {
                StringBuilder sb = new StringBuilder();
                foreach (KeyValuePair<string, object> item in obj.ToDynamic())
                {
                    if (item.Value != null)
                        sb.AppendLine(item.Key + ": " + item.Value.ToString());
                }
                Log(methodName, LogLevel.Verbose, partitionKey,
                    "Logging Received Parameters", sb.ToString());
            }
            catch
            {
                return;
            }
        }
    }
}
