using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.WindowsAzure.StorageClient;

namespace AzureTimerService.Logging
{
    public class LogEntry : TableServiceEntity
    {
        public LogEntry()
        {

        }

        public LogEntry(string partitionKey, string rowKey)
            : base(partitionKey, rowKey)
        {
            var partitionKeyGuid = Guid.Empty;
            base.PartitionKey = Guid.TryParse(partitionKey, out partitionKeyGuid) ? partitionKeyGuid.ToString() : Guid.Empty.ToString();
            base.RowKey = (DateTime.MaxValue.Ticks - DateTime.UtcNow.Ticks).ToString("d19");
        }

        public string Message { get; set; }
        public string Parameters { get; set; }
        public string SourceClass { get; set; }
        public string SourceMethod { get; set; }
        public int LogLevel { get; set; }

    }

    public enum LogLevel
    {
        Verbose = 1,
        Error,
        Critical
    }
}
