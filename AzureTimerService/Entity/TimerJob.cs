using Microsoft.WindowsAzure.StorageClient;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;

namespace AzureTimerService.Entity
{
    public class TimerJob : TableServiceEntity
    {
        private string _serviceName;

        public string ServiceName
        {
            get { return _serviceName; }
            set
            {
                if (!String.IsNullOrEmpty(value))
                {
                    _serviceName = value;
                    base.PartitionKey = _serviceName;
                }
            }
        }


        private string _timerJobId;
        public string TimerJobId
        {
            get
            {
                return _timerJobId;
            }
            set
            {
                if (!String.IsNullOrEmpty(value))
                {
                    _timerJobId = value;
                    base.RowKey = _timerJobId;
                }
            }
        }
        public DateTime ScheduledAppearanceOnInUTC { get; set; }
        public int RecurrenceType { get; set; }
        public bool IsActive { get; set; }
        public bool IsDelivered { get; set; }
        public DateTime LastAppearedOnInUTC { get; set; }
        public string BrokeredMessageId { get; set; }
        public DateTime CreatedOn { get; set; }
        public string JobComments { get; set; }
        public string TimerServiceCaller { get; set; }
        public bool IsProcessed { get; set; }
    }
}
