using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;

namespace AzureTimerService.Entity
{
    [Serializable]
    public class TimerJobMessage<T> : ISerializable where T : ISerializable
    {
        public string ServiceName { get; set; }
        public string TimerJobId { get; set; }
        public T CustomObject { get; set; }
        public DateTime ScheduledAppearanceOnInUTC { get; set; }
        public int RecurrenceType { get; set; }
        public DateTime ExpiresOn { get; set; }

        public TimerJobMessage()
        {

        }

        public void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            info.AddValue("ServiceName", this.ServiceName, typeof(string));
            info.AddValue("TimerJobId", this.TimerJobId, typeof(string));
            info.AddValue("CustomObject", this.CustomObject, typeof(T));
            info.AddValue("ScheduledAppearanceOnInUTC", this.ScheduledAppearanceOnInUTC, typeof(DateTime));
            info.AddValue("RecurrenceType", this.RecurrenceType, typeof(int));
            info.AddValue("ExpiresOn", this.ExpiresOn, typeof(DateTime));
        }

        public TimerJobMessage(SerializationInfo info, StreamingContext context)
        {
            this.ServiceName = (string)info.GetValue("ServiceName", typeof(string));
            this.TimerJobId = (string)info.GetValue("TimerJobId", typeof(string));
            this.CustomObject = (T)info.GetValue("CustomObject", typeof(T));
            this.ScheduledAppearanceOnInUTC = (DateTime)info.GetValue("ScheduledAppearanceOnInUTC", typeof(DateTime));
            this.RecurrenceType = (int)info.GetValue("RecurrenceType", typeof(int));
            this.ExpiresOn = (DateTime)info.GetValue("ExpiresOn", typeof(DateTime));
        }
    }
}
