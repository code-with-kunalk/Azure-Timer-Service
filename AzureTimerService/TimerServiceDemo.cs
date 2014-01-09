using AzureTimerService.Entity;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AzureTimerService
{
    public class TimerServiceDemo : AzureTimerService.Service.TimerJobService<MessageTobeProcessed>
    {
        private const string FileName = "demoTextFile";
        private const string ServiceName = "DemoService";

        public TimerServiceDemo()
        {
            //Starts the Timer Service automatically once the timer job is scheduled.
            base.IsServiceStartupTypeAutomatic = true;
        }

        /// <summary>
        /// Schedules a timer job.
        /// </summary>
        /// <param name="message">Custom Object that will be delivered at the scheduled time.</param>
        /// <param name="scheduledAppearanceInUtc">The time in UTC when timer job gets delivered.</param>
        /// <param name="recurrenceType">Type of occurence of the timer job.</param>
        /// <param name="expiresOn">When do the timer job expires. Never if left blank.</param>
        /// <returns>Returns true if job is scheduled successfully.</returns>
        public bool SchehduleTimerJob(MessageTobeProcessed message, DateTime scheduledAppearanceInUtc, RecurrenceType recurrenceType, DateTime expiresOn = default(DateTime))
        {
            try
            {
                var timerJobId = base.CreateTimerJob(ServiceName, message, scheduledAppearanceInUtc, recurrenceType, expiresOn);
                if (!String.IsNullOrEmpty(timerJobId))
                {
                    using (StreamWriter outfile = new StreamWriter(FileName))
                    {
                        var str = String.Format("Timer Job Id: {0}\nId: {1}\nName: {2}\nEstimated Time of Arrival in UTC: {3}", timerJobId, message.Id, message.Name, scheduledAppearanceInUtc);
                        outfile.Write(str);
                        return true;
                    }
                }
            }
            catch
            {
            }
            return false;
        }

        /// <summary>
        /// This is the method that gets invoked at the scheduled time. 
        /// </summary>
        /// <param name="customObject">The object that is passed to the timer job. It is delivered at the scheduled time.</param>
        /// <returns>Returns true if the object is processed as  supposed to be.</returns>
        protected override bool ProcessTimerJob(MessageTobeProcessed customObject)
        {
            try
            {
                using (StreamWriter outfile = new StreamWriter("StorageSummary.txt"))
                {
                    var str = String.Format("Id: {0}\nName: {1}", customObject.Id, customObject.Name);
                    outfile.Write(str);
                }
                return true;
            }
            catch
            {
            }
            return false;
        }

        /// <summary>
        /// Cancels the timer job.
        /// </summary>
        /// <param name="timerJobId">Timer job Id to identify the timer job.</param>
        /// <returns>Returns true if the job is cancelled successfully.</returns>
        public bool CancelTimerJob(string timerJobId)
        {
            try
            {
                return base.CancelTimerJob(ServiceName, timerJobId);
            }
            catch
            {
            }
            return false;
        }

        /// <summary>
        /// Retrieves the timer job details.
        /// </summary>
        /// <param name="timerJobId">Id of the timer job whose details are to be retrieved.</param>
        /// <returns>Returns details of the timer job in XML format.</returns>
        public string RetrieveTimerJobDetails(string timerJobId)
        {
            try
            {
                return base.RetrieveTimerJobDetails(ServiceName, timerJobId);
            }
            catch
            {
            }
            return String.Empty;
        }
    }
}
