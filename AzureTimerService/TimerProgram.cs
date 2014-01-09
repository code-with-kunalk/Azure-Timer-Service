using AzureTimerService.Entity;
using AzureTimerService.Service;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AzureTimerService
{
    public class TimerProgram
    {
        private static TimerServiceDemo _timerServiceDemo;

        public static void Main(string[] args)
        {
            _timerServiceDemo = new TimerServiceDemo();
            _timerServiceDemo.SchehduleTimerJob(new MessageTobeProcessed()
            {
                 Id = 1,
                 Name = "Kunal Kapoor"
            }, DateTime.UtcNow.AddMinutes(2), RecurrenceType.OneTime);
        }
    }
}
