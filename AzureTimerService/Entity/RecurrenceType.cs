using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace AzureTimerService.Entity
{
    public enum RecurrenceType
    {
        Unknown = 0,
        OneTime = 1,
        Daily = 2,
        Weekly = 3,
        Monthly = 4
    }
}
