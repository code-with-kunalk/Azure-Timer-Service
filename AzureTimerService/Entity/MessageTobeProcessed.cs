using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;

namespace AzureTimerService.Entity
{
    [Serializable]
    public class MessageTobeProcessed : ISerializable
    {
        public string Name { get; set; }
        public int Id { get; set; }

        public void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            info.AddValue("Name", this.Name, typeof(string));
            info.AddValue("Id", this.Id, typeof(int));
        }

        public MessageTobeProcessed()
        {

        }

        public MessageTobeProcessed(SerializationInfo info, StreamingContext context)
        {
            this.Name = (string)info.GetValue("Name", typeof(string));
            this.Id = (int)info.GetValue("Id", typeof(int));
        }
    }
}
