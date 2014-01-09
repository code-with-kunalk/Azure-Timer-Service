using System.IO;
using System.Runtime.Serialization.Formatters.Binary;

namespace AzureTimerService.Helper
{
    public class Serializer
    {
        public static byte[] SerializeObject(object toSerialize)
        {
            using (var stream = new MemoryStream())
            {
                var formatter = new BinaryFormatter();
                formatter.Serialize(stream, toSerialize);

                if (stream.Length != 0)
                    return stream.ToArray();
            }
            return null;
        }

        public static object DeserializeObject(byte[] byteArray)
        {
            var memoryStream = new MemoryStream(byteArray);
            var binaryFormatter = new BinaryFormatter();
            memoryStream.Position = 0;
            return binaryFormatter.Deserialize(memoryStream);
        }
    }
}