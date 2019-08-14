using System;
using Microsoft.Azure.EventHubs;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Collections.Generic;
using Newtonsoft.Json;
using MathNet.Numerics.Distributions;

namespace StreamData
{
    class Program
    {
        private static EventHubClient eventHubClient;
        private const string EventHubConnectionString = "Endpoint=sb://eventhhbnamespace.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=T7N2aUpp0T5U9uRj2oX1WIFHFczw=";
        private const string EventHubName = "cncyeventhub";

        static void Main(string[] args)
        {
            MainAsync(args).GetAwaiter().GetResult();
        }

        private static async Task MainAsync(string[] args)
        {
            // Creates an EventHubsConnectionStringBuilder object from the connection string, and sets the EntityPath.
            // Typically, the connection string should have the entity path in it, but this simple scenario
            // uses the connection string from the namespace.
            var connectionStringBuilder = new EventHubsConnectionStringBuilder(EventHubConnectionString)
            {
                EntityPath = EventHubName
            };

            eventHubClient = EventHubClient.CreateFromConnectionString(connectionStringBuilder.ToString());

            Console.Write("Enter The number of Devices: ");
            string numOfDevices = Console.ReadLine();

            await SendMessagesToEventHub(System.Convert.ToInt32(numOfDevices));

            await eventHubClient.CloseAsync();

            Console.WriteLine("Press ENTER to exit.");
            Console.ReadLine();
        }


        // Creates an event hub client and sends 100 messages to the event hub.
        private static async Task SendMessagesToEventHub(int numOfDevices)
        {
            using (StreamReader file = new StreamReader("C:\\2.TelematicsData.csv"))
            {
                int counter = 0;
                Random random = new Random();
                string ln;
                TelematicsMessage telematicsMessage = new TelematicsMessage();
                string message = null;

                double mean = 50;
                double stdDev = 50;


                double meanrpm = 3000;
                double stdDevrpm = 1000;

                MathNet.Numerics.Distributions.Normal normalDistspeed = new Normal(mean, stdDev);
                //int randomGaussianValue = Convert.ToInt32(normalDistspeed.Sample());
                MathNet.Numerics.Distributions.Normal normalDistrpm = new Normal(meanrpm, stdDevrpm);

                while ((ln = file.ReadLine()) != null)
                {
                    var values = ln.Split(',');
                    telematicsMessage.TimeStamp = DateTime.UtcNow.ToString();
                    telematicsMessage.DeviceID = "Device_"+random.Next(1, numOfDevices);
                    telematicsMessage.gps_speed =values[4];//Convert.ToInt32(normalDistspeed.Sample()).ToString()
                    telematicsMessage.battery = values[5];
                    telematicsMessage.cTemp = values[6];
                    telematicsMessage.dtc = values[7];
                    telematicsMessage.eLoad = values[8];
                    telematicsMessage.iat = values[9];
                    telematicsMessage.imap = values[10];
                    telematicsMessage.kpl = values[11];
                    telematicsMessage.maf = values[12];
                    telematicsMessage.rpm = values[13];//Convert.ToInt32(normalDistrpm.Sample()).ToString(); ;
                    telematicsMessage.gps_speed = values[14];
                    telematicsMessage.speed = values[15];
                    telematicsMessage.tAdv = values[16];
                    message = JsonConvert.SerializeObject(telematicsMessage);

                    try
                    {
                        await eventHubClient.SendAsync(new EventData(Encoding.UTF8.GetBytes(message)));
                    }
                    catch (Exception exception)
                    {
                        Console.WriteLine($"{DateTime.Now} > Exception: {exception.Message}");
                    }
                    counter++;
                    Console.WriteLine("Number of Messages Sent: " + counter);
                    await Task.Delay(100);

                }
            }
        }

    }

    public class TelematicsMessage
    {
        public string DeviceID { get; set; }
        public string TimeStamp { get; set; }
        public string gps_speed { get; set; }
        public string battery { get; set; }
        public string cTemp { get; set; }
        public string dtc { get; set; }
        public string eLoad { get; set; }
        public string iat { get; set; }
        public string imap { get; set; }
        public string kpl { get; set; }
        public string maf { get; set; }
        public string rpm { get; set; }
        public string speed { get; set; }
        public string tAdv { get; set; }
    }
}
