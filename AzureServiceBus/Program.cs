using Microsoft.Azure.ServiceBus;
using System;
using System.Text;
using Microsoft.Azure.ServiceBus;
using System.Threading;
using System.Threading.Tasks;


namespace AzureServiceBus
{
    class Program
    {
        static QueueClient queueClient;
        
        static void Main(string[] args)
        {
            AddtoMessageQueue();

            ReadFromQueue();
        }

        private static void ReadFromQueue()
        {
            string sbConnectionString = "Endpoint=sb://sprkservicebus.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=jZ7tAXX+RWChZU+TQN4g29J/QTbB48xRVAY0hlAy5lU=;";
            string sbQueueName = "Recharge";

            try
            {
                queueClient = new QueueClient(sbConnectionString, sbQueueName);

                var messageHandlerOptions = new MessageHandlerOptions(ExceptionReceivedHandler)
                {
                    MaxConcurrentCalls = 1,
                    AutoComplete = false
                };
                queueClient.RegisterMessageHandler(ReceiveMessagesAsync, messageHandlerOptions);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
            finally
            {
                Console.ReadKey();
                queueClient.CloseAsync();
            }
        }

        static void AddtoMessageQueue()
        {
            string sbConnectionString = "Endpoint=sb://sprkservicebus.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=jZ7tAXX+RWChZU+TQN4g29J/QTbB48xRVAY0hlAy5lU=;";
            string sbQueueName = "Recharge";

            string messageBody = string.Empty;
            try
            {
                Console.WriteLine("-------------------------------------------------------");
                Console.WriteLine("Mobile Recharge");
                Console.WriteLine("-------------------------------------------------------");
                Console.WriteLine("Operators");
                Console.WriteLine("1. Vodafone");
                Console.WriteLine("2. Airtel");
                Console.WriteLine("3. JIO");
                Console.WriteLine("-------------------------------------------------------");

                Console.WriteLine("Operator:");
                string mobileOperator = Console.ReadLine();
                Console.WriteLine("Amount:");
                string amount = Console.ReadLine();
                Console.WriteLine("Mobile:");
                string mobile = Console.ReadLine();

                Console.WriteLine("-------------------------------------------------------");

                switch (mobileOperator)
                {
                    case "1":
                        mobileOperator = "Vodafone";
                        break;
                    case "2":
                        mobileOperator = "Airtel";
                        break;
                    case "3":
                        mobileOperator = "JIO";
                        break;
                    default:
                        break;
                }

                messageBody = mobileOperator + "*" + mobile + "*" + amount;

                queueClient = new QueueClient(sbConnectionString, sbQueueName);

                var message = new Message(Encoding.UTF8.GetBytes(messageBody));
                Console.WriteLine($"Message Added in Queue: {messageBody}");

                //ADD MESSAGE IN THE QUEUE
                queueClient.SendAsync(message);

                //SCHEDULE MESSAGE (AFTER ONE MINUTE) TO ADD IN THE QUEUE
                //DateTimeOffset scheduleTime = DateTime.UtcNow.AddMinutes(1);
                //queueClient.ScheduleMessageAsync(message, scheduleTime);

            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
            finally
            {
                Console.ReadKey();
                queueClient.CloseAsync();
            }
        }

        static async Task ReceiveMessagesAsync(Message message, CancellationToken token)
        {
            try
            {
                Console.WriteLine($"Received message: {Encoding.UTF8.GetString(message.Body)}");

                //UNCOMMENT BELOW CODE TO GENERATE EXCEPTION, SO THAT MESSSAGE WILL BE ADDED IN DEAD LETTER QUEUE
                //int i = 0;
                //i = i / Convert.ToInt32(message);

                await queueClient.CompleteAsync(message.SystemProperties.LockToken);
            }
            catch (Exception ex)
            {
                await queueClient.AbandonAsync(message.SystemProperties.LockToken);
            }
        }


        static Task ExceptionReceivedHandler(ExceptionReceivedEventArgs exceptionReceivedEventArgs)
        {
            Console.WriteLine(exceptionReceivedEventArgs.Exception);
            return Task.CompletedTask;
        }
    }
}
