using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Amqp;
using Microsoft.Azure.ServiceBus;

namespace ServiceBusTopicSubscriber
{
   class Program
   {
      static async Task Main(string[] args)
      {
         var conn = "Endpoint=sb://chyaservicebus.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=unOAtl6Ccm9UJEL0tVDgJ0N8vzVdVsDvhfogj60Mfeo=";

         Console.WriteLine("Service Bus Subscription...");

         var tasks = new List<Task>()
                     {
                        Task.Factory.StartNew(()=>StartSubscriberAsync(conn, "T1Subscription1")),
                        Task.Factory.StartNew(()=>StartSubscriberAsync(conn, "T1Subscription2"))
                     };

         Task.WaitAll(tasks.ToArray());

         Console.ReadKey();
      }

      static async Task StartSubscriberAsync(string conn, string subscription)
      {
        var client = new SubscriptionClient(conn, "topic1", subscription);

        //Use RegisterSessionHandler if session enabled, otherwise use client.RegisterMessageHandler
        client.RegisterSessionHandler(MessageHandler, new SessionHandlerOptions(ExceptionHandler)
                                                      {
                                                         //If session is enabled, set  AutoComplete = true
                                                         //Cannot call client.CompleteAsync
                                                         AutoComplete = true
                                                      });
        
         Console.WriteLine($"Start receiving msg on {subscription}...");
         Console.ReadKey();

         await client.CloseAsync();

         Console.WriteLine($"Closing {subscription}...");

      }

      static Task ExceptionHandler(ExceptionReceivedEventArgs ex)
      {
         Console.WriteLine($"Exception: {ex.Exception.Message}");

         return Task.CompletedTask;
      }

      static Task MessageHandler(IMessageSession msgSession, Message msg, CancellationToken token)
      {
         Console.WriteLine($"Msg {msg.SystemProperties.SequenceNumber}: {Encoding.UTF8.GetString(msg.Body)}");

         return Task.CompletedTask;
      }
   }
}
