using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;

namespace SantasFunctions
{
    public static class ServiceBusQueueTrigger
    {
        [FunctionName("BlobStorageTrigger")]
        [return: ServiceBus("ServiceBusQueue", Connection = "AzureWebJobsStorage")]
        public static async Task<string> ListOfWishesRecieverFunction([BlobTrigger("WishLists/{name}", Connection = "AzureWebJobsStorage")]Stream wishList, string name, ILogger log)
        {
            log.LogInformation($"Recieved list of wishes from {name}");

            return await SantaMagic.Read(wishList);
        }


        [FunctionName("ServiceBusQueueTrigger")]
        public static void ProcessWishes([ServiceBusTrigger("WishListQueue", Connection = "AzureServiceBusConnection")] WishList wishList, ILogger log)
        {
            log.LogInformation($"Processing wishes from {wishList.Name}");

            var orders = SantaMagic.Process(wishList);

            SantaMagic.SendToWorkShop(orders);
        }
    }


    public class WishList
    {
        public string Name { get; set; }
        public string Country { get; set; }
        public string Town { get; set; }
        public List<string> Wishes { get; set; }
    }

    class WorkShopOrders {
    }

    static class SantaMagic
    {
        public static async Task<string> Read(Stream stream)
        {    
            StreamReader reader = new StreamReader(stream);
            return await reader.ReadToEndAsync().ConfigureAwait(false);
        }

        public static WorkShopOrders Process(WishList wishList)
        {
            return new WorkShopOrders();
        }

        public static void SendToWorkShop(WorkShopOrders orders){
            
        }
    }


}
