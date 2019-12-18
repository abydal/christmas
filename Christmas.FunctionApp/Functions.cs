using Microsoft.Azure.ServiceBus.Core;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace DataImport.FunctionApp
{
    public static class ServiceBusQueueTrigger
    {
        [FunctionName("BlobStorageTrigger")]
        public static async Task FileReaderFunction([BlobTrigger("customerimport/{fileName}", Connection = "AzureWebJobsStorage")]Stream file, string fileName, 
            [ServiceBus("CustomerImport", Connection = "AzureServiceBusConnection")] ICollector<string> queue, ILogger log)
        {
            log.LogInformation($"Recieved new customer file {fileName}");

            var rows = BusinessLogic.ReadCustomerCsvFile(file);

            var sendTasks = rows.Select(async t => await t.ContinueWith(async s => queue.Add(await s)));

            Task.WaitAll(sendTasks.ToArray());
        }


        [FunctionName("ServiceBusQueueTrigger")]
        public static async Task ProcessCustomer([ServiceBusTrigger("CustomerImport", Connection = "AzureServiceBusConnection")] string customerDataRaw,
            MessageReceiver messageReceiver,
            string lockToken,
            ILogger log)
        {
            Customer customer;

            try
            {
                customer = BusinessLogic.Process(customerDataRaw);
                log.LogInformation($"Processed customer {customer.FirstName} {customer.LastName}");
            }
            catch (Exception e)
            {
                await messageReceiver.DeadLetterAsync(lockToken, $"Exception occurred during processing of customer: {e.ToString()}");
                return;
            }

            BusinessLogic.PersistToDataModels(customer);
        }
    }


    public class Customer
    {
        public Guid Id { get; set; }
        public int CustomerNumber { get; set; }
        public string FirstName { get; set; }
        public string LastName { get; set; }
        public string Email { get; set; }
        public string Gender { get; set; }
        public string SSN { get; set; }
        public string Country { get; set; }
        public string City { get; set; }
        public string PostalCode { get; set; }
        public string Address { get; set; }
    }

    static class BusinessLogic
    {
        public static List<Task<string>> ReadCustomerCsvFile(Stream stream)
        {
            StreamReader reader = new StreamReader(stream);
            var lines = new List<Task<string>>();
            while(!reader.EndOfStream)
            {
                lines.Add(reader.ReadLineAsync());
            }

            return lines;
        }

        public static Customer Process(string rawCustomerData)
        {
            var splitData = rawCustomerData.Split(',');

            return new Customer
            {
                Id = Guid.NewGuid(),
                CustomerNumber = BusinessLogic.NewCustomerNumber(),
                FirstName = splitData[1],
                LastName = splitData[2],
                Email = splitData[3],
                Gender = splitData[4],
                SSN = splitData[5],
                Country = splitData[6],
                City = splitData[7],
                PostalCode = splitData[8],
                Address = splitData[9],
            };
        }

        public static void PersistToDataModels(Customer customer)
        {
            return;
        }

        private static int Counter = 0;
        public static int NewCustomerNumber()
        {
            return Counter++;
        }
    }


}
