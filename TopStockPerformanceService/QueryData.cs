using Microsoft.Azure.Cosmos.Table;
using Microsoft.Azure.WebJobs;
using System;
using System.Linq;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using System.IO;
using Newtonsoft.Json;
using Microsoft.AspNetCore.Mvc;
using System.Threading.Tasks;
using System.Collections.Generic;

namespace TopStockPerformance
{

    class PerformanceRequestData
    {
        public string Kpi { get; set; }
        public DateTime StartDate { get; set; }
        public DateTime EndDate { get; set; }
    }

    class PerformanceResponseData
    {
        public PerformanceResponseData()
        {
            companiesKPI = new List<KPIInfo>();
        }
        public List<KPIInfo> companiesKPI { get; set; }
    }

    public class KPIInfo
    {
        public KPIInfo(int rank, string companyAbriv, string companyName, double value)
        {
            this.rank = rank;
            this.companyAbriv = companyAbriv;
            this.companyName = companyName;
            this.value = value;
        }
        public int rank { get; set; }
        public string companyAbriv { get; set; }
        public string companyName { get; set; }
        public double value { get; set; }
    }

    class StockInfoEntity : Microsoft.Azure.Cosmos.Table.TableEntity
    {
        public StockInfoEntity(string partition, string companySymbol, DateTime date, double price)
        {
            PartitionKey = partition;
            RowKey = companySymbol;
            Date = date;
            Price = price;
        }
        public StockInfoEntity() { }
        public DateTime Date { get; set; }
        public double Price { get; set; }
    }

    public static class AzureFunction
    {

        private static readonly Dictionary<string, string> companies = new Dictionary<string, string>()
        {
            { "ctcm", "CTC Media Inc" },
            { "dpm", "DCP Midstream Partners LP"},
            { "dx", "Dynex Capital Inc"},
            { "lit","Global X Funds"},
            { "npo", "Enpro Industries Inc" },
            { "nymx", "Nymox Pharmaceutical Corp"},
            { "pmm", "Putnam Managed Muni Income TR" },
            { "sbra", "Sabra Healthcare Reit Inc" },
            { "smrt", "Stein Mart Inc" },
            { "ttt", "Proshares Trust"  }
        };

        [FunctionName("QueryData")]
        public static async Task<IActionResult> Run([HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = null)] HttpRequest req)
        {

            //Get request data values
            var content = await new StreamReader(req.Body).ReadToEndAsync();
            PerformanceRequestData data = JsonConvert.DeserializeObject<PerformanceRequestData>(content);
            string kpi = data.Kpi;
            DateTime startDate = data.StartDate;
            DateTime endDate = data.EndDate;

            //Get database references to client and table
            var client = GetCloudClient();
            var table = client.GetTableReference("stocksinfo");

            //Prepare response to send back
            PerformanceResponseData newResponse = new PerformanceResponseData();

            //Query database for the specified kpi within the specified time period
            List<StockInfoEntity> kpiList = table.CreateQuery<StockInfoEntity>()
            .Where(x => x.PartitionKey == kpi && x.Date >= startDate && x.Date <= endDate)
            .Select(x => new StockInfoEntity() { PartitionKey = x.PartitionKey, RowKey = x.RowKey, Price = x.Price })
            .ToList();

            //Order by ascending if kpi wants lowest values first, order by descending if kpi wants highest values first
            if (kpi == "low" || kpi == "price to earn ratio")
            {
                kpiList = kpiList.OrderBy(x => x.Price).ToList();
            }
            else
            {
                kpiList = kpiList.OrderByDescending(x => x.Price).ToList();
            }

            //Go through query results and find the correct KPI values and create KPIInfo objects to hold the values
            int rank = 1;
            Dictionary<string, string> addedCompanies = new Dictionary<string, string>();
            foreach (var tick in kpiList)
            {
                //If rank > 10, then the top 10 companies have already been found, so return the 10 companies
                if (rank > 10)
                    break;

                //Check if company already has a KPI entry
                string output;

                //If the company has not been added, then make a new entry
                //Additional company entries are skipped because the list is in the correct order, so companies with multiple entries will have
                //the correct entry show up first.
                if (!addedCompanies.TryGetValue(tick.RowKey, out output))
                {
                    //Get values to compile into KPIInfo object
                    string companyAbriv = tick.RowKey;
                    string companyName = companies[companyAbriv];
                    double value = tick.Price;
                    KPIInfo company = new KPIInfo(rank, companyAbriv, companyName, value);
                    newResponse.companiesKPI.Add(company);
                    rank++;
                }
            }

            return new OkObjectResult(newResponse);

        }


        public static Microsoft.Azure.Cosmos.Table.CloudTableClient GetCloudClient()
        {
            string connectionString = "DefaultEndpointsProtocol=https;AccountName=storageaccount4471b6a6;AccountKey=dX2VUCuxC0EcRnyZ7Srg+XIKLLagAO30kkpcBcsv9bq91rG+h2FomX6EHP/IByNzKSxVdRIqq6phUYDQ3PAPnw==;EndpointSuffix=core.windows.net";
            var storageAccount = CloudStorageAccount.Parse(connectionString);
            CloudTableClient tableClient = storageAccount.CreateCloudTableClient(new TableClientConfiguration());
            return tableClient;
        }
    }
}