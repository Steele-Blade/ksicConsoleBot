using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using BetfairNG.Data;
using System.Text.Json;
using ksicRacing.Core.Models;
using Hangfire;
using Microsoft.EntityFrameworkCore;
using Betfair.ESAClient;
using Betfair.ESAClient.Auth;
using Betfair.ESAClient.Cache;
using Serilog;
using Betfair.ESASwagger.Model;
using CsvHelper;
using System.Globalization;
using Betfair.ESAClient.Protocol;
using Hangfire.SqlServer;

namespace ksicConsoleBot
{
    public class App
    {
        private readonly IConfigurationRoot _config;
        private readonly ILogger<App> _logger;

        public App(IConfigurationRoot config, ILoggerFactory loggerFactory)
        {
            _logger = loggerFactory.CreateLogger<App>();
            _config = config;
        }

        public async Task Run()
        {
            GlobalConfiguration.Configuration
                                            .SetDataCompatibilityLevel(CompatibilityLevel.Version_170)
                                            .UseColouredConsoleLogProvider()
                                            .UseSimpleAssemblyNameTypeSerializer()
                                            .UseRecommendedSerializerSettings()
                                            .UseSqlServerStorage(_config.GetConnectionString("DataConnection"), new SqlServerStorageOptions
                                            {
                                                CommandBatchMaxTimeout = TimeSpan.FromMinutes(5),
                                                SlidingInvisibilityTimeout = TimeSpan.FromMinutes(5),
                                                QueuePollInterval = TimeSpan.Zero,
                                                UseRecommendedIsolationLevel = true
                                            });
            string dt = DateTime.Now.ToString("yyyy-MM-dd");
            List<MarketCatalogue> Markets = new();
            var FileUrl = @$"{_config.GetConnectionString("FileURL")}{dt}_markets.txt";
            //file lines
            string[] lines = File.ReadAllLines(FileUrl);

            //loop through each file line
            foreach (string line in lines)
            {
                List<MarketCatalogue> data = JsonSerializer.Deserialize<List<MarketCatalogue>>(line);
                data.ForEach(item => Markets.Add(item));
            }
            bool bAddJobs = false;
            using SQLServerContext sqldb = new(_config.GetConnectionString("DataConnection"));
            sqldb.ChangeTracker.QueryTrackingBehavior = QueryTrackingBehavior.NoTracking;
            var bj = from job in sqldb.Set<Job>()
                     where job.StateName == "Scheduled"
                     select job;
            if (!bj.Any())
            {
                bAddJobs = true;
            }
            else
            {
                _logger.LogInformation("Already have some jobs scheduled.");
            }
            if (bAddJobs)
            {
                foreach (var market in Markets)
                {
                    TimeSpan ts = market.Description.MarketTime.AddMinutes(-30).ToLocalTime().Subtract(DateTime.Now);
                    if (ts > TimeSpan.Zero)
                    {
                        _ = BackgroundJob.Schedule<SubscribeMarket>(x => SubscribeMarket.Subscribe(market), ts);
                    }
                }
            }
            using var server = new BackgroundJobServer();
            Console.ReadLine();
        }
    }
    public class SubscribeMarket
    {
        public static void Subscribe(MarketCatalogue market)
        {
            //1: Create a session provider
            AppKeyAndSessionProvider sessionProvider = new AppKeyAndSessionProvider(
                AppKeyAndSessionProvider.SSO_HOST_COM,
                "CeaZSpqjSVfDbSr9",
                "ksicracing",
                "malak1960");

            //2: Create a client
            Client client = new Client(
                "stream-api-integration.betfair.com", //NOTE: use production endpoint in prod: stream-api.betfair.com
                443,
                sessionProvider);
            ClientCache cache = new(client);
            //Register for change events
            cache.MarketCache.MarketChanged +=
                (sender, arg) => OnMarketChanged(arg.Snap, cache, market);
            cache.SubscribeMarkets(market.MarketId);
            Log.Logger?.Information($"Subscribed to {market.Event.Venue} {market.MarketName}");
        }
        private static void OnMarketChanged(MarketSnap snap, ClientCache cache, MarketCatalogue market)
        {
            Log.Logger?.Information($"Got OnMarketChanged {snap.MarketDefinition.Venue} {market.MarketName.Split(' ')[0]} {(bool)snap.MarketDefinition.BspReconciled} {(bool)snap.MarketDefinition.InPlay}");
            if ((bool)snap.MarketDefinition.BspReconciled && (bool)snap.MarketDefinition.InPlay)
            {
                Log.Logger?.Information($"{snap.MarketDefinition.Venue} {market.MarketName.Split(' ')[0]} is in play.");
                WriteRecords(snap, market, "_Final");
            }
            else
            {
                TimeSpan ts = snap.MarketDefinition.MarketTime.Value.AddMinutes(-1).ToLocalTime().Subtract(DateTime.Now);
                if (ts > TimeSpan.Zero)
                {
                    _ = BackgroundJob.Schedule<SubscribeMarket>(x => Subscribe(market), ts);
                    Log.Logger?.Information($"Rescheduled {snap.MarketDefinition.Venue} {market.MarketName.Split(' ')[0]} for {ts.TotalSeconds}");
                    WriteRecords(snap, market, "_Open");
                }
                else
                {
                    _ = BackgroundJob.Schedule<SubscribeMarket>(x => Subscribe(market), TimeSpan.FromSeconds(30));
                    Log.Logger?.Information($"Rescheduled {snap.MarketDefinition.Venue} {market.MarketName.Split(' ')[0]} for 30 seconds");
                    WriteRecords(snap, market, string.Empty);
                }
            }
            Console.WriteLine(snap);
            if (cache.Status == ConnectionStatus.SUBSCRIBED)
            {
                Log.Logger?.Information($"Stopped getting {snap.MarketDefinition.Venue}");
                cache.Stop();
            }
        }
        private static void WriteRecords(MarketSnap snap, MarketCatalogue market, string suffix)
        {
            List<RatingsELOHorse> list = new();
            foreach (MarketRunnerSnap runner in snap.MarketRunners.Where(x => x.Definition.Status == RunnerDefinition.StatusEnum.Active))
            {
                var horse = market.Runners.Where((x) => x.SelectionId == runner.RunnerId.SelectionId).SingleOrDefault();
                list.Add(new(runner.Definition, runner.Prices, horse.RunnerName, (bool)snap.MarketDefinition.BspReconciled && (bool)snap.MarketDefinition.InPlay));
            }
            string s = Path.Combine($"C:\\Users\\Ian\\AppData\\Local\\Packages\\ksicRacing_2xbmf2jyx4pdt\\LocalState\\Captured", $"{snap.MarketDefinition.Venue}_{market.MarketName.Split(' ')[0]}_{market.MarketId}_{DateTime.Now:HHmmss}{suffix}.csv");
            using StreamWriter writer = new(s);
            using CsvWriter csvwrite = new(writer, CultureInfo.GetCultureInfo("en-AU"));
            csvwrite.WriteRecords(list);
            csvwrite.Flush();
            csvwrite.Dispose();
        }
    }
}
