using Serilog;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace ksicConsoleBot
{
    class Program
    {
        public static IConfigurationRoot configuration;

        static int Main(string[] args)
        {
            // Initialize serilog logger
            Log.Logger = new LoggerConfiguration()
            .WriteTo.Seq("http://localhost:5341")
            .Enrich.WithProperty("Application", "ksicRacing")
            .Enrich.WithProperty("Project", "ksicConsoleBot")
            .Enrich.FromLogContext()

            .CreateLogger();
            try
            {
                // Start!
                MainAsync(args).Wait();
                return 0;
            }
            catch
            {
                return 1;
            }
        }
        static async Task MainAsync(string[] args)
        {
            // Create service collection
            Log.Information("Creating service collection");
            ServiceCollection serviceCollection = new ServiceCollection();
            ConfigureServices(serviceCollection);

            // Create service provider
            Log.Information("Building service provider");
            IServiceProvider serviceProvider = serviceCollection.BuildServiceProvider();

            try
            {
                Log.Information("Starting service");
                await serviceProvider.GetService<App>().Run();
                Log.Information("Ending service");
            }
            catch (Exception ex)
            {
                Log.Fatal(ex, "Error running service");
                throw ex;
            }
            finally
            {
                Log.CloseAndFlush();
            }
        }
        private static void ConfigureServices(IServiceCollection serviceCollection)
        {
            // Add logging
            serviceCollection.AddSingleton(LoggerFactory.Create(builder =>
            {
                builder
                    .AddSerilog(dispose: true);
            }));

            serviceCollection.AddLogging();

            // Build configuration
            configuration = new ConfigurationBuilder()
                .SetBasePath(Directory.GetParent(AppContext.BaseDirectory).FullName)
                .AddJsonFile("appsettings.json", false)
                .Build();

            // Add access to generic IConfigurationRoot
            serviceCollection.AddSingleton(configuration);

            // Add app
            serviceCollection.AddTransient<App>();
        }
    }
}