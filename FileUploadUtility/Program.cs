using System.Collections.Concurrent;
using Microsoft.Extensions.Configuration;

namespace FileUploadUtility;

public static class Program
{
    public static async Task Main(string[] args)
    {
        // Load app settings
        var configuration = GetConfiguration();

        var dataService = new DataService(configuration);
        dataService.CreateConnection();

        var uploadQueue = new BlockingCollection<FileMetadata>(100);

        var files = GetFiles(configuration.GetRequiredSection("SourceFolder").Value);

        // Start producer and consumer task
        var producer = new Producer(files, configuration, dataService, uploadQueue);
        var producerTask = Task.Run(async () => { await producer.Start(); });

        //  Start consumer task
        // var consumer = new Consumer(configuration, dataService, uploadQueue);
        // var consumerTask = Task.Run(async () =>
        // {
        //     await Task.Delay(TimeSpan.FromSeconds(4));
        //     await consumer.Start();
        //     Console.WriteLine("Consumer started");
        // });

        //Thread.Sleep(5000);
        await Task.WhenAll(producerTask);
        Console.WriteLine("File uploaded successfully!");
    }

    private static IConfiguration GetConfiguration()
    {
        var builder = new ConfigurationBuilder()
            .SetBasePath(Directory.GetCurrentDirectory())
            .AddJsonFile("appsettings.json", false, true);

        return builder.Build();
    }

    private static IEnumerable<FileInfo> GetFiles(string sourceFolder)
    {
        var dirInfo = new DirectoryInfo(sourceFolder);
        var files = dirInfo.GetFiles().Where(file => (file.Attributes & FileAttributes.Hidden) == 0).ToArray();
        return files;
    }
}