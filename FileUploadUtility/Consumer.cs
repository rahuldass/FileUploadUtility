using System.Collections.Concurrent;
using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Specialized;
using Microsoft.Extensions.Configuration;

namespace FileUploadUtility;

public class Consumer
{
    //  reference: https://stackoverflow.com/questions/77091896/checking-each-block-being-staged
    private readonly IConfiguration _configuration;
    private readonly DataService _dataService;
    private readonly BlockingCollection<FileMetadata> _uploadQueue;

    public Consumer(IConfiguration configuration, DataService dataService, BlockingCollection<FileMetadata> uploadQueue)
    {
        _configuration = configuration;
        _dataService = dataService;
        _uploadQueue = uploadQueue;
    }

    public async Task Start()
    {
        var connectionString =
            _configuration.GetRequiredSection("AzureBlobStorageSettings:ConnectionString").Value;

        var containerName =
            _configuration.GetRequiredSection("AzureBlobStorageSettings:ContainerName").Value;

        var containerClient = new BlobContainerClient(connectionString, containerName);

        var semaphoreSlim = new SemaphoreSlim(4);

        while (_uploadQueue.TryTake(out var chunk))
        {
            await semaphoreSlim.WaitAsync();

            var blockBlobClient = containerClient.GetBlockBlobClient(chunk.FileId + "-" + chunk.FileName);

            try
            {
                //  Stage chunk to Azure server
                await StageBlocksAsync(blockBlobClient, chunk);
            }
            catch (RequestFailedException ex)
            {
                Console.WriteLine($"Error while staging block {chunk.Id}: {ex.Message}");
                var retryCount = await RetryStagingFailedBlocks(blockBlobClient, chunk);
                if (retryCount == 0)
                {
                    //  If all retries failed update status to "Failed"
                    _dataService.UpdateChunkStatusById(chunk.Id, Status.Failed.ToString());
                    throw;
                }
            }

            //  Commit the blocks to blob
            // if (blockBlobClient != null)
            // {
            //     await blockBlobClient.CommitBlockListAsync(blockIds);
            //     _dataService.UpdateChunkStatusByFileName(chunk.FileName, Status.Finished.ToString());
            // }

            semaphoreSlim.Release();
        }
    }

    private async Task StageBlocksAsync(BlockBlobClient blockBlobClient, FileMetadata chunk)
    {
        using var memoryStream = new MemoryStream(chunk.Data);
        var blockId = Convert.ToBase64String(BitConverter.GetBytes(chunk.Id));

        var threadId = Thread.CurrentThread.ManagedThreadId;
        Console.WriteLine(
            $"Staging chunk of File - {chunk.FileName} with Chunk Id - {chunk.Id} by Thread Id - {threadId}");
        await blockBlobClient.StageBlockAsync(blockId, memoryStream);

        //  Update blockId and status to "Staged"
        _dataService.UpdateChunkById(chunk.Id, Status.Staged.ToString(), blockId);
    }

    private async Task<int> RetryStagingFailedBlocks(BlockBlobClient blockBlobClient, FileMetadata chunk)
    {
        var retryCount = 3;
        while (retryCount > 0)
        {
            await Task.Delay(TimeSpan.FromSeconds(1));
            try
            {
                await StageBlocksAsync(blockBlobClient, chunk);
                break;
            }
            catch (RequestFailedException retryEx)
            {
                Console.WriteLine($"Retry failed: {retryEx.Message}");
                retryCount--;
            }
        }

        return retryCount;
    }
}