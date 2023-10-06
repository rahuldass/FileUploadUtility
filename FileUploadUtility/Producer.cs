using System.Collections.Concurrent;
using Microsoft.Extensions.Configuration;

namespace FileUploadUtility;

public class Producer
{
    private readonly IConfiguration _configuration;
    private readonly DataService _dataService;
    private readonly IEnumerable<FileInfo> _files;
    private readonly BlockingCollection<FileMetadata> _uploadQueue;

    public Producer(IEnumerable<FileInfo> files, IConfiguration configuration, DataService dataService,
        BlockingCollection<FileMetadata> uploadQueue)
    {
        _files = files;
        _configuration = configuration;
        _dataService = dataService;
        _uploadQueue = uploadQueue;
    }

    public async Task Start()
    {
        // A timer that puts chunks in to queue after every 10 second
        int delayMilliseconds = 3000;
        TimerCallback timerCallback = new TimerCallback(AddPendingChunksToQueue);
        Timer timer = new Timer(timerCallback.Invoke, null, 0, delayMilliseconds);

        // Set the chunk size to 8MB
        var chunkSizeInBytes = int.Parse(_configuration.GetRequiredSection("ChunkSizeInBytes").Value!) * 1024 * 1024;

        var options = new ParallelOptions
        {
            MaxDegreeOfParallelism = 4
        };

        await Parallel.ForEachAsync(
            _files.Select((file, i) => (Value: file, Index: i)), options,
            async (file, cancellationToken) =>
            {
                try
                {
                    var fileId = Guid.NewGuid().ToString();

                    await using var fileStream = new FileStream(file.Value.FullName, FileMode.Open, FileAccess.Read);
                    {
                        //  Calculate number of chunks
                        int numberOfChunks = (int)Math.Ceiling((double)fileStream.Length / chunkSizeInBytes);

                        //  Create a buffer for each chunk
                        var buffer = new byte[chunkSizeInBytes];
                        long bytesRead;
                        long offset = 0;
                        bool endOfFile = false;

                        for (var i = 0; i < numberOfChunks; i++)
                        {
                            endOfFile = i == numberOfChunks - 1;
                            bytesRead = await fileStream.ReadAsync(buffer, cancellationToken);

                            var fileMetadata = new FileMetadata(buffer, offset, bytesRead,
                            Status.Pending.ToString(), i, fileId, file.Value.Name, file.Value.FullName, endOfFile, string.Empty);

                            _dataService.InsertFileMetadata(fileMetadata);
                            offset += bytesRead;
                        }
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex);
                    throw;
                }
            }
        );

        //_uploadQueue.CompleteAdding();

        // foreach (var file in _files)
        //     try
        //     {
        //         var chunks = _dataService.GetChunksByFileName(file.Name);
        //         if (chunks.Any())
        //             continue;
        //         await using var fileStream = new FileStream(file.FullName, FileMode.Open, FileAccess.Read);
        //
        //         // Create a buffer for each chunk
        //         var buffer = new byte[chunkSizeInBytes];
        //         long bytesRead;
        //         long offset = 0;
        //
        //         while ((bytesRead = await fileStream.ReadAsync(buffer)) > 0)
        //         {
        //             var chunk = new Chunk(buffer, offset, bytesRead, file.Name, Status.Pending.ToString(),
        //                 string.Empty);
        //             _dataService.InsertChunk(chunk);
        //             offset += bytesRead;
        //         }
        //     }
        //     catch (Exception ex)
        //     {
        //         Console.WriteLine($"Error reading file: {ex.Message}");
        //     }
        //
    }

    private void AddPendingChunksToQueue(object state)
    {
        var pendingChunks = _dataService.GetFileMetadataByStatus(Status.Pending.ToString());
        if (pendingChunks.Any())
        {
            var chunks = pendingChunks.OrderBy(fn => fn.Sequence).ToList();
            foreach (var chunk in chunks)
            {
                _uploadQueue.Add(chunk);
            }
        }
    }
}