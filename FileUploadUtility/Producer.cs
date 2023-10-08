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
        // Get file metadata for resuming chunking
        var resumeChunking = _dataService.GetFileMetadataToChunk();

        // A timer that puts chunks in to queue after every 10 second
        var delayMilliseconds = 2000;
        var timerCallback = new TimerCallback(AddPendingChunksToQueue);
        var timer = new Timer(timerCallback.Invoke, null, 0, delayMilliseconds);

        // Set the chunk size to 8MB
        var chunkSizeInBytes = int.Parse(_configuration.GetRequiredSection("ChunkSizeInBytes").Value!) * 1024 * 1024;

        // Set the maximum degree of parallelism to 4
        var options = new ParallelOptions
        {
            MaxDegreeOfParallelism = 4  
        };
        
        // Iterate over files in parallel   
        await Parallel.ForEachAsync(
            _files.Select((file, i) => (Value: file, Index: i)), options,
            async (file, cancellationToken) =>
            {
                if (resumeChunking.Any(x => x.FilePath == file.Value.FullName && x.IsEndOfFile == 1))
                {
                    // Skip processing if complete file is chunked
                    return;
                }
                else
                    try
                    {
                        // Get last created chunk for the file
                        var lastCreatedChunk = resumeChunking.ToList().Where(x => x.FilePath == file.Value.FullName);

                        // Generate a unique file id if no chunks have been created for the file
                        var fileId = lastCreatedChunk.Any()
                            ? lastCreatedChunk.First().FileId
                            : Guid.NewGuid().ToString();

                        await using var fileStream =
                            new FileStream(file.Value.FullName, FileMode.Open, FileAccess.Read);
                        {
                            //  Calculate number of chunks
                            var numberOfChunks = (int)Math.Ceiling((double)fileStream.Length / chunkSizeInBytes);

                            //  Create a buffer for each chunk
                            var buffer = new byte[chunkSizeInBytes];

                            var startingPoint = 0;
                            long offset = 0;
                            if (lastCreatedChunk.Any())
                            {
                                // Set the stream position to the start of the next chunk
                                fileStream.Seek(lastCreatedChunk.First().NumberOfChunks * chunkSizeInBytes,
                                    SeekOrigin.Begin);
                                startingPoint = lastCreatedChunk.First().NumberOfChunks;
                                offset = lastCreatedChunk.First().NumberOfChunks * chunkSizeInBytes;
                            }

                            for (var i = startingPoint; i < numberOfChunks; i++)
                            {
                                var endOfFile = i == numberOfChunks - 1;
                                long bytesRead = await fileStream.ReadAsync(buffer, cancellationToken);

                                var fileMetadata = new FileMetadata(buffer, offset, bytesRead,
                                    Status.Pending.ToString(), i, fileId, file.Value.Name, file.Value.FullName,
                                    endOfFile,
                                    string.Empty, numberOfChunks);

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
    }

    private void AddPendingChunksToQueue(object state)
    {
        var pendingChunks = _dataService.GetFileMetadataByStatus(Status.Pending.ToString());
        if (pendingChunks.Any())
        {
            var chunks = pendingChunks.OrderBy(fn => fn.Sequence).ToList();
            foreach (var chunk in chunks)
                _uploadQueue.Add(chunk);
        }

        Console.WriteLine("Adding chunks to queue");
    }
}