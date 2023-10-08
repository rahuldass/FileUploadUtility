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
        AddAllPendingChunksToQueue();

        // Get existing chunk information for purpose of continuing chunking if file is half chunked
        var existingChunks = _dataService.GetExistingChunksInfo();

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
                if (existingChunks.Any(x => x.FilePath == file.Value.FullName && x.IsEndOfFile == 1))
                    // Skip processing if file is already chunked
                    return;

                try
                {
                    // Generate a unique file id if no chunks have been created for the file
                    var fileId = existingChunks
                                     .Where(x => x.FilePath == file.Value.FullName)
                                     .Select(x => x.FileId)
                                     .FirstOrDefault() ??
                                 Guid.NewGuid().ToString();

                    await using var fileStream = new FileStream(file.Value.FullName, FileMode.Open, FileAccess.Read);

                    //  Calculate number of chunks
                    var numberOfChunks = (int)Math.Ceiling((double)fileStream.Length / chunkSizeInBytes);

                    //  Set buffer size for chunk
                    var buffer = new byte[chunkSizeInBytes];

                    //  Get the last created chunk for the file
                    var lastCreatedChunk = existingChunks.FirstOrDefault(x => x.FilePath == file.Value.FullName);

                    //  Set the stream starting position
                    var startingPoint = lastCreatedChunk?.NumberOfChunks ?? 0;

                    fileStream.Seek(startingPoint * chunkSizeInBytes, SeekOrigin.Begin);

                    for (var i = startingPoint; i < numberOfChunks; i++)
                    {
                        var endOfFile = i == numberOfChunks - 1;

                        var bytesRead = await fileStream.ReadAsync(buffer, 0, chunkSizeInBytes, cancellationToken);

                        var fileMetadata = new FileMetadata(buffer, startingPoint * chunkSizeInBytes, bytesRead,
                            Status.Pending.ToString(), i, fileId, file.Value.Name, file.Value.FullName,
                            endOfFile, string.Empty, numberOfChunks);

                        _dataService.InsertFileMetadata(fileMetadata);
                        _uploadQueue.Add(fileMetadata, cancellationToken);
                        while (_uploadQueue.Count > 100)
                            // Queue overflow.
                            Console.WriteLine("Waiting for queue to process.");
                        startingPoint++;
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

    private void AddAllPendingChunksToQueue()
    {
        var pendingChunks = _dataService.GetPendingChunksInfo();
        foreach (var chunk in pendingChunks)
            _uploadQueue.Add(chunk);
    }
}