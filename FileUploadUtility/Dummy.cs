namespace FileUploadUtility;

public class Dummy
{
    #region ResumableFileUpload

    public async Task ResumableFileUpload()
    {
        // This file is used to track uploaded blocks
        string resumeStateFilePath = @"C:\Users\rdass\source\repos\file-upload\ResumeState\resume_state.txt";

        var connectionString = _config["AzureStorage:ConnectionString"];
        var containerName = _config["AzureStorage:Container"];

        var containerClient = new BlobContainerClient(connectionString, containerName);

        var files = FilesProvider.GetFiles(_config["AzureStorage:SourceFolder"]);
        if (!files.Any())
        {
            _logger.LogInformation("No files found in the source folder to upload");
        }

        int blockSize = 8 * 1024 * 1024;

        var options = new BlockBlobStageBlockOptions();

        _logger.LogInformation("Transfer files to blob storage started.");

        foreach (var file in files)
        {
            try
            {
                _logger.LogInformation("Transferring file {name} of {size} bytes", file.Name, file.Length);

                var blobClient = containerClient.GetBlockBlobClient(file.Name);
                using (var fileStream = File.OpenRead(file.FullName))
                {
                    long fileSize = fileStream.Length;

                    // Calculate the number of blocks to upload
                    int numBlocks = (int)Math.Ceiling((double)fileSize / blockSize);
                    _logger.LogInformation("{blockNumber} number of blocks to upload", numBlocks);

                    List<string> uploadedBlockIds = LoadUploadedBlockIds(resumeStateFilePath);
                    int lastUploadedBlockIndex = uploadedBlockIds.Count - 1;
                    if (lastUploadedBlockIndex >= 0)
                    {
                        int lastUploadedBlockNumber = BitConverter.ToInt32(Convert.FromBase64String(uploadedBlockIds[lastUploadedBlockIndex]), 0);
                        _logger.LogInformation("Last uploaded block number {lastBlock}", lastUploadedBlockNumber);
                       
                        int nextBlockNumber = lastUploadedBlockNumber + 1;

                        if (nextBlockNumber < numBlocks)
                        {
                            // Set the stream position to the start of the next block
                            fileStream.Seek(nextBlockNumber * blockSize, SeekOrigin.Begin);

                            _logger.LogInformation("Next block to upload {nextBlock}", nextBlockNumber);
                            File.WriteAllLines(resumeStateFilePath, uploadedBlockIds);
                        }
                    }

                    // Track progress
                    var progressHandler = new Progress<long>();
                    progressHandler.ProgressChanged += (_, uploadedBytes) =>
                    {
                        Console.Write($"\rUploading file {file.Name}, {uploadedBytes} of {fileSize} bytes transferred.");
                    };
                    options.ProgressHandler = progressHandler;

                    // Upload each block
                    List<string> blockIds = new List<string>();
                    for (int i = lastUploadedBlockIndex + 1; i < numBlocks; i++)
                    {
                        byte[] blockData = new byte[blockSize];
                        int bytesRead = await fileStream.ReadAsync(blockData, 0, blockSize);

                        // Upload the block
                        string blockId = Convert.ToBase64String(BitConverter.GetBytes(i));
                        using (MemoryStream memoryStream = new MemoryStream(blockData, 0, bytesRead))
                        {
                            await blobClient.StageBlockAsync(blockId, memoryStream, options);
                        }

                        // Save the uploaded block ID to resume from in case of interruptions
                        blockIds.Add(blockId);
                        SaveUploadedBlockIds(blockIds, resumeStateFilePath);
                    }

                    // Commit the block list
                    if (numBlocks == uploadedBlockIds.Count)
                    {
                        // All blocks are already uploaded; no need to commit again
                        Console.WriteLine("File already uploaded!");
                    }
                    else
                    {
                        // Combine the newly uploaded blocks with any previously uploaded blocks
                        uploadedBlockIds.AddRange(blockIds);

                        await blobClient.CommitBlockListAsync(uploadedBlockIds);
                        Console.WriteLine("\nFile upload complete!");

                        ValidateFile(file, connectionString, containerName);

                        // Once the file uploaded delete the tracking file
                        //File.Delete(resumeStateFilePath);
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex.ToString());
            }
        }
    }

    private List<string> LoadUploadedBlockIds(string resumeStateFilePath)
    {
        if (File.Exists(resumeStateFilePath))
        {
            return File.ReadAllLines(resumeStateFilePath).ToList();
        }
        else
        {
            return new List<string>();
        }
    }

    private void SaveUploadedBlockIds(List<string> blockIds, string resumeStateFilePath)
    {
        // Write the uploaded block IDs to the resume state file
        File.WriteAllLines(resumeStateFilePath, blockIds);
        _logger.LogInformation("\nSave uploaded block {block}", blockIds);
    }

    private void ValidateFile(FileInfo fileInfo, string connectionString, string containerName)
    {
        _logger.LogInformation("Validating file...");
        string expectedHash;
        using (var fileStream = File.OpenRead(fileInfo.FullName))
        {
            expectedHash = HashFile(SHA512.Create(), fileStream);
        }

        var containerClient = new BlobContainerClient(connectionString, containerName);
        var blobClient = containerClient.GetBlockBlobClient(fileInfo.Name);

        string uploadedHash;
        using (var readStream = blobClient.OpenRead())
        {
            uploadedHash = HashFile(SHA512.Create(), readStream);
        }

        if (uploadedHash != expectedHash)
        {
            _logger.LogInformation("Hashes don't match Uploaded Hash: {uploadedHash} | Expected Hash: {expectedHash} ", uploadedHash, expectedHash);
        }
        else
        {
            _logger.LogInformation("Hash is valid!");
        }
    }

    private string HashFile(HashAlgorithm hasher, Stream stream)
    {
        hasher.Initialize();
        var buffer = new byte[8 * 1024 * 1024];
        while (true)
        {
            int read = stream.Read(buffer, 0, buffer.Length);
            if (read == 0) break;
            hasher.TransformBlock(buffer, 0, read, null, 0);
        }

        hasher.TransformFinalBlock(new byte[0], 0, 0);
        var hash = hasher.Hash;
        return BitConverter.ToString(hash).Replace("-", "");
    }

    #endregion
}