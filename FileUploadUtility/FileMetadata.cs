namespace FileUploadUtility;

public class FileMetadata
{
    public FileMetadata()
    {
    }

    public FileMetadata(byte[] data, long offset, long length, string status, int sequence, string fileId, string fileName,
        string filePath, bool isEndOfFile, string blockId)
    {
        Data = data;
        Offset = offset;
        Length = length;
        FileId = fileId;
        Status = status;
        Sequence = sequence;
        IsEndOfFile = isEndOfFile;
        BlockId = blockId;
        FileName = fileName;
        FilePath = filePath;
    }

    public int Id { get; set; }
    public byte[] Data { get; set; }
    public long Offset { get; set; }
    public long Length { get; set; }
    public string Status { get; set; }
    public int Sequence { get; set; }
    public bool IsEndOfFile { get; set; }
    public string FileId { get; set; }
    public string FileName { get; set; }
    public string FilePath { get; set; }

    // Populated after staging the chunk
    public string BlockId { get; set; }
}

public enum Status
{
    Pending,
    Finished,
    Failed,
    Staged
}