namespace FileUploadUtility;

public class FileMetadataMapper
{
    public string FileId { get; set; }
    public string FilePath { get; set; }
    public int IsEndOfFile { get; set; }
    public int NumberOfChunks { get; set; }
}