using System.Data;
using Dapper;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Configuration;

namespace FileUploadUtility;

public class DataService
{
    private readonly string _connectionString;

    public DataService(IConfiguration configuration)
    {
        _connectionString = configuration.GetRequiredSection("ConnectionStrings:SqliteConnection").Value;
    }

    public void CreateConnection()
    {
        using IDbConnection dbConnection = new SqliteConnection(_connectionString);
        {
            dbConnection.Open();
            CreateTable(dbConnection);
        }
    }

    private static void CreateTable(IDbConnection dbConnection)
    {
        dbConnection.Execute(@"
            CREATE TABLE IF NOT EXISTS FileMetadata (
                Id INTEGER PRIMARY KEY AUTOINCREMENT,
                Data BLOB,
                Offset INTEGER,
                Length INTEGER,
                Status TEXT,
                Sequence INTEGER,
                FileId INTEGER,
                FileName TEXT,
                FilePath TEXT,
                IsEndOfFile INTEGER,
                BlockId TEXT,
                NumberOfChunks INTEGER
            )");
    }

    public void InsertFileMetadata(FileMetadata fileMetadata)
    {
        const string rawSql =
            @"INSERT INTO FileMetadata (Data, Offset, Length, Status, Sequence, FileId, 
                                        FileName, FilePath, IsEndOfFile, BlockId, NumberOfChunks) 
                                VALUES (@Data, @Offset, @Length, @Status, @Sequence, @FileId, 
                                        @FileName, @FilePath, @IsEndOfFile, @BlockId, @NumberOfChunks)";

        using IDbConnection dbConnection = new SqliteConnection(_connectionString);
        {
            dbConnection.Open();
            dbConnection.Execute(rawSql, fileMetadata);
        }
    }

    public List<FileMetadata> GetFileMetadataByStatus(string status)
    {
        const string rawSql = @"SELECT * FROM FileMetadata WHERE Status = @Status";

        using IDbConnection dbConnection = new SqliteConnection(_connectionString);
        {
            dbConnection.Open();
            return dbConnection.Query<FileMetadata>(rawSql, new { Status = status }).ToList();
        }
    }

    public List<FileMetadataMapper> GetExistingChunksInfo()
    {
        // const string rawSql = @"select * from (Select FileId, FilePath, SUM(IsEndOfFile) as ST, Count(IsEndOfFile) 
        //                         as CT from FileMetadata GROUP BY FileId, FilePath) as FD";

        const string rawSql = @"Select FileId, FilePath, SUM(IsEndOfFile) as IsEndOfFile, Count(IsEndOfFile) 
                                as NumberOfChunks from FileMetadata GROUP BY FileId, FilePath";

        using IDbConnection dbConnection = new SqliteConnection(_connectionString);
        {
            dbConnection.Open();
            return dbConnection.Query<FileMetadataMapper>(rawSql).ToList();
        }
    }

    public List<FileMetadata> GetPendingChunksInfo()
    {
        // const string rawSql = @"select * from (Select FileId, FilePath, SUM(IsEndOfFile) as ST, Count(IsEndOfFile) 
        //                         as CT from FileMetadata GROUP BY FileId, FilePath) as FD";

        const string rawSql = @"Select * from FileMetadata WHERE Status = 'Pending' ORDER BY FileId, Sequence";

        using IDbConnection dbConnection = new SqliteConnection(_connectionString);
        {
            dbConnection.Open();
            return dbConnection.Query<FileMetadata>(rawSql).ToList();
        }
    }

    public Dictionary<string, string> GetBlocksToCommit()
    {
        const string rawSql = @"
                    select BlockId as [Key], F.FileId AS [Value] from FileMetadata as F 
                    inner join (select FileId, Count(FileId) as Total from FileMetadata
                    where Status == 'Staged' GROUP By FileId Having Count(FileId) = NumberOfChunks) as T 
                    On F.FileId = T.FileId
                    ORDER BY F.FileId, F.Sequence";

        using IDbConnection dbConnection = new SqliteConnection(_connectionString);
        {
            dbConnection.Open();
            return dbConnection.Query<(string Key, string Value)>(rawSql)
                .ToDictionary(pair => pair.Key, pair => pair.Value);
        }
    }

    public IEnumerable<FileMetadata> GetChunksByFileName(string fileName)
    {
        const string rawSql = @"SELECT * FROM FileMetadata WHERE FileName = @FileName";

        using IDbConnection dbConnection = new SqliteConnection(_connectionString);
        {
            dbConnection.Open();
            return dbConnection.Query<FileMetadata>(rawSql, new { FileName = fileName }).ToList();
        }
    }

    public void UpdateChunkStatusById(int id, string status)
    {
        const string rawSql = @"UPDATE FileMetadata SET Status = @Status WHERE Id = @Id";

        using IDbConnection dbConnection = new SqliteConnection(_connectionString);
        {
            dbConnection.Open();
            dbConnection.Execute(rawSql, new { Id = id, Status = status });
        }
    }

    public void UpdateChunkStatusByFileName(string fileName, string status)
    {
        const string rawSql = @"UPDATE FileMetadata SET Status = @Status WHERE FileName = @FileName";

        using IDbConnection dbConnection = new SqliteConnection(_connectionString);
        {
            dbConnection.Open();
            dbConnection.Execute(rawSql, new { FileName = fileName, Status = status });
        }
    }

    public void UpdateChunkById(int id, string status, string blockId)
    {
        const string rawSql = @"UPDATE FileMetadata SET Status = @Status, BlockId = @BlockId WHERE Id = @Id";

        using IDbConnection dbConnection = new SqliteConnection(_connectionString);
        {
            dbConnection.Open();
            dbConnection.Execute(rawSql, new { Id = id, Status = status, BlockId = blockId });
        }
    }

    public List<FileMetadata> GetStagedBlocks()
    {
        const string rawSql = @"SELECT * FROM FileMetadata WHERE Status = @Status ORDER BY Sequence";

        using IDbConnection dbConnection = new SqliteConnection(_connectionString);
        {
            dbConnection.Open();
            return dbConnection.Query<FileMetadata>(rawSql, new { Status = Status.Staged.ToString() })
                .ToList();
        }
    }

    public void DeleteCommittedChunk()
    {
        const string rawSql = @"DELETE FROM FileMetadata WHERE Status = @Status";
        using IDbConnection dbConnection = new SqliteConnection(_connectionString);
        {
            dbConnection.Open();
            dbConnection.Execute(rawSql, new { Status = Status.Finished.ToString() });
        }
    }
}