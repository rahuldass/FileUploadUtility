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
                BlockId TEXT
            )");
    }

    public void InsertFileMetadata(FileMetadata fileMetadata)
    {
        const string rawSql =
            @"INSERT INTO FileMetadata (Data, Offset, Length, Status, Sequence, FileId, FileName, FilePath, IsEndOfFile, BlockId) 
                                VALUES (@Data, @Offset, @Length, @Status, @Sequence, @FileId, @FileName, @FilePath, @IsEndOfFile, @BlockId)";

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

    public List<string> GetUnCommittedBlockIdsByFileName(string fileName)
    {
        const string rawSql = @"SELECT BlockId FROM FileMetadata WHERE FileName = @FileName AND Status = @Status";

        using IDbConnection dbConnection = new SqliteConnection(_connectionString);
        {
            dbConnection.Open();
            return dbConnection.Query<string>(rawSql, new { FileName = fileName, Status = Status.Staged.ToString() })
                .ToList();
        }
    }

    public void DeleteChunk(string fileName)
    {
        const string rawSql = @"DELETE FROM FileMetadata WHERE FileName = @FileName";
        using IDbConnection dbConnection = new SqliteConnection(_connectionString);
        {
            dbConnection.Open();
            dbConnection.Execute(rawSql, new { FileName = fileName });
        }
    }
}