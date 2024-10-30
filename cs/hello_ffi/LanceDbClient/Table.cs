using Apache.Arrow;
using LanceDbInterface;
using Array = Apache.Arrow.Array;

namespace LanceDbClient;

//TODO: Implement IDiposable and the matching FFI call
public class Table : ITable, IDisposable
{
    internal Table(string name, long tableHandle, long connectionId, Schema schema)
    {
        Name = name;
        _tableHandle = tableHandle;
        _connectionHandle = connectionId;
        Schema = schema;
    }

    private readonly long _tableHandle;
    private readonly long _connectionHandle;
    
    ~Table()
    {
        Dispose(true);
    }
    
    public void Dispose()
    {
        // Dispose of unmanaged resources.
        Dispose(true);
        // Suppress finalization.
        GC.SuppressFinalize(this);
    }
    
    protected virtual void Dispose(bool disposing)
    {
        if (disposing)
        {
            Ffi.close_table(_connectionHandle, _tableHandle, (code, message) =>
            {
                if (code < 0 && message != null)
                {
                    throw new Exception("Failed to close the table: " + message);
                }
            });
        }
    }
    
    public int CountRows(string? filter = null)
    {
        var count = 0L;
        Ffi.count_rows(_connectionHandle, _tableHandle, (code, message) =>
        {
            if (code < 0 && message != null)
            {
                throw new Exception("Failed to count rows: " + message);
            }
            count = code;
        });
        return (int)count;
    }

    public void CreateScalarIndex(string columnName, LanceDbInterface.IndexType indexType = LanceDbInterface.IndexType.BTree, bool replace = true)
    {
        // TODO: Not handling index type yet
        Ffi.create_scalar_index(_connectionHandle, _tableHandle, columnName, 0, replace, (code, message) =>
        {
            if (code < 0 && message != null)
            {
                throw new Exception("Failed to create the scalar index: " + message);
            }
        });
    }

    public Task CreateScalarIndexAsync(string columnName, LanceDbInterface.IndexType indexType = LanceDbInterface.IndexType.BTree, bool replace = true,
        CancellationToken token = default)
    {
        throw new NotImplementedException();
    }

    public void CreateIndex(string columnName, Metric metric = Metric.L2, int numPartitions = 256, int numSubVectors = 96,
        bool replace = true)
    {
        throw new NotImplementedException();
    }

    public Task CreateIndexAsync(string columnName, Metric metric = Metric.L2, int numPartitions = 256, int numSubVectors = 96,
        bool replace = true, CancellationToken token = default)
    {
        throw new NotImplementedException();
    }

    public void CreateFtsIndex(IEnumerable<string> columnNames, IEnumerable<string> orderingColumnNames, bool replace = false,
        bool withPosition = true, int writerHeapSize = 1073741824, string tokenizerName = "default",
        bool useTantivy = true)
    {
        throw new NotImplementedException();
    }

    public Task CreateFtsIndexAsync(IEnumerable<string> columnNames, IEnumerable<string> orderingColumnNames, bool replace = false,
        bool withPosition = true, int writerHeapSize = 1073741824, string tokenizerName = "default", bool useTantivy = true,
        CancellationToken token = default)
    {
        throw new NotImplementedException();
    }

    public Task<int> CountRowsAsync(string? filter = null, CancellationToken token = default)
    {
        throw new NotImplementedException();
    }

    public ILanceMergeInsertBuilder MergeInsert(IEnumerable<string> on)
    {
        throw new NotImplementedException();
    }

    public Task<ILanceMergeInsertBuilder> MergeInsertAsync(IEnumerable<string> on, CancellationToken token = default)
    {
        throw new NotImplementedException();
    }

    public void Update(IDictionary<string, object> updates, string? whereClause = null)
    {
        throw new NotImplementedException();
    }

    public Task UpdateAsync(IDictionary<string, object> updates, string? whereClause = null, CancellationToken token = default)
    {
        throw new NotImplementedException();
    }

    public void UpdateSQL(IDictionary<string, string> updates, string? whereClause = null)
    {
        throw new NotImplementedException();
    }

    public Task UpdateSQLAsync(IDictionary<string, string> updates, string? whereClause = null, CancellationToken token = default)
    {
        throw new NotImplementedException();
    }

    public void Delete(string whereClause)
    {
        throw new NotImplementedException();
    }

    public Task DeleteAsync(string whereClause, CancellationToken token = default)
    {
        throw new NotImplementedException();
    }

    public void Close()
    {
        throw new NotImplementedException();
    }

    public Task CloseAsync(CancellationToken cancellationToken = default)
    {
        throw new NotImplementedException();
    }

    public void CompactFiles()
    {
        throw new NotImplementedException();
    }

    public Task CompactFilesAsync(CancellationToken token = default)
    {
        throw new NotImplementedException();
    }

    public ILanceQueryBuilder Search(Array vector, string vectorColumnName, QueryType queryType = QueryType.Auto)
    {
        throw new NotImplementedException();
    }

    public ILanceQueryBuilder Search(ChunkedArray vectors, string vectorColumnName, QueryType queryType = QueryType.Auto)
    {
        throw new NotImplementedException();
    }

    public bool IsOpen { get; }
    public Schema Schema { get; }
    public string Name { get; }

    public Task AddAsync(IEnumerable<Dictionary<string, object>> data, WriteMode mode = WriteMode.Append,
        BadVectorHandling badVectorHandling = BadVectorHandling.Error, float fillValue = 0,
        CancellationToken token = default)
    {
        throw new NotImplementedException();
    }

    public void Add(IEnumerable<Dictionary<string, object>> data, WriteMode mode = WriteMode.Append,
        BadVectorHandling badVectorHandling = BadVectorHandling.Error, float fillValue = 0)
    {
        throw new NotImplementedException();
    }

    public Task AddAsync(IEnumerable<RecordBatch> data, WriteMode mode = WriteMode.Append,
        BadVectorHandling badVectorHandling = BadVectorHandling.Error, float fillValue = 0,
        CancellationToken token = default)
    {
        throw new NotImplementedException();
    }

    public void Add(IEnumerable<RecordBatch> data, WriteMode mode = WriteMode.Append,
        BadVectorHandling badVectorHandling = BadVectorHandling.Error, float fillValue = 0)
    {
        throw new NotImplementedException();
    }

    public Task AddAsync(Apache.Arrow.Table data, WriteMode mode = WriteMode.Append,
        BadVectorHandling badVectorHandling = BadVectorHandling.Error, float fillValue = 0,
        CancellationToken token = default)
    {
        throw new NotImplementedException();
    }

    public void Add(Apache.Arrow.Table data, WriteMode mode = WriteMode.Append, BadVectorHandling badVectorHandling = BadVectorHandling.Error,
        float fillValue = 0)
    {
        throw new NotImplementedException();
    }
}