using Apache.Arrow;
using LanceDbInterface;
using Array = Apache.Arrow.Array;

namespace LanceDbClient;

//TODO: Implement IDiposable and the matching FFI call
public class Table : ITable, IDisposable
{
    /// <summary>
    /// Creates a Table object, which represents a table in the database. It's represented as a handle,
    /// linked to a handle in-memory on the driver side.
    ///
    /// This is deliberately internal: you should NOT be able to create a Table object directly, you MUST
    /// go through Connection.
    /// </summary>
    /// <param name="name">The table name</param>
    /// <param name="tableHandle">The table handle</param>
    /// <param name="connectionId">The parent connection ID handle</param>
    /// <param name="schema">The table schema.</param>
    internal Table(string name, long tableHandle, long connectionId, Schema schema)
    {
        Name = name;
        _tableHandle = tableHandle;
        _connectionHandle = connectionId;
        Schema = schema;
        IsOpen = true;
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
            Exception? exception = null;
            Ffi.close_table(_connectionHandle, _tableHandle, (code, message) =>
            {
                if (code < 0 && message != null)
                {
                    exception = new Exception("Failed to close the table: " + message);
                }
            });
            if (exception != null) throw exception;
        }
    }
    
    /// <summary>
    /// Count the number of rows in the table.
    /// </summary>
    /// <param name="filter">Filter to apply</param>
    /// <returns>Row count</returns>
    /// <exception cref="Exception">If the table is not open or the operation fails</exception>
    public int CountRows(string? filter = null)
    {
        if (!IsOpen) throw new Exception("Table is not open.");
        var count = 0L;
        Exception? exception = null;
        Ffi.count_rows(_connectionHandle, _tableHandle, filter, (code, message) =>
        {
            if (code < 0 && message != null)
            {
                exception = new Exception("Failed to count rows: " + message);
            }
            count = code;
        });
        if (exception != null) throw exception;
        return (int)count;
    }

    public void CreateScalarIndex(string columnName, LanceDbInterface.IndexType indexType = LanceDbInterface.IndexType.BTree, bool replace = true)
    {
        if (!IsOpen) throw new Exception("Table is not open.");
        Exception? exception = null;
        Ffi.create_scalar_index(_connectionHandle, _tableHandle, columnName, (uint)indexType, replace, (code, message) =>
        {
            if (code < 0 && message != null)
            {
                exception = new Exception("Failed to create the scalar index: " + message);
            }
        });
        if (exception != null) throw exception;
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

    /// <summary>
    /// Close the table.
    /// </summary>
    /// <exception cref="Exception">If the table is open, or the operation fails</exception>
    public void Close()
    {
        if (!IsOpen) throw new Exception("Table is not open.");
        Exception? exception = null;
        Ffi.close_table(_connectionHandle, _tableHandle, (code, message) =>
        {
            if (code < 0 && message != null)
            {
                exception = new Exception("Failed to close the table: " + message);
            }
        });
        if (exception != null) throw exception;
        IsOpen = false;
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

    public bool IsOpen { get; private set; }
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