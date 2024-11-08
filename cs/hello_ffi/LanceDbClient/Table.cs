using Apache.Arrow;
using LanceDbInterface;
using MathNet.Numerics.LinearAlgebra;
using Array = Apache.Arrow.Array;

namespace LanceDbClient;

public partial class Table : ITable, IDisposable
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
    
    /// <summary>
    /// Creates a scalar index on the specified column.
    /// </summary>
    /// <param name="columnName">The column to index</param>
    /// <param name="indexType">The type of index to create</param>
    /// <param name="replace">Should the index be replaced?</param>
    /// <exception cref="Exception">If index creation fails.</exception>
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
    
    /// <summary>
    /// Creates a IvfPqIndex on the specified column.
    /// </summary>
    /// <param name="columnName">The name of the column to index</param>
    /// <param name="metric">The distance metric</param>
    /// <param name="numPartitions">Number of partitions</param>
    /// <param name="numSubVectors">Number of sub vectors</param>
    /// <param name="replace">Should the index be replaced if it already exists?</param>
    /// <exception cref="Exception">If the table is not open, or an error occurs executing the request.</exception>
    public void CreateIndex(string columnName, Metric metric = Metric.L2, int numPartitions = 256, int numSubVectors = 96,
        bool replace = true)
    {
        if (!IsOpen) throw new Exception("Table is not open.");
        Exception? exception = null;
        Ffi.create_index(_connectionHandle, _tableHandle, columnName, (uint)metric, (uint)numPartitions, (uint)numSubVectors, replace, (code, message) =>
        {
            if (code < 0 && message != null)
            {
                exception = new Exception("Failed to create the index: " + message);
            }
        });
        if (exception != null) throw exception;
    }
    
    /// <summary>
    /// Create a Full Text Search index on the specified columns.
    /// </summary>
    /// <param name="columnNames">The column names.</param>
    /// <param name="orderingColumnNames">Not implemented yet.</param>
    /// <param name="replace">Should the index be replaced?</param>
    /// <param name="withPosition">Specify a position in the index</param>
    /// <param name="writerHeapSize">Not implemented yet</param>
    /// <param name="tokenizerName">Defaults to "simple", you can also use "whitespace" and "raw"</param>
    /// <param name="useTantivy">Not implemented yet</param>
    /// <exception cref="Exception">If index creation fails</exception>
    public void CreateFtsIndex(IEnumerable<string> columnNames, IEnumerable<string> orderingColumnNames, bool replace = false,
        bool withPosition = true, int writerHeapSize = 1073741824, string tokenizerName = "default",
        bool useTantivy = true)
    {
        if (!IsOpen) throw new Exception("Table is not open.");
        Exception? exception = null;
        Ffi.create_full_text_index(_connectionHandle, _tableHandle, columnNames.ToArray(), (ulong)columnNames.Count(), withPosition, replace, tokenizerName, (code, message) =>
        {
            if (code < 0 && message != null)
            {
                exception = new Exception("Failed to create the full text index: " + message);
            }
        });
        if (exception != null) throw exception;
    }
    
    /// <summary>
    /// Creates a MergeInsert builder, allowing you to build a MergeInsert task.
    /// </summary>
    /// <param name="on">Columns to include in the merge insert</param>
    /// <returns>A MergeInsert builder.</returns>
    public ILanceMergeInsertBuilder MergeInsert(IEnumerable<string> on)
    {
        return new MergeInsertBuilder(_connectionHandle, _tableHandle, on);
    }
    
    /// <summary>
    /// Update rows in the table.
    /// </summary>
    /// <param name="updates">A dictionary of (column => value) updates. Currently, value MUST be a string - the Rust API expects it.</param>
    /// <param name="whereClause">Optional where clause for update</param>
    /// <exception cref="Exception">If the table is closed, or the command fails.</exception>
    public void Update(IDictionary<string, object> updates, string? whereClause = null)
    {
        if (!IsOpen) throw new Exception("Table is not open.");
        Exception? exception = null;

        var updateList = new List<string>();
        foreach (var (key, value) in updates)
        {
            updateList.Add(key + "=" + value.ToString());
        }

        var rowsUpdated = 0;
        
        Ffi.update_rows(
            _connectionHandle,
            _tableHandle,
            updateList.ToArray(),
            (ulong)updateList.Count,
            whereClause,
            (code, message) =>
            {
                if (code < 0 && message != null)
                {
                    exception = new Exception("Failed to update rows: " + message);
                }
            },
            (rows =>
            {
                rowsUpdated += (int)rows;
            })
        );
        // TODO: Would it make sense for the signature to return the number of rows updated?
        if (exception != null) throw exception;
    }
    
    public void UpdateSql(IDictionary<string, string> updates, string? whereClause = null)
    {
        // TODO: This isn't implemented in the NodeJS API, or directly in the Rust API.
        throw new NotImplementedException();
    }
    
    /// <summary>
    /// Deletes rows from the table.
    /// </summary>
    /// <param name="whereClause">SQL-like query to specify which rows should be deleted.</param>
    /// <exception cref="Exception">If deletion fails</exception>
    public void Delete(string whereClause)
    {
        if (!IsOpen) throw new Exception("Table is not open.");
        Exception? exception = null;
        Ffi.delete_rows(_connectionHandle, _tableHandle, whereClause, (code, message) =>
        {
            if (code < 0 && message != null)
            {
                exception = new Exception("Failed to delete rows: " + message);
            }
        });
        if (exception != null) throw exception;
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
    
    /// <summary>
    /// Calls the table optimizer, compacting and pruning the table.
    /// </summary>
    /// <param name="cleanupOlderThan">Not Implemented Yet</param>
    /// <param name="deleteUnverified">Not Implemented Yet</param>
    /// <returns></returns>
    /// <exception cref="Exception">If optimization fails</exception>
    public OptimizeStats Optimize(TimeSpan? cleanupOlderThan = null, bool deleteUnverified = false)
    {
        if (!IsOpen) throw new Exception("Table is not open.");

        CompactionMetrics? compaction = null;
        RemovalStats? prune = null;
        
        Exception? exception = null;
        Ffi.optimize_table(_connectionHandle, _tableHandle, (code, message) =>
        {
            if (code < 0 && message != null)
            {
                exception = new Exception("Failed to compact files: " + message);
            }
        }, (fragmentsRemoved, fragmentsAdded, filesRemoved, filesAdded) =>
        {
            compaction = new CompactionMetrics
            {
                FragmentsRemoved = (int)fragmentsRemoved,
                FragmentsAdded = (int)fragmentsAdded,
                FilesRemoved = (int)filesRemoved,
                FilesAdded = (int)filesAdded
            };
        }, (removed, added) =>
        {
            prune = new RemovalStats
            {
                BytesRemoved = (int)removed,
                OldVersionsRemoved = (int)added
            };
        }
            );
        if (exception != null) throw exception;
        
        return new OptimizeStats
        {
            Compaction = compaction,
            Prune = prune
        };
    }
    
    /// <summary>
    /// Create a query builder for searching the table.
    /// </summary>
    /// <returns>A ready to use QueryBuilder</returns>
    public ILanceQueryBuilder Search()
    {
        return new QueryBuilder(_connectionHandle, _tableHandle);
    }

    public ILanceQueryBuilder Search(Array vector, string vectorColumnName, QueryType queryType = QueryType.Auto)
    {
        throw new NotImplementedException();
    }

    public ILanceQueryBuilder Search(ChunkedArray vectors, string vectorColumnName, QueryType queryType = QueryType.Auto)
    {
        throw new NotImplementedException();
    }

    public ILanceQueryBuilder Search<T>(List<T> vector, string vectorColumnName, QueryType queryType = QueryType.Auto)
    {
        throw new NotImplementedException();
    }

    public ILanceQueryBuilder Search<T>(Vector<T> vector, string vectorColumnName, QueryType queryType = QueryType.Auto) where T : struct, IEquatable<T>, IFormattable
    {
        throw new NotImplementedException();
    }

    public ILanceQueryBuilder Search<T>(Matrix<T> vector, string vectorColumnName, QueryType queryType = QueryType.Auto) where T : struct, IEquatable<T>, IFormattable
    {
        throw new NotImplementedException();
    }

    public bool IsOpen { get; private set; }
    public Schema Schema { get; }
    public string Name { get; }
    
    public void Add(IEnumerable<Dictionary<string, object>> data, WriteMode mode = WriteMode.Append,
        BadVectorHandling badVectorHandling = BadVectorHandling.Error, float fillValue = 0)
    {
        throw new NotImplementedException();
    }
    
    /// <summary>
    /// Adds rows to the table in RecordBatch format.
    /// </summary>
    /// <param name="data">The record batch to add.</param>
    /// <param name="mode">Append or overrwrite</param>
    /// <param name="badVectorHandling">Not implemented yet.</param>
    /// <param name="fillValue">Not Implemented yet.</param>
    /// <exception cref="ArgumentNullException">Data must be provided</exception>
    /// <exception cref="Exception">If the add fails.</exception>
    public unsafe void Add(IEnumerable<RecordBatch> data, WriteMode mode = WriteMode.Append,
        BadVectorHandling badVectorHandling = BadVectorHandling.Error, float fillValue = 0)
    {
        if (data == null) throw new ArgumentNullException(nameof(data));
        if (!IsOpen) throw new Exception("Table is not open.");
        Exception? exception = null;
        
        foreach (var recordBatch in data)
        {
            var batch = Ffi.SerializeRecordBatch(recordBatch);
            fixed (byte* p = batch)
            {
                Ffi.add_record_batch(_connectionHandle, _tableHandle, p, (ulong)batch.Length, (uint)mode, (uint)badVectorHandling, fillValue, (code, message) =>
                {
                    if (code < 0 && message != null)
                    {
                        exception = new Exception("Failed to add record batch: " + message);
                    }
                });
            }
            if (exception != null) throw exception;
        }
    }
    
    /// <summary>
    /// Adds data to the table in Arrow Table format.
    /// </summary>
    /// <param name="data">The dat to add</param>
    /// <param name="mode">Write mode - append or overwrite</param>
    /// <param name="badVectorHandling">Not implemented yet</param>
    /// <param name="fillValue">Not implemented yet</param>
    public void Add(Apache.Arrow.Table data, WriteMode mode = WriteMode.Append, BadVectorHandling badVectorHandling = BadVectorHandling.Error,
        float fillValue = 0)
    {
        // Extract the schema from the table
        Schema schema = data.Schema;

        // Create a RecordBatch from the table
        var arrays = new List<IArrowArray>();
        for (int i = 0; i < data.ColumnCount; i++)
        {
            var chunkedArray = data.Column(i).Data;
            var count = chunkedArray.ArrayCount; 
            for (int n = 0; n < count; n++)
            {
                var array = chunkedArray.ArrowArray(n);
                arrays.Add(array);
            }
        }
        var recordBatch = new RecordBatch(schema, arrays, (int)data.RowCount);
        var recordBatches = new List<RecordBatch> { recordBatch };
        Add(recordBatches);
    }
}