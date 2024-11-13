using Apache.Arrow;
using LanceDbInterface;

namespace LanceDbClient;

public partial class MergeInsertBuilder : ILanceMergeInsertBuilder
{
    private readonly long _connectionId;
    private readonly long _tableId;
    private readonly List<string> _columns;
    private string? _where = null;
    private bool _whenNotMatchedInsertAll = false;
    private string? _whenNotMatchedBySourceDelete = null;
    private readonly Schema _schema;

    internal MergeInsertBuilder(long connectionId, long tableId, IEnumerable<string> columns, Schema schema)
    {
        _connectionId = connectionId;
        _tableId = tableId;
        _columns = columns.ToList();
        _schema = schema;
    }
    
    /// <summary>
    /// Sets "when matched, update all" in the query engine.
    /// </summary>
    /// <param name="where">Optional search criteria</param>
    /// <returns>The MergeInsert builder to continue building.</returns>
    public ILanceMergeInsertBuilder WhenMatchedUpdateAll(string? where = null)
    {
        _where = where;
        return this;
    }

    /// <summary>
    /// Sets "when not matched, insert all" in the query engine.
    /// </summary>
    /// <returns>The MergeInsert builder to continue building.</returns>
    public ILanceMergeInsertBuilder WhenNotMatchedInsertAll()
    {
        _whenNotMatchedInsertAll = true;
        return this;
    }

    /// <summary>
    /// Sets "when not matched by source, delete" in the query engine.
    /// </summary>
    /// <param name="condition">The row criteria to match.</param>
    /// <returns>The MergeInsert builder to continue building.</returns>
    public ILanceMergeInsertBuilder WhenNotMatchedBySourceDelete(string? condition = null)
    {
        _whenNotMatchedBySourceDelete = condition;
        return this;
    }

    /// <summary>
    /// Transforms Apache Arrow Table data into a RecordBatch and executes the merge insert.
    /// </summary>
    /// <param name="data">Data in Apache Arrow table format.</param>
    public void Execute(Apache.Arrow.Table data)
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
        Execute(recordBatches);
    }
    
    /// <summary>
    /// Executes the query with a RecordBatch.
    /// </summary>
    /// <param name="data">The data in RecordBatch format.</param>
    /// <exception cref="Exception">If the query fails</exception>
    public unsafe void Execute(IEnumerable<RecordBatch> data)
    {
        Exception? exception = null;
        
        // Serialize the data
        foreach (var recordBatch in data)
        {
            var batch = Ffi.SerializeRecordBatch(recordBatch);
            fixed (byte* p = batch)
            {
                Ffi.merge_insert_with_record_batch(
                    _connectionId,
                    _tableId,
                    _columns.ToArray(),
                    (ulong)_columns.Count,
                    _whenNotMatchedInsertAll,
                    _where,
                    _whenNotMatchedBySourceDelete,
                    p,
                    (ulong) batch.Length,
                    (result, message) =>
                    {
                        if (result < 0)
                        {
                            exception = new Exception(message);
                        }
                    }
                );
                if (exception != null) throw exception;
            }
        }
    }
    
    /// <summary>
    /// Execute a merge-insert with a list of dictionaries. Data will be converted
    /// to a RecordBatch and submitted.
    /// </summary>
    /// <param name="data">Each entry should be a dictionary. Each dictionary item represents a field.</param>
    public void Execute(IEnumerable<Dictionary<string, object>> data)
    {
        var batches = ArrayHelpers.ConcreteToArrowTable(data, _schema);
        Execute(batches);
    }

}