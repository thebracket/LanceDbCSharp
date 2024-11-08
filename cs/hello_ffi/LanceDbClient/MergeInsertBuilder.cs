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
    
    internal MergeInsertBuilder(long connectionId, long tableId, IEnumerable<string> columns)
    {
        _connectionId = connectionId;
        _tableId = tableId;
        _columns = columns.ToList();
    }
    
    public ILanceMergeInsertBuilder WhenMatchedUpdateAll(string? where = null)
    {
        _where = where;
        return this;
    }

    public ILanceMergeInsertBuilder WhenNotMatchedInsertAll()
    {
        _whenNotMatchedInsertAll = true;
        return this;
    }

    public ILanceMergeInsertBuilder WhenNotMatchedBySourceDelete(string? condition = null)
    {
        _whenNotMatchedBySourceDelete = condition;
        return this;
    }

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
    
    public void Execute(IEnumerable<Dictionary<string, object>> data)
    {
        throw new NotImplementedException();
    }

}