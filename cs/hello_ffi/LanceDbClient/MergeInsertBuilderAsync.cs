using Apache.Arrow;

namespace LanceDbClient;

public partial class MergeInsertBuilder
{
    public Task ExecuteAsync(Apache.Arrow.Table data, CancellationToken token = default)
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
        return ExecuteAsync(recordBatches, token);
    }

    public Task ExecuteAsync(IEnumerable<RecordBatch> data, CancellationToken token = default)
    {
        var tcs = new TaskCompletionSource();
        
        Ffi.ResultCallback callback = (result, message) =>
        {
            if (result < 0)
            {
                tcs.SetException(new Exception(message));
            }
            else
            {
                tcs.SetResult();
            }
        };
        
        Task.Run(() =>
        {
            unsafe
            {
                foreach (var recordBatch in data)
                {
                    var batch = Ffi.SerializeRecordBatch(recordBatch);
                    fixed (byte* p = batch)
                    {
                        Ffi.merge_insert_with_record_batch(
                            _connectionId, _tableId, _columns.ToArray(), (ulong)_columns.Count, _whenNotMatchedInsertAll,
                            _where, _whenNotMatchedBySourceDelete, p, (ulong)batch.Length, callback);
                    }
                }
            }
        }, token);
        return tcs.Task;
    }
    
    public Task ExecuteAsync(IEnumerable<Dictionary<string, object>> data, CancellationToken token = default)
    {
        var batches = ArrayHelpers.ConcreteToArrowTable(data, _schema);
        return ExecuteAsync(batches, token);
    }

}