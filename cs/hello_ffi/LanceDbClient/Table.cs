using Apache.Arrow;
using LanceDbInterface;

namespace LanceDbClient;

//TODO: Implement IDiposable and the matching FFI call
public class Table : IDisposable
{
    internal Table(string name, long tableId, long connectionId, Schema schema)
    {
        _name = name;
        _tableId = tableId;
        _connectionHandle = connectionId;
        this.Schema = schema;
    }

    private string _name;
    private readonly long _tableId;
    private readonly long _connectionHandle;
    private Schema Schema { get; }
    
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
            long result = Ffi.close_table(_connectionHandle, _tableId);
            if (result < 0)
            {
                var errorMessage = Ffi.GetErrorMessageOnce(result);
                throw new Exception("Failed to close the table: " + errorMessage);
            }
        }
    }
    
    public long CountRows()
    {
        return Ffi.count_rows(_connectionHandle, _tableId);
    }

    public void CreateScalarIndex(string columnName, IndexType indexType = IndexType.BTree, bool replace = true)
    {
        // TODO: Not handling index type yet
        Ffi.create_scalar_index(_connectionHandle, _tableId, columnName, 0, replace);
    }
    
    public void Add(RecordBatch data, WriteMode mode = WriteMode.Append, BadVectorHandling badVectorHandling = BadVectorHandling.Error, float fillValue = 0.0F)
    {
        var bytes = Ffi.SerializeRecordBatch(data);
        var len = (ulong)bytes.Length;
        var result = -1L;
        unsafe
        {
            fixed (byte* p = bytes)
            {
                result = Ffi.add_rows(_connectionHandle, _tableId, p, len);
            }
        }
        if (result < 0)
        {
            var errorMessage = Ffi.GetErrorMessageOnce(result);
            throw new Exception("Failed to add rows: " + errorMessage);
        }
    }
}