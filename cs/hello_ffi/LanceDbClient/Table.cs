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
            Ffi.close_table(_connectionHandle, _tableId, (code, message) =>
            {
                if (code < 0 && message != null)
                {
                    throw new Exception("Failed to close the table: " + message);
                }
            });
        }
    }
    
    public long CountRows()
    {
        var count = 0L;
        Ffi.count_rows(_connectionHandle, _tableId, (code, message) =>
        {
            if (code < 0 && message != null)
            {
                throw new Exception("Failed to count rows: " + message);
            }
            count = code;
        });
        return count;
    }

    public void CreateScalarIndex(string columnName, IndexType indexType = IndexType.BTree, bool replace = true)
    {
        // TODO: Not handling index type yet
        Ffi.create_scalar_index(_connectionHandle, _tableId, columnName, 0, replace, (code, message) =>
        {
            if (code < 0 && message != null)
            {
                throw new Exception("Failed to create the scalar index: " + message);
            }
        });
    }
}