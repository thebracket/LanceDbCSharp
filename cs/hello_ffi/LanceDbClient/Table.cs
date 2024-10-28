using Apache.Arrow;

namespace LanceDbClient;

//TODO: Implement IDiposable and the matching FFI call
public class Table : IDisposable
{
    internal Table(string name, long tableId, long connectionId, Schema? schema)
    {
        _name = name;
        _tableId = tableId;
        _connectionHandle = connectionId;
        this.schema = schema;
    }

    private string _name;
    private readonly long _tableId;
    private readonly long _connectionHandle;

    Schema? schema
    {
        get
        {
            if (this.schema == null)
            {
                // TODO: Fetch it.
            }
            return this.schema;
        }
    }
    
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
}