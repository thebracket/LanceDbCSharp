using Apache.Arrow;

namespace LanceDbClient;

public class Connection : IDisposable
{
    // <summary>
    // Creates a new connection to the database.
    // </summary>
    // <param name="uri">The URI of the database to connect to.</param>
    // <exception cref="Exception">If the connection fails.</exception>
    public Connection(string uri)
    {
        // Note that this will return OK even if setup had already been called, and won't
        // duplicate the setup.
        var status = FFI.setup();
        if (status != 0)
        {
            throw new Exception("Failed to setup the database client environment");
        }
        
        var handle = FFI.connect(uri);
        if (handle < 0)
        {
            var errorMessage = FFI.GetErrorMessageOnce(handle);
            throw new Exception("Failed to connect to the database: " + errorMessage);
        }
        _handle = handle;
        _connected = true;
    }

    public Table CreateTable(string name, Schema schema)
    {
        if (_handle < 0)
        {
            throw new Exception("Connection is not open");
        }

        var bytes = FFI.SerializeSchemaOnly(schema);
        var schemaHandle = -1L;

        unsafe
        {
            fixed (byte* p = bytes)
            {
                schemaHandle = FFI.submit_record_batch(p, (ulong)bytes.Length);
            }
        }
        if (schemaHandle < 0)
        {
            var errorMessage = FFI.GetErrorMessageOnce(schemaHandle);
            throw new Exception("Failed to submit the schema to the database: " + errorMessage);
        }

        var tableHandle = FFI.create_table(name, _handle, schemaHandle);
        if (tableHandle < 0)
        {
            var errorMessage = FFI.GetErrorMessageOnce(tableHandle);
            throw new Exception("Failed to create the table: " + errorMessage);
        }
        
        return new Table(name, tableHandle);
    }

    public Table OpenTable(string name)
    {
        if (_handle < 0)
        {
            throw new Exception("Connection is not open");
        }
        var tableHandle = FFI.open_table(name, _handle);
        if (tableHandle < 0)
        {
            var errorMessage = FFI.GetErrorMessageOnce(tableHandle);
            throw new Exception("Failed to open the table: " + errorMessage);
        }

        return new Table(name, tableHandle);
    }
    
    public void DropTable(string name, bool ignoreMissing = false)
    {
        if (_handle < 0)
        {
            throw new Exception("Connection is not open");
        }
        var status = FFI.drop_table(name, _handle);
        if (status < 0)
        {
            var errorMessage = FFI.GetErrorMessageOnce(status);
            if (ignoreMissing && errorMessage.Contains("not found"))
            {
                return;
            }
            throw new Exception("Failed to drop the table: " + errorMessage);
        }
    }
    
    public void DropDatabase()
    {
        if (_handle < 0)
        {
            throw new Exception("Connection is not open");
        }
        var status = FFI.drop_database(_handle);
        if (status < 0)
        {
            var errorMessage = FFI.GetErrorMessageOnce(status);
            throw new Exception("Failed to drop the database: " + errorMessage);
        }
    }
    
    ~Connection()
    {
        FFI.disconnect(this._handle);
    }

    // Handle Cleanup: the handle should be freed, or there will be a small resource leak.
    
    public void Dispose()
    {
        // Dispose of unmanaged resources.
        Dispose(true);
        // Suppress finalization.
        GC.SuppressFinalize(this);
    }
    
    protected virtual void Dispose(bool disposing)
    {
        if (!_connected)
        {
            return;
        }

        if (disposing)
        {
            FFI.disconnect(this._handle);
        }

        _connected = false;
        _handle = -1;
    }

    private long _handle;
    private bool _connected;
}