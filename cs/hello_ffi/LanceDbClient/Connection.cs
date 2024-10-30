using System.Runtime.InteropServices;
using Apache.Arrow;
using LanceDbInterface;

namespace LanceDbClient;

public class Connection : IConnection, IDisposable
{
    // <summary>
    // Creates a new connection to the database.
    // </summary>
    // <param name="uri">The URI of the database to connect to.</param>
    // <exception cref="Exception">If the connection fails.</exception>
    public Connection(Uri uri)
    {
        _connectionId = -1L;
        Ffi.connect(uri.AbsolutePath, ((result, message) =>
        {
            _connectionId = result;
            if (message != null)
            {
                throw new Exception(message);
            }
        }));
        _connected = true;
        Uri = uri;
        IsOpen = true;
    }

    /// <summary>
    /// Lists the names of the tables in the database.
    /// </summary>
    /// <returns>A list of table names</returns>
    /// <exception cref="Exception">If the connection is not open, or table names are unavailable.</exception>
    public IEnumerable<string> TableNames()
    {
        if (_connectionId < 0)
        {
            IsOpen = false;
            throw new Exception("Connection is not open");
        }
        var strings = new List<string>();
        Ffi.list_table_names(_connectionId, 
            s => strings.Add(s),
            (code, message) =>
            {
                if (code < 0 && message != null)
                {
                    throw new Exception("Failed to list table names: " + message);
                }
            });
        return strings;
    }

    /// <summary>
    /// Creates a new table in the database. The table will have the provided schema, but no data.
    /// </summary>
    /// <param name="name">The desired table name.</param>
    /// <param name="schema">The schema, which must be a valid Apache Arrow schema (no validation is performed beyond serialization/deserialization working)</param>
    /// <returns>A handle to the newly created table</returns>
    /// <exception cref="Exception"></exception>
    public ITable CreateTable(string name, Schema schema)
    {
        if (_connectionId < 0)
        {
            throw new Exception("Connection is not open");
        }

        var schemaBytes = Ffi.SerializeSchemaOnly(schema);
        var tableHandle = -1L;

        unsafe
        {
            fixed (byte* p = schemaBytes)
            {
                Ffi.create_empty_table(name, _connectionId, p, (ulong)schemaBytes.Length, (code, message) =>
                {
                    tableHandle = code;
                    if (message != null)
                    {
                        throw new Exception("Failed to create the table: " + message);
                    }
                });
            }
        }
        
        return new Table(name, tableHandle, _connectionId, schema);
    }

    public ITable OpenTable(string name)
    {
        if (_connectionId < 0)
        {
            throw new Exception("Connection is not open");
        }

        var tableHandle = -1L;
        Schema? schema = null;
        unsafe
        {
            Ffi.open_table(name, _connectionId,
                (bytes, len) =>
                {
                    // Convert byte pointer and length to byte[]
                    var schemaBytes = new byte[len];
                    Marshal.Copy((IntPtr)bytes, schemaBytes, 0, (int)len);
                    schema = Ffi.DeserializeSchema(schemaBytes);
                }
                , (code, message) =>
                {
                    tableHandle = code;
                    if (message != null)
                    {
                        throw new Exception("Failed to open the table: " + message);
                    }
                }
            );
        }
        if (schema == null)
        {
            throw new Exception("Failed to open the table: schema is null");
        }

        return new Table(name, tableHandle, _connectionId, schema);
    }
    
    public void DropTable(string name, bool ignoreMissing = false)
    {
        if (_connectionId < 0)
        {
            throw new Exception("Connection is not open");
        }

        Ffi.drop_table(name, _connectionId, (code, message) =>
        {
            if (code < 0 && message != null)
            {
                throw new Exception("Failed to drop the table: " + message);
            }
        });
    }
    
    public void DropDatabase()
    {
        if (_connectionId < 0)
        {
            throw new Exception("Connection is not open");
        }

        Ffi.drop_database(_connectionId, (code, message) =>
        {
            if (code < 0 && message != null)
            {
                throw new Exception("Failed to drop the database: " + message);
            }
        });
    }

    public Task<IEnumerable<string>> TableNamesAsync(CancellationToken cancellationToken = default)
    {
        throw new NotImplementedException();
    }

    public Task<ITable> CreateTableAsync(string name, Schema schema, CancellationToken cancellationToken = default)
    {
        throw new NotImplementedException();
    }

    public Task<ITable> OpenTableAsync(string name, CancellationToken cancellationToken = default)
    {
        throw new NotImplementedException();
    }

    public Task DropTableAsync(string name, bool ignoreMissing = false, CancellationToken cancellationToken = default)
    {
        throw new NotImplementedException();
    }

    public Task DropDatabaseAsync(CancellationToken cancellationToken = default)
    {
        throw new NotImplementedException();
    }

    public void Close()
    {
        Ffi.disconnect(this._connectionId, (code, message) =>
        {
            if (code < 0 && message != null)
            {
                throw new Exception("Failed to disconnect: " + message);
            }
        });
        IsOpen = false;
    }

    public Task CloseAsync(CancellationToken cancellationToken = default)
    {
        throw new NotImplementedException();
    }

    public Uri Uri { get; }

    ~Connection()
    {
        Dispose(true);
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
            Ffi.disconnect(this._connectionId, (code, message) =>
            {
                if (code < 0 && message != null)
                {
                    throw new Exception("Failed to disconnect: " + message);
                }
            });
            this.IsOpen = false;
        }

        _connected = false;
        _connectionId = -1;
    }
    
    

    private long _connectionId;
    private bool _connected;
    public bool IsOpen { get; private set; }
}