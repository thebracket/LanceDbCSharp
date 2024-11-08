using System.Runtime.InteropServices;
using Apache.Arrow;
using LanceDbInterface;

namespace LanceDbClient;

public partial class Connection : IConnection
{
    // <summary>
    // Creates a new connection to the database.
    // </summary>
    // <param name="uri">The URI of the database to connect to.</param>
    // <exception cref="Exception">If the connection fails, or is already open.</exception>
    public Connection(Uri uri)
    {
        if (IsOpen) throw new Exception("Connection is already open");
        _connectionId = -1L;
        Exception? exception = null;
        Ffi.connect(uri.AbsolutePath, ((result, message) =>
        {
            _connectionId = result;
            if (result < 0 && message != null)
            {
                exception = new Exception(message);
            }
        }));
        if (exception != null) throw exception;
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
        if (!this.IsOpen) throw new Exception("Connection is not open");
        
        var strings = new List<string>();
        Exception? exception = null;
        Ffi.list_table_names(_connectionId, 
            s => strings.Add(s),
            (code, message) =>
            {
                if (code < 0 && message != null)
                {
                    exception = new Exception("Failed to list table names: " + message);
                }
            });
        if (exception != null) throw exception;
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
        if (!IsOpen) throw new Exception("Connection is not open");

        var schemaBytes = Ffi.SerializeSchemaOnly(schema);
        var tableHandle = -1L;
        Exception? exception = null;

        unsafe
        {
            fixed (byte* p = schemaBytes)
            {
                Ffi.create_empty_table(name, _connectionId, p, (ulong)schemaBytes.Length, (code, message) =>
                {
                    tableHandle = code;
                    if (message != null)
                    {
                        exception = new Exception("Failed to create the table: " + message);
                    }
                });
            }
        }
        if (exception != null) throw exception;
        
        return new Table(name, tableHandle, _connectionId, schema);
    }

    /// <summary>
    /// Opens an existing table in the database by name.
    /// </summary>
    /// <param name="name">The table name to open</param>
    /// <returns>A handle to the opened table.</returns>
    /// <exception cref="Exception">If the connection isn't open, if the table isn't found.</exception>
    public ITable OpenTable(string name)
    {
        if (!IsOpen) throw new Exception("Connection is not open");

        var tableHandle = -1L;
        Schema? schema = null;
        Exception? exception = null;
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
                        exception = new Exception("Failed to open the table: " + message);
                    }
                }
            );
        }
        if (exception != null) throw exception;
        if (schema == null) throw new Exception("Failed to open the table: schema is null");

        return new Table(name, tableHandle, _connectionId, schema);
    }
    
    /// <summary>
    /// Drops a table from the database by name.
    /// </summary>
    /// <param name="name">Table name to drop</param>
    /// <param name="ignoreMissing">Do not throw an exception if the table does not exist</param>
    /// <exception cref="Exception">If the connection is unavailable, or the table doesn't exist and you didn't specify ignoreMissing.</exception>
    public void DropTable(string name, bool ignoreMissing = false)
    {
        if (!IsOpen) throw new Exception("Connection is not open");

        Exception? exception = null;
        Ffi.drop_table(name, _connectionId, ignoreMissing, (code, message) =>
        {
            if (code < 0 && message != null)
            {
                exception = new Exception("Failed to drop the table: " + message);
            }
        });
        if (exception != null) throw exception;
    }

    public void RenameTable(string oldName, string newName)
    {
        if (!IsOpen) throw new Exception("Connection is not open");
        Exception? exception = null;
        Ffi.rename_table(_connectionId, oldName, newName, (code, message) =>
        {
            if (code < 0 && message != null)
            {
                exception = new Exception("Failed to rename the table: " + message);
            }
        });
        if (exception != null) throw exception;
    }
    
    /// <summary>
    /// Drops the entire database, including all tables.
    /// </summary>
    /// <exception cref="Exception">If the connection is not open.</exception>
    public void DropDatabase()
    {
        if (!IsOpen) throw new Exception("Connection is not open");

        Exception? exception = null;
        Ffi.drop_database(_connectionId, (code, message) =>
        {
            if (code < 0 && message != null)
            {
                exception = new Exception("Failed to drop the database: " + message);
            }
        });
        if (exception != null) throw exception;
        IsOpen = false;
    }
    
    /// <summary>
    /// Close the connection to the database.
    /// </summary>
    /// <exception cref="Exception">If the disconnect FFI call returns an error.</exception>
    public void Close()
    {
        if (!IsOpen) throw new Exception("Connection is not open");
        Exception? exception = null;
        Ffi.disconnect(this._connectionId, (code, message) =>
        {
            if (code < 0 && message != null)
            {
                exception = new Exception("Failed to disconnect: " + message);
            }
        });
        if (exception != null) throw exception;
        IsOpen = false;
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
        if (!IsOpen)
        {
            return;
        }

        if (disposing)
        {
            Exception? exception = null;
            Ffi.disconnect(this._connectionId, (code, message) =>
            {
                if (code < 0 && message != null)
                {
                    exception = new Exception("Failed to disconnect: " + message);
                }
            });
            if (exception != null) throw exception;
            this.IsOpen = false;
        }

        _connectionId = -1;
    }
    
    

    private long _connectionId;
    public bool IsOpen { get; private set; }
}