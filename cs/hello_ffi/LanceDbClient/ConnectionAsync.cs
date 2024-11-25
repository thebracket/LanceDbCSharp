using System.Runtime.InteropServices;
using Apache.Arrow;
using LanceDbInterface;

namespace LanceDbClient;

public sealed partial class Connection
{
    public Task<IEnumerable<string>> TableNamesAsync(CancellationToken cancellationToken = default)
    {
        var tcs = new TaskCompletionSource<IEnumerable<string>>();
        var strings = new List<string>();
        Ffi.ResultCallback callback = (code, message) =>
        {
            if (code < 0)
            {
                tcs.SetException(new Exception(message));
            }
            else
            {
                tcs.SetResult(strings);
            }
        };
        Ffi.StringCallback stringCallback = (s) =>
        {
            strings.Add(s);
        };
        Task.Run(() =>
        {
            Ffi.list_table_names(_connectionId, stringCallback, callback);
        }, cancellationToken);
        return tcs.Task;
    }

    public Task<ITable> CreateTableAsync(string name, Schema schema, CancellationToken cancellationToken = default)
    {
        var tcs = new TaskCompletionSource<ITable>();
        
        var schemaBytes = Ffi.SerializeSchemaOnly(schema);
        
        Ffi.ResultCallback callback = (code, message) =>
        {
            if (code < 0)
            {
                tcs.SetException(new Exception(message));
            }
            else
            {
                tcs.SetResult(new Table(name, code, _connectionId, schema));
            }
        };
        
        Task.Run(() =>
        {
            unsafe
            {
                fixed (byte* p = schemaBytes)
                {
                    Ffi.create_empty_table(name, _connectionId, p, (ulong)schemaBytes.Length, callback);
                }
            }
        });
        return tcs.Task;
    }

    public Task<ITable> OpenTableAsync(string name, CancellationToken cancellationToken = default)
    {
        var tcs = new TaskCompletionSource<ITable>();

        unsafe
        {
            Schema? schema = null;
            Ffi.ResultCallback callback = (code, message) =>
            {
                if (code < 0)
                {
                    tcs.SetException(new Exception(message));
                }
                else
                {
                    tcs.SetResult(new Table(name, code, _connectionId, schema));
                }
            };
            Task.Run(() =>
            {
                Ffi.BlobCallback schemaBlob = (bytes, len) =>
                {
                    // Convert byte pointer and length to byte[]
                    var schemaBytes = new byte[len];
                    Marshal.Copy((IntPtr)bytes, schemaBytes, 0, (int)len);
                    schema = Ffi.DeserializeSchema(schemaBytes);
                };
                Ffi.open_table(name, _connectionId, schemaBlob, callback);
            });
            return tcs.Task;
        }
    }

    public Task DropTableAsync(string name, bool ignoreMissing = false, CancellationToken cancellationToken = default)
    {
        var tcs = new TaskCompletionSource();
        Ffi.ResultCallback callback = (code, message) =>
        {
            if (code < 0)
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
            Ffi.drop_table(name, _connectionId, ignoreMissing, callback);
        }, cancellationToken);
        return tcs.Task;
    }

    public Task DropDatabaseAsync(CancellationToken cancellationToken = default)
    {
        var tcs = new TaskCompletionSource();
        Ffi.ResultCallback callback = (code, message) =>
        {
            if (code < 0)
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
            Ffi.drop_database(_connectionId, callback);
            IsOpen = false;
        }, cancellationToken);
        return tcs.Task;
    }

    public Task CloseAsync(CancellationToken cancellationToken = default)
    {
        var tcs = new TaskCompletionSource();
        Ffi.ResultCallback callback = (code, message) =>
        {
            if (code < 0)
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
            Ffi.disconnect(this._connectionId, callback);
            IsOpen = false;
        }, cancellationToken);
        return tcs.Task;
    }

}