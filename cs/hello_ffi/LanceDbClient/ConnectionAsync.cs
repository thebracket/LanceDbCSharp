using Apache.Arrow;
using LanceDbInterface;

namespace LanceDbClient;

public sealed partial class Connection
{
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