using System.Runtime.InteropServices;
using Apache.Arrow;
using LanceDbInterface;

namespace LanceDbClient;

public partial class Connection : IConnection
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
        throw new NotImplementedException();
    }

    public Task DropDatabaseAsync(CancellationToken cancellationToken = default)
    {
        throw new NotImplementedException();
    }

    public Task CloseAsync(CancellationToken cancellationToken = default)
    {
        throw new NotImplementedException();
    }

}