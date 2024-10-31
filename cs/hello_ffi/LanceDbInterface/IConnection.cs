using Apache.Arrow;

namespace LanceDbInterface
{
    public interface IConnection : IDisposable
    {
        IEnumerable<string> TableNames();
        Task<IEnumerable<string>> TableNamesAsync(CancellationToken cancellationToken = default);

        ITable CreateTable(string name, Schema schema);
        Task<ITable> CreateTableAsync(string name, Schema schema, CancellationToken cancellationToken = default);

        ITable OpenTable(string name);
        Task<ITable> OpenTableAsync(string name, CancellationToken cancellationToken = default);

        void DropTable(string name, bool ignoreMissing = false);
        Task DropTableAsync(string name, bool ignoreMissing = false, CancellationToken cancellationToken = default);

        void DropDatabase();
        Task DropDatabaseAsync(CancellationToken cancellationToken = default);

        void Close();
        Task CloseAsync(CancellationToken cancellationToken = default);

        bool IsOpen { get;  }

        Uri Uri { get; }
    }
}
