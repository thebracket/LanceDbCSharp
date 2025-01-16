namespace LanceDbClient
{
    public interface ILanceDb
    {
        IConnection Connect(Uri uri, TimeSpan? readConsistencyInterval = null);
        Task<IConnection> ConnectAsync(Uri uri, TimeSpan? readConsistencyInterval = null, CancellationToken token = default);
    }

}
