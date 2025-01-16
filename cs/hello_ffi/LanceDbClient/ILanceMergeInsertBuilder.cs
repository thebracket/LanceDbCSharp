namespace LanceDbClient
{
    public interface ILanceMergeInsertBuilder
    {
        ILanceMergeInsertBuilder WhenMatchedUpdateAll(string? where = null);
        ILanceMergeInsertBuilder WhenNotMatchedInsertAll();
        ILanceMergeInsertBuilder WhenNotMatchedBySourceDelete(string? condition = null);

        void Execute(Apache.Arrow.Table data);
        Task ExecuteAsync(Apache.Arrow.Table data, CancellationToken token = default);

        void Execute(IEnumerable<Apache.Arrow.RecordBatch> data);
        Task ExecuteAsync(IEnumerable<Apache.Arrow.RecordBatch> data, CancellationToken token = default);

        void Execute(IEnumerable<Dictionary<string, object>> data);
        Task ExecuteAsync(IEnumerable<Dictionary<string, object>> data, CancellationToken token = default);
    }
}
