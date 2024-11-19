using Apache.Arrow;
using LanceDbInterface;

namespace LanceDbClient;

public partial class MergeInsertBuilder
{
    public Task ExecuteAsync(Apache.Arrow.Table data, CancellationToken token = default)
    {
        throw new NotImplementedException();
    }

    public Task ExecuteAsync(IEnumerable<RecordBatch> data, CancellationToken token = default)
    {
        throw new NotImplementedException();
    }
    
    public Task ExecuteAsync(IEnumerable<Dictionary<string, object>> data, CancellationToken token = default)
    {
        throw new NotImplementedException();
    }

}