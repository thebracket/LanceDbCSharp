using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Apache.Arrow;
using LanceDbInterface;
using MathNet.Numerics.LinearAlgebra;

namespace LanceDbClient;

public partial class QueryBuilder : ILanceQueryBuilder
{
    public Task<Apache.Arrow.Table> ToArrowAsync(CancellationToken token = default)
    {
        throw new NotImplementedException();
    }

    public Task<IEnumerable<IDictionary<string, object>>> ToListAsync(CancellationToken token = default)
    {
        throw new NotImplementedException();
    }
    
    public IAsyncEnumerable<RecordBatch> ToBatchesAsync(int batchSize, CancellationToken token = default)
    {
        throw new NotImplementedException();
    }

}