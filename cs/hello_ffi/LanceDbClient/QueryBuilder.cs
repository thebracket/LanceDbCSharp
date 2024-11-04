using Apache.Arrow;
using LanceDbInterface;
using MathNet.Numerics.LinearAlgebra;

namespace LanceDbClient;

public class QueryBuilder : ILanceQueryBuilder
{
    long _connectionId;
    long _tableId;
    
    internal QueryBuilder(long connectionId, long tableId)
    {
        _connectionId = connectionId;
        _tableId = tableId;
    }
    
    public ILanceQueryBuilder Limit(int limit)
    {
        throw new NotImplementedException();
    }

    public ILanceQueryBuilder SelectColumns(IEnumerable<string> selectColumns)
    {
        throw new NotImplementedException();
    }

    public ILanceQueryBuilder WhereClause(string whereClause, bool prefilter = false)
    {
        throw new NotImplementedException();
    }

    public ILanceQueryBuilder WithRowId(bool withRowId)
    {
        throw new NotImplementedException();
    }

    public string ExplainPlan(bool verbose = false)
    {
        throw new NotImplementedException();
    }

    public ILanceQueryBuilder Vector<T>(List<T> vector)
    {
        throw new NotImplementedException();
    }

    public ILanceQueryBuilder Vector<T>(Vector<T> vector) where T : struct, IEquatable<T>, IFormattable
    {
        throw new NotImplementedException();
    }

    public ILanceQueryBuilder Vector<T>(Matrix<T> vector) where T : struct, IEquatable<T>, IFormattable
    {
        throw new NotImplementedException();
    }

    public ILanceQueryBuilder Text(string text)
    {
        throw new NotImplementedException();
    }

    public ILanceQueryBuilder Rerank(IReranker reranker)
    {
        throw new NotImplementedException();
    }

    public Apache.Arrow.Table ToArrow()
    {
        throw new NotImplementedException();
    }

    public Task<Apache.Arrow.Table> ToArrowAsync(CancellationToken token = default)
    {
        throw new NotImplementedException();
    }

    public IEnumerable<IDictionary<string, object>> ToList()
    {
        throw new NotImplementedException();
    }

    public Task<IEnumerable<IDictionary<string, object>>> ToListAsync(CancellationToken token = default)
    {
        throw new NotImplementedException();
    }

    public IEnumerable<RecordBatch> ToBatches(int batchSize)
    {
        throw new NotImplementedException();
    }

    public IAsyncEnumerable<RecordBatch> ToBatchesAsync(int batchSize, CancellationToken token = default)
    {
        throw new NotImplementedException();
    }
}