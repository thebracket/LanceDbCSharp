using Apache.Arrow;
using MathNet.Numerics.LinearAlgebra;

namespace LanceDbInterface
{
    public interface ILanceQueryBuilder
    {
        ILanceQueryBuilder Limit(int limit);
        ILanceQueryBuilder SelectColumns(IEnumerable<string> selectColumns);
        ILanceQueryBuilder WhereClause(string whereClause, bool prefilter = false);
        ILanceQueryBuilder WithRowId(bool withRowId);
        string ExplainPlan(bool verbose = false);
        ILanceVectorQueryBuilder Vector<T>(List<T> vector);
        
        ILanceVectorQueryBuilder Vector<T>(Vector<T> vector) where T : struct, IEquatable<T>, IFormattable;
        ILanceVectorQueryBuilder Vector<T>(Matrix<T> vector) where T : struct, IEquatable<T>, IFormattable;
        
        ILanceQueryBuilder Text(string text);
        ILanceQueryBuilder Rerank(IReranker reranker);
        
        Apache.Arrow.Table ToArrow();
        Task<Apache.Arrow.Table> ToArrowAsync(CancellationToken token = default);
        IEnumerable<IDictionary<string, object>> ToList();
        Task<IEnumerable<IDictionary<string, object>>> ToListAsync(CancellationToken token = default);
        
        IEnumerable<RecordBatch> ToBatches(int batchSize); 
        IAsyncEnumerable<RecordBatch> ToBatchesAsync(int batchSize, CancellationToken token = default); 
    }
}
