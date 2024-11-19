using LanceDbInterface;

namespace LanceDbClient;

public class RRFReranker : IReranker
{
    public Apache.Arrow.Table RerankVector(string query, Apache.Arrow.Table vectorResults)
    {
        throw new NotImplementedException();
    }

    public Apache.Arrow.Table RerankFts(string query, Apache.Arrow.Table ftsResults)
    {
        throw new NotImplementedException();
    }

    public Apache.Arrow.Table RerankHybrid(string query, Apache.Arrow.Table vectorResults, Apache.Arrow.Table ftsResults)
    {
        throw new NotImplementedException();
    }

    public Apache.Arrow.Table MergeResults(Apache.Arrow.Table vectorResults, Apache.Arrow.Table ftsResults)
    {
        throw new NotImplementedException();
    }

    public Apache.Arrow.Table RerankMultiVector(IEnumerable<Apache.Arrow.Table> vectorResults, string? query = null, bool deduplicate = false)
    {
        throw new NotImplementedException();
    }

    public Apache.Arrow.Table RerankMultiVector(IEnumerable<ILanceVectorQueryBuilder> vectorResults, string? query = null, bool deduplicate = false)
    {
        throw new NotImplementedException();
    }
}