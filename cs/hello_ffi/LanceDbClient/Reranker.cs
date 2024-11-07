using LanceDbInterface;

namespace LanceDbClient;

public class Reranker : IReranker
{
    internal Reranker()
    {
        
    }
    
    public void RerankVector(string query, Apache.Arrow.Table vectorResults)
    {
        throw new NotImplementedException();
    }

    public void RerankFts(string query, Apache.Arrow.Table ftsResults)
    {
        throw new NotImplementedException();
    }

    public void RerankHybrid(string query, Apache.Arrow.Table vectorResults, Apache.Arrow.Table ftsResults)
    {
        throw new NotImplementedException();
    }

    public void MergeResults(Apache.Arrow.Table vectorResults, Apache.Arrow.Table ftsResults)
    {
        throw new NotImplementedException();
    }

    public void RerankMultiVector(IEnumerable<Apache.Arrow.Table> vectorResults, string? query = null, bool deduplicate = false)
    {
        throw new NotImplementedException();
    }

    public void RerankMultiVector(IEnumerable<ILanceVectorQueryBuilder> vectorResults, string? query = null, bool deduplicate = false)
    {
        throw new NotImplementedException();
    }
}