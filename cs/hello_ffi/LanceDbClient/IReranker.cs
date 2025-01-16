namespace LanceDbClient;

// Currently we only need RRFReranker
public interface IReranker
{
    Apache.Arrow.Table RerankVector(string query, Apache.Arrow.Table vectorResults);
    Apache.Arrow.Table RerankFts(string query, Apache.Arrow.Table ftsResults);
    Apache.Arrow.Table RerankHybrid(string query, Apache.Arrow.Table vectorResults, Apache.Arrow.Table ftsResults, int limit = 0);
    Apache.Arrow.Table MergeResults(Apache.Arrow.Table vectorResults, Apache.Arrow.Table ftsResults);
    Apache.Arrow.Table RerankMultiVector(IEnumerable<Apache.Arrow.Table> vectorResults, string? query = null, bool deduplicate = false);
    Apache.Arrow.Table RerankMultiVector(IEnumerable<ILanceVectorQueryBuilder> vectorResults, string? query = null, bool deduplicate = false);
}