using Apache.Arrow;

namespace LanceDbInterface;

// Currently we only need RRFReranker
public interface IReranker
{
    void RerankVector(string query, Table vectorResults);
    void RerankFts(string query, Table ftsResults);
    void RerankHybrid(string query, Table vectorResults, Table ftsResults);
    void MergeResults(Table vectorResults, Table ftsResults);
    void RerankMultiVector(IEnumerable<Table> vectorResults, string? query = null, bool deduplicate = false);
    void RerankMultiVector(IEnumerable<ILanceVectorQueryBuilder> vectorResults, string? query = null, bool deduplicate = false);
}