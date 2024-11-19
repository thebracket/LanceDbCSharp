using Apache.Arrow;

namespace LanceDbInterface;

// Currently we only need RRFReranker
public interface IReranker
{
    Table RerankVector(string query, Table vectorResults);
    Table RerankFts(string query, Table ftsResults);
    Table RerankHybrid(string query, Table vectorResults, Table ftsResults);
    Table MergeResults(Table vectorResults, Table ftsResults);
    Table RerankMultiVector(IEnumerable<Table> vectorResults, string? query = null, bool deduplicate = false);
    Table RerankMultiVector(IEnumerable<ILanceVectorQueryBuilder> vectorResults, string? query = null, bool deduplicate = false);
}