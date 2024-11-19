using Apache.Arrow;
using LanceDbInterface;

namespace LanceDbClient;

public class HybridQueryBuilder : VectorQueryBuilder, ILanceHybridQueryBuilder
{
    internal HybridQueryBuilder(long connectionId, long tableId) : base(connectionId, tableId)
    {
        // Defaults
        DistanceMetric = LanceDbInterface.Metric.L2;
        NumProbes = 1;
        RefinementFactor = 1;
    }

    public override IEnumerable<RecordBatch> ToBatches(int batchSize)
    {
        // A Hybrid query runs both a vector and a full-text query. We have to make sure that both are set.
        if (FullTextSearch == null || VectorData == null)
        {
            throw new Exception("FullTextSearch and VectorData must be set before calling ToBatches");
        }
        if (Reranker == null)
        {
            throw new Exception("Reranker must be set before calling ToBatches");
        }
        
        // Run the vector query and FTS query
        var vectorQuery = new VectorQueryBuilder(this).WithVectorData(VectorData).ToArrow();
        var ftsQuery = new QueryBuilder(ConnectionId, TableId).Text(FullTextSearch).ToArrow();
        
        // Perform the re-ranking
        var reranked = Reranker.RerankHybrid(FullTextSearch, vectorQuery, ftsQuery);
        
        // Convert reranked into a RecordBatch
        var batches = ArrayHelpers.ArrowTableToRecordBatch(reranked);
        return batches;
    }
}