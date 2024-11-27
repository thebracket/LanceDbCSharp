using Apache.Arrow;
using LanceDbInterface;

namespace LanceDbClient;

public class HybridQueryBuilder : VectorQueryBuilder, ILanceHybridQueryBuilder
{
    internal HybridQueryBuilder(long connectionId, long tableId) : base(connectionId, tableId)
    {
        // Defaults
        DistanceMetric = LanceDbInterface.Metric.L2;
        NumProbes = 0;
        RefinementFactor = 0;
    }
    
    internal new HybridQueryBuilder WithVectorData(ArrayHelpers.VectorDataImpl vectorData)
    {
        VectorData = vectorData;
        return this;
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
        var vectorQuery = new VectorQueryBuilder(this)
            .WithVectorData(VectorData)
            .SelectColumns(SelectColumnsList)
            .WithRowId(true)
            .ToArrow();
        var ftsQuery = new QueryBuilder(ConnectionId, TableId)
            .WithRowId(true)
            .Text(FullTextSearch)
            .ToArrow();
        
        // Perform the re-ranking
        var reranked = Reranker.RerankHybrid(FullTextSearch, vectorQuery, ftsQuery);
        
        // Convert reranked into a RecordBatch
        var batches = ArrayHelpers.ArrowTableToRecordBatch(reranked);
        return batches;
    }

    public new async IAsyncEnumerable<RecordBatch> ToBatchesAsync(int batchSize, CancellationToken token = default)
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
        var vectorQuery = await new VectorQueryBuilder(this).WithVectorData(VectorData).ToArrowAsync();
        var ftsQuery = await new QueryBuilder(ConnectionId, TableId).Text(FullTextSearch).ToArrowAsync();
        
        // Perform the re-ranking
        var reranked = Reranker.RerankHybrid(FullTextSearch, vectorQuery, ftsQuery);
        
        // Convert reranked into a RecordBatch
        var batches = ArrayHelpers.ArrowTableToRecordBatch(reranked);
        foreach (var batch in batches)
        {
            yield return batch;
        }
    }
}