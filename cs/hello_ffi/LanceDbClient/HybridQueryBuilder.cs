using Apache.Arrow;

namespace LanceDbClient;

public class HybridQueryBuilder : VectorQueryBuilder, ILanceHybridQueryBuilder
{
    internal HybridQueryBuilder(long connectionId, long tableId) : base(connectionId, tableId)
    {
        // Defaults
        DistanceMetric = LanceDbClient.Metric.L2;
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
            .NProbes(NumProbes)
            .RefineFactor(RefinementFactor)
            .Metric(DistanceMetric)
            .SelectColumns(SelectColumnsList)
            .WithRowId(true)
            .Limit(LimitCount > 1 ? (int)LimitCount : 0)
            .ToArrow();
        var ftsQuery = new QueryBuilder(ConnectionId, TableId)
            .Text(FullTextSearch)
            .SelectColumns(SelectColumnsList)
            .WithRowId(true)
            .Limit(LimitCount > 1 ? (int)LimitCount : 0)
            .ToArrow();
        
        // Perform the re-ranking
        var reranked = Reranker.RerankHybrid(FullTextSearch, vectorQuery, ftsQuery, (int)LimitCount);
        
        // Convert reranked into a RecordBatch
        var batches = ArrayHelpers.ArrowTableToRecordBatch(reranked);
        return batches;
    }

    public override async IAsyncEnumerable<RecordBatch> ToBatchesAsync(int batchSize, CancellationToken token = default)
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
        var vectorQuery = await new VectorQueryBuilder(this)
            .WithVectorData(VectorData)
            .NProbes(NumProbes)
            .RefineFactor(RefinementFactor)
            .Metric(DistanceMetric)
            .SelectColumns(SelectColumnsList)
            .WithRowId(true)
            .Limit(LimitCount > 1 ? (int)LimitCount : 0)
            .ToArrowAsync(token);
        var ftsQuery = await new QueryBuilder(ConnectionId, TableId)
            .Text(FullTextSearch)
            .SelectColumns(SelectColumnsList)
            .WithRowId(true)
            .Limit(LimitCount > 1 ? (int)LimitCount : 0)
            .ToArrowAsync(token);
        
        // Perform the re-ranking
        var reranked = Reranker.RerankHybrid(FullTextSearch, vectorQuery, ftsQuery);
        
        // Convert reranked into a RecordBatch
        var batches = ArrayHelpers.ArrowTableToRecordBatch(reranked);
        foreach (var batch in batches)
        {
            yield return batch;
        }
    }

    public new ILanceHybridQueryBuilder Metric(Metric metric = LanceDbClient.Metric.L2)
    {
        DistanceMetric = metric;
        return this;
    }

    public new ILanceHybridQueryBuilder NProbes(int nProbes)
    {
        NumProbes = nProbes;
        return this;
    }

    public new ILanceHybridQueryBuilder RefineFactor(int refineFactor)
    {
        RefinementFactor = refineFactor;
        return this;
    }
}