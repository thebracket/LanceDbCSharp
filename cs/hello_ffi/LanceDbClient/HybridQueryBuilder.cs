using LanceDbInterface;

namespace LanceDbClient;

public class HybridQueryBuilder : QueryBuilder, ILanceQueryBuilder
{
    private Metric _metric;
    private int _nProbes;
    private int _refineFactor;
    private ArrayHelpers.VectorDataImpl _vectorData;
    
    internal HybridQueryBuilder(long connectionId, long tableId) : base(connectionId, tableId)
    {
        // Defaults
        _metric = LanceDbInterface.Metric.L2;
        _nProbes = 1;
        _refineFactor = 1;
    }
    
    internal HybridQueryBuilder(QueryBuilder parent) : base(parent.ConnectionId, parent.TableId)
    {
        LimitCount = parent.LimitCount;
        WhereSql = parent.WhereSql;
        WithRowIdent = parent.WithRowIdent;
        SelectColumnsList = parent.SelectColumnsList;
        FullTextSearch = parent.FullTextSearch;
        
        // Defaults
        _metric = LanceDbInterface.Metric.L2;
        _nProbes = 1;
        _refineFactor = 1;
    }
    
    internal HybridQueryBuilder WithVectorData(ArrayHelpers.VectorDataImpl vectorData)
    {
        return new HybridQueryBuilder(this)
        {
            _vectorData = vectorData
        };
    }
}