using System.Runtime.InteropServices;
using Apache.Arrow;
using LanceDbInterface;

namespace LanceDbClient;

public class VectorQueryBuilder : QueryBuilder, ILanceVectorQueryBuilder
{
    private ArrayHelpers.VectorDataImpl _vectorData;
    private Metric _metric;
    private int _nProbes;
    private int _refineFactor;

    internal VectorQueryBuilder(long connectionId, long tableId) : base(connectionId, tableId)
    {
        _metric = LanceDbInterface.Metric.L2;
        _nProbes = 1;
        _refineFactor = 1;
    }
    
    internal VectorQueryBuilder(QueryBuilder parent) : base(parent.ConnectionId, parent.TableId)
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
    
    internal VectorQueryBuilder WithVectorData(ArrayHelpers.VectorDataImpl vectorData)
    {
        return new VectorQueryBuilder(this)
        {
            _vectorData = vectorData
        };
    }
    
    /*internal VectorQueryBuilder(long connectionId, long tableId, VectorDataImpl vectorData,
        ulong limit, string? whereClause, bool withRowId, List<string> selectColumns) 
        : base(connectionId, tableId)
    {
        _vectorData = vectorData;
        _limit = limit;
        _whereClause = whereClause;
        _withRowId = withRowId;
        _selectColumns = selectColumns;
        _metric = LanceDbInterface.Metric.L2;
        _nProbes = 1;
        _refineFactor = 1;
    }
    
    internal VectorQueryBuilder(long connectionId, long tableId,
        ulong limit, string? whereClause, bool withRowId, List<string> selectColumns) 
        : base(connectionId, tableId)
    {
        _limit = limit;
        _whereClause = whereClause;
        _withRowId = withRowId;
        _selectColumns = selectColumns;
        _metric = LanceDbInterface.Metric.L2;
        _nProbes = 1;
        _refineFactor = 1;
    }*/
    
    public ILanceVectorQueryBuilder Metric(Metric metric = LanceDbInterface.Metric.L2)
    {
        _metric = metric;
        return this;
    }

    public ILanceVectorQueryBuilder NProbes(int nProbes)
    {
        _nProbes = nProbes;
        return this;
    }

    public ILanceVectorQueryBuilder RefineFactor(int refineFactor)
    {
        _refineFactor = refineFactor;
        return this;
    }
    
    /// <summary>
    /// Submit the query and retrieve the query plan from the LanceDb engine.
    /// </summary>
    /// <param name="verbose">Verbose provides more information</param>
    /// <returns>A string containing the query execution plan.</returns>
    /// <exception cref="Exception">If the call fails.</exception>
    public override unsafe string ExplainPlan(bool verbose = false)
    {
        Exception? exception = null;
        string? result = null;

        fixed (byte* b = _vectorData.Data)
        {
            Ffi.explain_vector_query(
                ConnectionId,
                TableId,
                (code, message) =>
                {
                    if (code < 0 && message != null)
                    {
                        exception = new Exception("Failed to explain plan: " + message);
                    }
                },
                LimitCount,
                WhereSql,
                WithRowIdent,
                verbose,
                (message) => { result = message; },
                SelectColumnsList.ToArray(),
                (ulong)SelectColumnsList.Count,
                (uint)_vectorData.DataType,
                b,
                (ulong)_vectorData.Data.Length,
                _vectorData.Length,
                (uint)_metric,
                (ulong)_nProbes,
                (uint)_refineFactor
            );
        }

        if (exception != null) throw exception;
        return result ??= "No explanation returned";
    }
    
    /// <summary>
    /// Perform the query and return the results as a list of RecordBatch objects.
    /// </summary>
    /// <param name="batchSize">Not implemented yet.</param>
    /// <returns>The query result</returns>
    /// <exception cref="Exception">If the query fails</exception>
    public override unsafe IEnumerable<RecordBatch> ToBatches(int batchSize)
    {
        var result = new List<RecordBatch>();
        Exception? exception = null;
        
        string[]? selectColumns = null;
        if (SelectColumnsList.Count > 0)
        {
            selectColumns = SelectColumnsList.ToArray();
        }

        fixed (byte* b = _vectorData.Data)
        {

            Ffi.vector_query(ConnectionId, TableId, (bytes, len) =>
                {
                    // Marshall schema/length into a managed object
                    var schemaBytes = new byte[len];
                    Marshal.Copy((IntPtr)bytes, schemaBytes, 0, (int)len);
                    var batch = Ffi.DeserializeRecordBatch(schemaBytes);
                    result.Add(batch);
                }, (code, message) =>
                {
                    // If an error occurred, turn it into an exception
                    if (code < 0 && message != null)
                    {
                        exception = new Exception("Failed to compact files: " + message);
                    }
                }, LimitCount, WhereSql, WithRowIdent, selectColumns!, (ulong)SelectColumnsList.Count,
                (uint)_vectorData.DataType, b, (ulong)_vectorData.Data.Length, _vectorData.Length,
                (uint)_metric, (ulong)_nProbes, (uint)_refineFactor, (uint)batchSize);
        }

        if (exception != null) throw exception;
        return result;
    }
}