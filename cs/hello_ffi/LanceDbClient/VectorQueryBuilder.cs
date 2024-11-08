using System.Runtime.InteropServices;
using System.Security.Cryptography;
using Apache.Arrow;
using LanceDbInterface;

namespace LanceDbClient;

public class VectorQueryBuilder : QueryBuilder, ILanceVectorQueryBuilder
{
    private readonly VectorDataImpl _vectorData;
    private Metric _metric;
    private int _nProbes;
    private int _refineFactor;
    
    internal VectorQueryBuilder(long connectionId, long tableId, VectorDataImpl vectorData,
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
        string[]? selectColumns = null;
        if (_selectColumns.Count > 0)
        {
            selectColumns = _selectColumns.ToArray();
        }

        fixed (byte* b = _vectorData.Data)
        {
            Ffi.explain_vector_query(
                _connectionId,
                _tableId,
                (code, message) =>
                {
                    if (code < 0 && message != null)
                    {
                        exception = new Exception("Failed to explain plan: " + message);
                    }
                },
                _limit,
                _whereClause,
                _withRowId,
                verbose,
                (message) => { result = message; },
                _selectColumns.ToArray(),
                (ulong)_selectColumns.Count,
                (uint)_vectorData.DataType,
                b,
                (ulong)_vectorData.Data.Length,
                (ulong)_vectorData.Length,
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
    /// <returns>The queyr result</returns>
    /// <exception cref="Exception">If the query fails</exception>
    public override unsafe IEnumerable<RecordBatch> ToBatches(int batchSize)
    {
        var result = new List<RecordBatch>();
        Exception? exception = null;
        
        string[]? selectColumns = null;
        if (_selectColumns.Count > 0)
        {
            selectColumns = _selectColumns.ToArray();
        }

        fixed (byte* b = _vectorData.Data)
        {

            Ffi.vector_query(_connectionId, _tableId, (bytes, len) =>
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
                }, _limit, _whereClause, _withRowId, selectColumns, (ulong)_selectColumns.Count,
                (uint)_vectorData.DataType, b, (ulong)_vectorData.Data.Length, _vectorData.Length,
                (uint)_metric, (ulong)_nProbes, (uint)_refineFactor, (uint)batchSize);
        }

        if (exception != null) throw exception;
        return result;
    }
}