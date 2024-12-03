using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading.Channels;
using Apache.Arrow;
using LanceDbInterface;

namespace LanceDbClient;

public class VectorQueryBuilder : QueryBuilder, ILanceVectorQueryBuilder
{
    protected internal ArrayHelpers.VectorDataImpl? VectorData = null;
    protected Metric DistanceMetric;
    protected int NumProbes;
    protected int RefinementFactor;

    internal VectorQueryBuilder(long connectionId, long tableId) : base(connectionId, tableId)
    {
        DistanceMetric = LanceDbInterface.Metric.L2;
        NumProbes = 1;
        RefinementFactor = 1;
    }
    
    internal VectorQueryBuilder(QueryBuilder parent) : base(parent.ConnectionId, parent.TableId)
    {
        LimitCount = parent.LimitCount;
        WhereSql = parent.WhereSql;
        WithRowIdent = parent.WithRowIdent;
        SelectColumnsList = parent.SelectColumnsList;
        FullTextSearch = parent.FullTextSearch;
        Reranker = parent.Reranker;
        
        // Defaults
        DistanceMetric = LanceDbInterface.Metric.L2;
        NumProbes = 0;
        RefinementFactor = 0;
    }
    
    internal VectorQueryBuilder WithVectorData(ArrayHelpers.VectorDataImpl vectorData)
    {
        return new VectorQueryBuilder(this)
        {
            VectorData = vectorData
        };
    }
    
    public ILanceVectorQueryBuilder Metric(Metric metric = LanceDbInterface.Metric.L2)
    {
        DistanceMetric = metric;
        return this;
    }

    public ILanceVectorQueryBuilder NProbes(int nProbes)
    {
        NumProbes = nProbes;
        return this;
    }
    
    public ILanceVectorQueryBuilder RefineFactor(int refineFactor)
    {
        RefinementFactor = refineFactor;
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
        if (VectorData == null)
        {
            throw new Exception("VectorData must be set before calling ToBatches");
        }
        Exception? exception = null;
        string? result = null;

        fixed (byte* b = VectorData.Data)
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
                (uint)VectorData.DataType,
                b,
                (ulong)VectorData.Data.Length,
                VectorData.Length,
                (uint)DistanceMetric,
                (ulong)NumProbes,
                (uint)RefinementFactor
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
        if (VectorData == null)
        {
            throw new Exception("VectorData must be set before calling ToBatches");
        }
        var result = new List<RecordBatch>();
        Exception? exception = null;
        
        string[]? selectColumns = null;
        if (SelectColumnsList.Count > 0)
        {
            selectColumns = SelectColumnsList.ToArray();
        }

        fixed (byte* b = VectorData.Data)
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
                (uint)VectorData.DataType, b, (ulong)VectorData.Data.Length, VectorData.Length,
                (uint)DistanceMetric, (ulong)NumProbes, (uint)RefinementFactor, (uint)batchSize);
        }

        if (exception != null) throw exception;
        return result;
    }

    public new async IAsyncEnumerable<RecordBatch> ToBatchesAsync(int batchSize, [EnumeratorCancellation] CancellationToken token = default)
    {
        if (VectorData == null)
        {
            throw new Exception("VectorData must be set before calling ToBatches");
        }
        var result = new List<RecordBatch>();
        Exception? exception = null;
        
        string[]? selectColumns = null;
        if (SelectColumnsList.Count > 0)
        {
            selectColumns = SelectColumnsList.ToArray();
        }
        
        var channel = Channel.CreateUnbounded<RecordBatch>();
        
        Ffi.ResultCallback resultCallback = (code, message) =>
        {
            // If an error occurred, turn it into an exception
            if (code < 0 && message != null)
            {
                throw new Exception("Failed to compact files: " + message);
            }
        };
        
        _ = Task.Run(() =>
        {
            unsafe
            {
                Ffi.BlobCallback blobCallback = (bytes, len) =>
                {
                    // Marshall schema/length into a managed object
                    var schemaBytes = new byte[len];
                    Marshal.Copy((IntPtr)bytes, schemaBytes, 0, (int)len);
                    var batch = Ffi.DeserializeRecordBatch(schemaBytes);
                    channel.Writer.TryWrite(batch);
                };

                fixed (byte* b = VectorData.Data)
                {
                    Ffi.vector_query(ConnectionId, TableId, blobCallback, resultCallback,
                        LimitCount, WhereSql, WithRowIdent, selectColumns!, (ulong)SelectColumnsList.Count,
                        (uint)VectorData.DataType, b, (ulong)VectorData.Data.Length, VectorData.Length,
                        (uint)DistanceMetric, (ulong)NumProbes, (uint)RefinementFactor, (uint)batchSize);
                    channel.Writer.Complete();
                }
            }


        }, token);

        while (await channel.Reader.WaitToReadAsync(token))
        {
            while (channel.Reader.TryRead(out var batch))
            {
                yield return batch;
            }
        }
    }
}