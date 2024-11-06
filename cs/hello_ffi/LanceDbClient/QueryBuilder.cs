using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Apache.Arrow;
using LanceDbInterface;
using MathNet.Numerics.LinearAlgebra;

namespace LanceDbClient;

public class QueryBuilder : ILanceQueryBuilder
{
    readonly long _connectionId;
    readonly long _tableId;
    private ulong _limit;

    internal QueryBuilder(long connectionId, long tableId)
    {
        _connectionId = connectionId;
        _tableId = tableId;
        _limit = 0;
    }
    
    public ILanceQueryBuilder Limit(int limit)
    {
        _limit = (ulong)limit;
        return this;
    }

    public ILanceQueryBuilder SelectColumns(IEnumerable<string> selectColumns)
    {
        throw new NotImplementedException();
    }

    public ILanceQueryBuilder WhereClause(string whereClause, bool prefilter = false)
    {
        throw new NotImplementedException();
    }

    public ILanceQueryBuilder WithRowId(bool withRowId)
    {
        throw new NotImplementedException();
    }

    public string ExplainPlan(bool verbose = false)
    {
        throw new NotImplementedException();
    }

    public ILanceQueryBuilder Vector<T>(List<T> vector)
    {
        throw new NotImplementedException();
    }

    public ILanceQueryBuilder Vector<T>(Vector<T> vector) where T : struct, IEquatable<T>, IFormattable
    {
        throw new NotImplementedException();
    }

    public ILanceQueryBuilder Vector<T>(Matrix<T> vector) where T : struct, IEquatable<T>, IFormattable
    {
        throw new NotImplementedException();
    }

    public ILanceQueryBuilder Text(string text)
    {
        throw new NotImplementedException();
    }

    public ILanceQueryBuilder Rerank(IReranker reranker)
    {
        throw new NotImplementedException();
    }

    public Apache.Arrow.Table ToArrow()
    {
        throw new NotImplementedException();
    }

    public Task<Apache.Arrow.Table> ToArrowAsync(CancellationToken token = default)
    {
        throw new NotImplementedException();
    }

    public IEnumerable<IDictionary<string, object>> ToList()
    {
        throw new NotImplementedException();
    }

    public Task<IEnumerable<IDictionary<string, object>>> ToListAsync(CancellationToken token = default)
    {
        throw new NotImplementedException();
    }

    public unsafe IEnumerable<RecordBatch> ToBatches(int batchSize)
    {
        // TODO: We're ignoring batch size completely right now
        var result = new List<RecordBatch>();
        Exception? exception = null;
        
        Ffi.query(_connectionId, _tableId, (bytes, len) =>
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
        }, _limit);
        
        if (exception != null) throw exception;
        return result;
    }

    public IAsyncEnumerable<RecordBatch> ToBatchesAsync(int batchSize, CancellationToken token = default)
    {
        throw new NotImplementedException();
    }
}