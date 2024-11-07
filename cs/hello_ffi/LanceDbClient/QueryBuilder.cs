using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Apache.Arrow;
using LanceDbInterface;
using MathNet.Numerics.LinearAlgebra;

namespace LanceDbClient;

public class QueryBuilder : ILanceQueryBuilder
{
    private readonly long _connectionId;
    private readonly long _tableId;
    private ulong _limit;
    private string? _whereClause;
    private bool _withRowId;
    private List<string> _selectColumns;
    private string? _fullTextSearch;

    internal QueryBuilder(long connectionId, long tableId)
    {
        _connectionId = connectionId;
        _tableId = tableId;
        _limit = 0;
        _whereClause = null;
        _withRowId = false;
        _selectColumns = [];
        _fullTextSearch = null;
    }
    
    public ILanceQueryBuilder Limit(int limit)
    {
        _limit = (ulong)limit;
        return this;
    }

    public ILanceQueryBuilder SelectColumns(IEnumerable<string> selectColumns)
    {
        _selectColumns = selectColumns.ToList();
        return this;
    }

    public ILanceQueryBuilder WhereClause(string whereClause, bool prefilter = false)
    {
        _whereClause = whereClause;
        return this;
    }

    public ILanceQueryBuilder WithRowId(bool withRowId)
    {
        _withRowId = withRowId;
        return this;
    }

    public string ExplainPlan(bool verbose = false)
    {
        Exception? exception = null;
        string? result = null;
        string[]? selectColumns = null;
        if (_selectColumns.Count > 0)
        {
            selectColumns = _selectColumns.ToArray();
        }
        Ffi.explain_query(_connectionId, _tableId, _limit, _whereClause, _withRowId, verbose, (message) =>
            {
                result = message;
            },
            (code, message) =>
            {
                if (code < 0 && message != null)
                {
                    exception = new Exception("Failed to explain plan: " + message);
                }
            },
            selectColumns, (ulong)_selectColumns.Count,
            _fullTextSearch
        );
        if (exception != null) throw exception;
        return result ??= "No explanation returned";
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
        _fullTextSearch = text;
        return this;
    }

    public virtual ILanceQueryBuilder Rerank(IReranker reranker)
    {
        // Rerankers appear to be Full Text Search specific, so we're not implementing this (yet)
        throw new NotImplementedException();
    }

    public Apache.Arrow.Table ToArrow()
    {
        var batches = ToBatches(0);
        var batchesList = batches.ToList();
        var schema = batches.First().Schema;
        Apache.Arrow.Table table = Apache.Arrow.Table.TableFromRecordBatches(schema, batchesList);
        return table;
    }

    public Task<Apache.Arrow.Table> ToArrowAsync(CancellationToken token = default)
    {
        throw new NotImplementedException();
    }

    public IEnumerable<IDictionary<string, object>> ToList()
    {
        // Referencing query.py line 1326, this function is implemented in Python as calling ToArrow and
        // then morphing into a PyList.
        // TODO: I'm not 100% sure about this?
        var table = ToArrow();
        var result = new List<IDictionary<string, object>>();
        for (var i = 0; i < table.RowCount; i++)
        {
            var row = new Dictionary<string, object>();
            for (var j = 0; j < table.ColumnCount; j++)
            {
                var column = table.Column(j);
                var data = new List<object>();
                for (var k=0; k<column.Data.Length; k++)
                {
                    data.Add(column.Data.Array(k));
                }
                row[column.Name] = data;
            }
            result.Add(row);
        }
        return result;
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
        
        if (_withRowId) throw new Exception("Row ID does not work with RecordBatch queries");
        
        string[]? selectColumns = null;
        if (_selectColumns.Count > 0)
        {
            selectColumns = _selectColumns.ToArray();
        }
        
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
        }, _limit, _whereClause, _withRowId, selectColumns, (ulong)_selectColumns.Count,
            _fullTextSearch);
        
        if (exception != null) throw exception;
        return result;
    }

    public IAsyncEnumerable<RecordBatch> ToBatchesAsync(int batchSize, CancellationToken token = default)
    {
        throw new NotImplementedException();
    }
}