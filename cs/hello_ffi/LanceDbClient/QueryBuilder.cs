using System.Collections;
using System.Runtime.InteropServices;
using Apache.Arrow;
using LanceDbInterface;
using MathNet.Numerics.LinearAlgebra;

namespace LanceDbClient;

public partial class QueryBuilder : ILanceQueryBuilder
{
    protected internal readonly long ConnectionId;
    protected internal readonly long TableId;
    protected internal ulong LimitCount;
    protected internal string? WhereSql;
    protected internal bool WithRowIdent;
    protected internal List<string> SelectColumnsList;
    protected internal string? FullTextSearch;
    protected internal IReranker? Reranker;

    internal QueryBuilder(long connectionId, long tableId)
    {
        ConnectionId = connectionId;
        TableId = tableId;
        LimitCount = 0;
        WhereSql = null;
        WithRowIdent = false;
        SelectColumnsList = [];
        FullTextSearch = null;
        Reranker = null;
    }
    
    /// <summary>
    /// Sets a limit to how many records can be returned.
    /// </summary>
    /// <param name="limit">The limit (or 0 for none)</param>
    /// <returns>The query builder to continue building.</returns>
    public ILanceQueryBuilder Limit(int limit)
    {
        LimitCount = (ulong)limit;
        return this;
    }

    /// <summary>
    /// Select the list of columns to include in the query
    /// </summary>
    /// <param name="selectColumns">Column list</param>
    /// <returns>The query builder to continue building.</returns>
    public ILanceQueryBuilder SelectColumns(IEnumerable<string> selectColumns)
    {
        SelectColumnsList = selectColumns.ToList();
        return this;
    }

    /// <summary>
    /// Adds a "where" clause to the query.
    /// </summary>
    /// <param name="whereClause">The query, in SQL-like syntax.</param>
    /// <param name="prefilter">Not implemented yet.</param>
    /// <returns>The query builder to continue building.</returns>
    public ILanceQueryBuilder WhereClause(string whereClause, bool prefilter = false)
    {
        WhereSql = whereClause;
        return this;
    }

    /// <summary>
    /// Include the unique RowId in the query results.
    /// </summary>
    /// <param name="withRowId">True to include the RowId, false to not include it.</param>
    /// <returns>The query builder to continue building.</returns>
    public ILanceQueryBuilder WithRowId(bool withRowId)
    {
        WithRowIdent = withRowId;
        return this;
    }

    /// <summary>
    /// Submit the query and retrieve the query plan from the LanceDb engine.
    /// </summary>
    /// <param name="verbose">Verbose provides more information</param>
    /// <returns>A string containing the query execution plan.</returns>
    /// <exception cref="Exception">If the call fails.</exception>
    public virtual string ExplainPlan(bool verbose = false)
    {
        Exception? exception = null;
        string? result = null;
        string[]? selectColumns = null;
        if (SelectColumnsList.Count > 0)
        {
            selectColumns = SelectColumnsList.ToArray();
        }
        Ffi.explain_query(ConnectionId, TableId, LimitCount, WhereSql, WithRowIdent, verbose, (message) =>
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
            selectColumns!, (ulong)SelectColumnsList.Count,
            FullTextSearch
        );
        if (exception != null) throw exception;
        return result ??= "No explanation returned";
    }

    /// <summary>
    /// Constructs a new vector query builder.
    /// </summary>
    /// <param name="vector">A list of either f16 (half), f32, f64</param>
    /// <typeparam name="T">Half, f16 or f32</typeparam>
    /// <returns>A VectorQueryBuilder</returns>
    public ILanceVectorQueryBuilder Vector<T>(List<T> vector)
    {
        var vectorData = ArrayHelpers.CastVectorList(vector);
        return new VectorQueryBuilder(this)
            .WithVectorData(vectorData);
    }

    /// <summary>
    /// Constructs a new vector query builder.
    /// </summary>
    /// <param name="vector">A vector of either f16 (half), f32, f64</param>
    /// <typeparam name="T">Half, f16 or f32</typeparam>
    /// <returns>A VectorQueryBuilder</returns>
    public ILanceVectorQueryBuilder Vector<T>(Vector<T> vector) where T : struct, IEquatable<T>, IFormattable
    {
        var vectorData = ArrayHelpers.CastVectorList(vector.ToList());
        return new VectorQueryBuilder(this)
            .WithVectorData(vectorData);
    }

    /// <summary>
    /// Constructs a new vector query builder.
    /// </summary>
    /// <param name="vector">A matrix of either f16 (half), f32, f64</param>
    /// <typeparam name="T">Half, f16 or f32</typeparam>
    /// <returns>A VectorQueryBuilder</returns>
    public ILanceVectorQueryBuilder Vector<T>(Matrix<T> vector) where T : struct, IEquatable<T>, IFormattable
    {
        // Is column-major the correct choice?
        var asArray = vector.ToColumnMajorArray();
        var vectorData = ArrayHelpers.CastVectorList(asArray.ToList());
        return new VectorQueryBuilder(this)
            .WithVectorData(vectorData);
    }

    /// <summary>
    /// Sets the full text search query.
    /// </summary>
    /// <param name="text">The text for which you wish to search</param>
    /// <returns>The query builder to continue building.</returns>
    public ILanceQueryBuilder Text(string text)
    {
        FullTextSearch = text;
        return this;
    }

    /// <summary>
    /// Attaches a re-ranker to the query builder
    /// </summary>
    /// <param name="reranker">The re-ranker to attach</param>
    /// <returns>The updated query builder</returns>
    public virtual ILanceQueryBuilder Rerank(IReranker reranker)
    {
        throw new NotImplementedException();
    }

    /// <summary>
    /// Perform the query and return the results as an Arrow Table.
    /// </summary>
    /// <returns>An Apache.Arrow.Table object containing the results.</returns>
    public Apache.Arrow.Table ToArrow()
    {
        var batches = ToBatches(0).ToList();
        var schema = batches.First().Schema;
        var table = Apache.Arrow.Table.TableFromRecordBatches(schema, batches);
        return table;
    }
    
    /// <summary>
    /// Perform the query and return the results as a list of dictionaries.
    /// </summary>
    /// <returns>The query results</returns>
    public IEnumerable<IDictionary<string, object>> ToList()
    {
        // Referencing query.py line 1326, this function is implemented in Python as calling ToArrow and
        // then morphing into a PyList.
        // TODO: I'm not 100% sure about this?
        var table = ToArrow();
        var result = new List<IDictionary<string, object>>();
        
        // Pre-fill the list with empty dictionaries
        for (var i = 0; i < table.RowCount; i++)
        {
            result.Add(new Dictionary<string, object>());
        }
        
        for (var j = 0; j < table.ColumnCount; j++)
        {
            var column = table.Column(j);
            if (column.Data.ArrayCount > 0)
            {
                
                var raw = ArrayHelpers.ArrowArrayDataToConcrete(column.Data.Array(0), rowCount:(int)table.RowCount);
                if (raw is ArrayList chunked)
                {
                    if (chunked[0] is IEnumerable inner)
                    {
                        var rowIdx = 0;
                        foreach(var row in inner)
                        {
                            result[rowIdx][column.Name] = row;
                            rowIdx++;
                        }
                    }
                    else
                    {
                        throw new Exception("Unexpected chunked data");
                    }
                }
                else
                {
                    result[0][column.Name] = raw;
                }
            }
        }
        return result;
    }
    
    /// <summary>
    /// Perform the query and return the results as a list of RecordBatch objects.
    /// </summary>
    /// <param name="batchSize">Not implemented yet.</param>
    /// <returns>The queyr result</returns>
    /// <exception cref="Exception">If the query fails</exception>
    public virtual unsafe IEnumerable<RecordBatch> ToBatches(int batchSize)
    {
        var result = new List<RecordBatch>();
        Exception? exception = null;
        
        string[]? selectColumns = null;
        if (SelectColumnsList.Count > 0)
        {
            selectColumns = SelectColumnsList.ToArray();
        }
        
        Ffi.query(ConnectionId, TableId, (bytes, len) =>
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
            FullTextSearch, (uint)batchSize);
        
        if (exception != null) throw exception;
        return result;
    }
}