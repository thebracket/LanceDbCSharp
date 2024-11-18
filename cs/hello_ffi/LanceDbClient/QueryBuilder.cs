using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Apache.Arrow;
using LanceDbInterface;
using MathNet.Numerics.LinearAlgebra;
using Array = Apache.Arrow.Array;

namespace LanceDbClient;

public partial class QueryBuilder : ILanceQueryBuilder
{
    protected readonly long _connectionId;
    protected readonly long _tableId;
    protected ulong _limit;
    protected string? _whereClause;
    protected bool _withRowId;
    protected List<string> _selectColumns;
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
    
    /// <summary>
    /// Sets a limit to how many records can be returned.
    /// </summary>
    /// <param name="limit">The limit (or 0 for none)</param>
    /// <returns>The query builder to continue building.</returns>
    public ILanceQueryBuilder Limit(int limit)
    {
        _limit = (ulong)limit;
        return this;
    }

    /// <summary>
    /// Select the list of columns to include in the query
    /// </summary>
    /// <param name="selectColumns">Column list</param>
    /// <returns>The query builder to continue building.</returns>
    public ILanceQueryBuilder SelectColumns(IEnumerable<string> selectColumns)
    {
        _selectColumns = selectColumns.ToList();
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
        _whereClause = whereClause;
        return this;
    }

    /// <summary>
    /// Include the unique RowId in the query results.
    /// </summary>
    /// <param name="withRowId">True to include the RowId, false to not include it.</param>
    /// <returns>The query builder to continue building.</returns>
    public ILanceQueryBuilder WithRowId(bool withRowId)
    {
        _withRowId = withRowId;
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

    /// <summary>
    /// Constructs a new vector query builder.
    /// </summary>
    /// <param name="vector">A list of either f16 (half), f32, f64</param>
    /// <typeparam name="T">Half, f16 or f32</typeparam>
    /// <returns>A VectorQueryBuilder</returns>
    public ILanceQueryBuilder Vector<T>(List<T> vector)
    {
        var vectorData = CastVectorList(vector);
        return new VectorQueryBuilder(_connectionId, _tableId, vectorData, _limit, _whereClause, _withRowId, _selectColumns);
    }

    /// <summary>
    /// Constructs a new vector query builder.
    /// </summary>
    /// <param name="vector">A vector of either f16 (half), f32, f64</param>
    /// <typeparam name="T">Half, f16 or f32</typeparam>
    /// <returns>A VectorQueryBuilder</returns>
    public ILanceQueryBuilder Vector<T>(Vector<T> vector) where T : struct, IEquatable<T>, IFormattable
    {
        var vectorData = CastVectorList(vector.ToList());
        return new VectorQueryBuilder(_connectionId, _tableId, vectorData, _limit, _whereClause, _withRowId, _selectColumns);
    }

    /// <summary>
    /// Constructs a new vector query builder.
    /// </summary>
    /// <param name="vector">A matrix of either f16 (half), f32, f64</param>
    /// <typeparam name="T">Half, f16 or f32</typeparam>
    /// <returns>A VectorQueryBuilder</returns>
    public ILanceQueryBuilder Vector<T>(Matrix<T> vector) where T : struct, IEquatable<T>, IFormattable
    {
        // Is column-major the correct choice?
        var asArray = vector.ToColumnMajorArray();
        var vectorData = CastVectorList(asArray.ToList());
        return new VectorQueryBuilder(_connectionId, _tableId, vectorData, _limit, _whereClause, _withRowId, _selectColumns);
    }

    /// <summary>
    /// Sets the full text search query.
    /// </summary>
    /// <param name="text">The text for which you wish to search</param>
    /// <returns>The query builder to continue building.</returns>
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

    /// <summary>
    /// Perform the query and return the results as an Arrow Table.
    /// </summary>
    /// <returns>An Apache.Arrow.Table object containing the results.</returns>
    public Apache.Arrow.Table ToArrow()
    {
        var batches = ToBatches(0);
        var batchesList = batches.ToList();
        var schema = batches.First().Schema;
        Apache.Arrow.Table table = Apache.Arrow.Table.TableFromRecordBatches(schema, batchesList);
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
        
        for (var i = 0; i < table.RowCount; i++)
        {
            var row = new Dictionary<string, object>();
            for (var j = 0; j < table.ColumnCount; j++)
            {
                var column = table.Column(j);
                if (column.Data.ArrayCount > 0)
                {
                    row[column.Name] = ArrayHelpers.ArrowArrayDataToConcrete(column.Data.Array(0));
                }
                
                /*var data = new List<object>();
                for (var k=0; k<column.Data.ArrayCount; k++)
                {
                    data.Add(column.Data.Array(k));
                }
                row[column.Name] = data;*/
            }
            result.Add(row);
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
            _fullTextSearch, (uint)batchSize);
        
        if (exception != null) throw exception;
        return result;
    }
    
    public enum TypeIndex
    {
        Half = 1,
        Float = 2,
        Double = 3,
        ArrowArray = 4,
    }

    public struct VectorDataImpl
    {
        public byte[] Data;
        public ulong Length;
        public TypeIndex DataType;
    }
    
    // TODO: Can C# do this at compile time?
    static internal VectorDataImpl CastVectorList<T>(List<T> vector)
    {
        // Calculate the buffer size
        var bufferSize = vector.Count * Unsafe.SizeOf<T>();
        // Adjust size to ensure 32-bit alignment
        if (bufferSize % 4 != 0)
        {
            bufferSize += 4 - (bufferSize % 4);
        }
        // Allocate byte array
        var data = new byte[bufferSize];
        Buffer.BlockCopy(vector.ToArray(), 0, data, 0, data.Length);

        if (typeof(T) == typeof(Half))
        {
            return new VectorDataImpl
            {
                Data = data,
                Length = (ulong)vector.Count,
                DataType = TypeIndex.Half
            };
        }
        if (typeof(T) == typeof(float))
        {
            return new VectorDataImpl
            {
                Data = data,
                Length = (ulong)vector.Count,
                DataType = TypeIndex.Float
            };
        }
        if (typeof(T) == typeof(double))
        {
            return new VectorDataImpl
            {
                Data = data,
                Length = (ulong)vector.Count,
                DataType = TypeIndex.Double
            };
        }
        
        throw new Exception("Unsupported type: " + typeof(T) + ". Supported types are Half, float, double, and Apache.Arrow.Array.");
    }
}