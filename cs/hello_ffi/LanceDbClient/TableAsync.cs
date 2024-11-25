using Apache.Arrow;
using LanceDbInterface;
using MathNet.Numerics.LinearAlgebra;
using Array = Apache.Arrow.Array;

namespace LanceDbClient;

public sealed partial class Table
{
    public Task<int> CountRowsAsync(string? filter = null, CancellationToken token = default)
    {
        var tcs = new TaskCompletionSource<int>();
        Ffi.ResultCallback callback = (code, message) =>
        {
            if (code < 0)
            {
                tcs.SetException(new Exception(message));
            }
            else
            {
                tcs.SetResult((int)code);
            }
        };
        Task.Run(() =>
        {
            Ffi.count_rows(_connectionHandle, _tableHandle, filter, callback);
        }, token);
        return tcs.Task;
    }

    public Task CreateScalarIndexAsync(string columnName, LanceDbInterface.ScalarIndexType indexType = LanceDbInterface.ScalarIndexType.BTree, bool replace = true,
        CancellationToken token = default)
    {
        var tcs = new TaskCompletionSource();
        Ffi.ResultCallback callback = (code, message) =>
        {
            if (code < 0)
            {
                tcs.SetException(new Exception(message));
            }
            else
            {
                tcs.SetResult();
            }
        };
        Task.Run(() =>
        {
            Ffi.create_scalar_index(_connectionHandle, _tableHandle, columnName, (uint)indexType, replace, callback);
        }, token);
        return tcs.Task;
    }

    public Task CreateIndexAsync(string columnName, Metric metric = Metric.L2, int numPartitions = 256, int numSubVectors = 96,
        bool replace = true, CancellationToken token = default)
    {
        var tcs = new TaskCompletionSource();
        Ffi.ResultCallback callback = (code, message) =>
        {
            if (code < 0)
            {
                tcs.SetException(new Exception(message));
            }
            else
            {
                tcs.SetResult();
            }
        };
        Task.Run(() =>
        {
            Ffi.create_index(_connectionHandle, _tableHandle, columnName, (uint)metric, (uint)numPartitions, (uint)numSubVectors, replace, callback);
        }, token);
        return tcs.Task;
    }

    public Task CreateFtsIndexAsync(IEnumerable<string> columnNames, IEnumerable<string> orderingColumnNames, bool replace = false,
        bool withPosition = true, int writerHeapSize = 1073741824, string tokenizerName = "default", bool useTantivy = true,
        CancellationToken token = default)
    {
        var tcs = new TaskCompletionSource();
        Ffi.ResultCallback callback = (code, message) =>
        {
            if (code < 0)
            {
                tcs.SetException(new Exception(message));
            }
            else
            {
                tcs.SetResult();
            }
        };
        Task.Run(() =>
        {
            var columnNamesList = columnNames.ToArray();
            Ffi.create_full_text_index(_connectionHandle, _tableHandle, columnNamesList, (ulong)columnNamesList.Count(),
                withPosition, replace, tokenizerName, callback);
        }, token);
        return tcs.Task;
    }

    public Task<ILanceMergeInsertBuilder> MergeInsertAsync(IEnumerable<string> on, CancellationToken token = default)
    {
        throw new NotImplementedException();
    }

    public Task UpdateAsync(IDictionary<string, object> updates, string? whereClause = null, CancellationToken token = default)
    {
        throw new NotImplementedException();
    }

    public Task UpdateSqlAsync(IDictionary<string, string> updates, string? whereClause = null, CancellationToken token = default)
    {
        throw new NotImplementedException();
    }

    public Task DeleteAsync(string whereClause, CancellationToken token = default)
    {
        throw new NotImplementedException();
    }

    public Task CloseAsync(CancellationToken cancellationToken = default)
    {
        throw new NotImplementedException();
    }

    public Task<OptimizeStats> OptimizeAsync(TimeSpan? cleanupOlderThan = null, bool deleteUnverified = false, CancellationToken token = default)
    {
        throw new NotImplementedException();
    }

    public Task<IEnumerable<IndexConfig>> ListIndicesAsync(CancellationToken token = default)
    {
        throw new NotImplementedException();
    }

    public Task<IndexStatistics> GetIndexStatisticsAsync(string columnName, CancellationToken token = default)
    {
        throw new NotImplementedException();
    }

    public Task AddAsync(IEnumerable<Dictionary<string, object>> data, WriteMode mode = WriteMode.Append,
        BadVectorHandling badVectorHandling = BadVectorHandling.Error, float fillValue = 0,
        CancellationToken token = default)
    {
        throw new NotImplementedException();
    }

    public Task AddAsync(IEnumerable<RecordBatch> data, WriteMode mode = WriteMode.Append,
        BadVectorHandling badVectorHandling = BadVectorHandling.Error, float fillValue = 0,
        CancellationToken token = default)
    {
        throw new NotImplementedException();
    }

    public Task AddAsync(Apache.Arrow.Table data, WriteMode mode = WriteMode.Append,
        BadVectorHandling badVectorHandling = BadVectorHandling.Error, float fillValue = 0,
        CancellationToken token = default)
    {
        throw new NotImplementedException();
    }

}