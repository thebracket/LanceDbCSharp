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
        var result = this.MergeInsert(on);
        return Task.FromResult(result);
    }

    public Task<ulong> UpdateAsync(IDictionary<string, object> updates, string? whereClause = null, CancellationToken token = default)
    {
        var updateList = new List<string>();
        foreach (var (key, value) in updates)
        {
            if (value is string s)
            {
                // SQL Sanitizing
                s = s.Replace("'", "''");
                updateList.Add(key + "='" + s + "'");
            }
            else
            {
                updateList.Add(key + "=" + value);
            }
        }
        
        var tcs = new TaskCompletionSource<ulong>();
        
        var rowsUpdated = 0ul;
        var countCallback = new Ffi.UpdateCalback((count) =>
        {
            rowsUpdated = count;
        });
        
        var resultCallback = new Ffi.ResultCallback((code, message) =>
        {
            if (code < 0)
            {
                tcs.SetException(new Exception(message));
            }
            else
            {
                tcs.SetResult(rowsUpdated);
            }
        });
        
        Task.Run(() =>
        {
            Ffi.update_rows(_connectionHandle, _tableHandle, updateList.ToArray(), (ulong)updateList.Count, whereClause,
                resultCallback, countCallback);
        }, token);
        
        return tcs.Task;
    }

    public Task<ulong> UpdateSqlAsync(IDictionary<string, string> updates, string? whereClause = null, CancellationToken token = default)
    {
        var updateList = new List<string>();
        foreach (var (key, value) in updates)
        {
            // In this case they are all guaranteed to be strings - because full SQL statements
            // with escaping already baked in are expected.
            updateList.Add(key + "=" + value);
        }
        
        var tcs = new TaskCompletionSource<ulong>();
        
        var rowsUpdated = 0ul;
        var countCallback = new Ffi.UpdateCalback((count) =>
        {
            rowsUpdated = count;
        });
        
        var resultCallback = new Ffi.ResultCallback((code, message) =>
        {
            if (code < 0)
            {
                tcs.SetException(new Exception(message));
            }
            else
            {
                tcs.SetResult(rowsUpdated);
            }
        });

        Task.Run(() =>
        {
            Ffi.update_rows(_connectionHandle, _tableHandle, updateList.ToArray(), (ulong)updateList.Count,
                whereClause, resultCallback, countCallback);
        });
        
        return tcs.Task;
    }

    public Task DeleteAsync(string whereClause, CancellationToken token = default)
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
            Ffi.delete_rows(_connectionHandle, _tableHandle, whereClause, callback);
        }, token);
        return tcs.Task;
    }

    public Task CloseAsync(CancellationToken cancellationToken = default)
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
            Ffi.close_table(_connectionHandle, _tableHandle, callback);
        }, cancellationToken);
        return tcs.Task;
    }

    public Task<OptimizeStats> OptimizeAsync(TimeSpan? cleanupOlderThan = null, bool deleteUnverified = false, CancellationToken token = default)
    {
        CompactionMetrics? compaction = null;
        RemovalStats? prune = null;
        
        var tcs = new TaskCompletionSource<OptimizeStats>();
        Ffi.CompactCallback compactCallback = (fragmentsRemoved, fragmentsAdded, filesRemoved, filesAdded) =>
        {
            compaction = new CompactionMetrics
            {
                FragmentsRemoved = (int)fragmentsRemoved,
                FragmentsAdded = (int)fragmentsAdded,
                FilesRemoved = (int)filesRemoved,
                FilesAdded = (int)filesAdded
            };
        };
        Ffi.PruneCallback pruneCallback = (removed, added) =>
        {
            prune = new RemovalStats
            {
                BytesRemoved = (int)removed,
                OldVersionsRemoved = (int)added
            };
        };
        Ffi.ResultCallback callback = (code, message) =>
        {
            if (code < 0)
            {
                tcs.SetException(new Exception(message));
            }
            else
            {
                tcs.SetResult(new OptimizeStats
                {
                    Compaction = compaction,
                    Prune = prune
                });
            }
        };
        var cleanup = cleanupOlderThan?.TotalSeconds ?? 0;
        Task.Run(() =>
        {
            Ffi.optimize_table(_connectionHandle, _tableHandle, (long)cleanup, deleteUnverified, callback, compactCallback, pruneCallback);
        }, token);
        return tcs.Task;
    }

    public Task<IEnumerable<IndexConfig>> ListIndicesAsync(CancellationToken token = default)
    {
        var indices = new List<IndexConfig>();
        var tcs = new TaskCompletionSource<IEnumerable<IndexConfig>>();
        Ffi.TableIndexEntryCallback indexCallback = (name, type, columns, _) =>
        {
            indices.Add(new IndexConfig
            {
                Name = name,
                IndexType = (IndexType)type,
                Columns = columns
            });
        };
        Ffi.ResultCallback callback = (code, message) =>
        {
            if (code < 0)
            {
                tcs.SetException(new Exception(message));
            }
            else
            {
                tcs.SetResult(indices);
            }
        };
        Task.Run(() =>
        {
            Ffi.list_indices(_connectionHandle, _tableHandle, indexCallback, callback);
        }, token);
        return tcs.Task;
    }

    public Task<IndexStatistics> GetIndexStatisticsAsync(string columnName, CancellationToken token = default)
    {
        IndexStatistics stats = new IndexStatistics();
        Ffi.IndexStatisticsCallback indexCallback =
            (indexType, distanceType, numIndexedRows, numIndices, numUnIndexedRows) =>
            {
                stats.NumIndexedRows = (int)numIndexedRows;
                stats.NumUnIndexedRows = (int)numUnIndexedRows;
                stats.IndexType = (IndexType)indexType;
                stats.DistanceType = (Metric)distanceType;
                stats.NumIndices = (int)numIndices;
            };
        var tcs = new TaskCompletionSource<IndexStatistics>();
        Ffi.ResultCallback callback = (code, message) =>
        {
            if (code < 0)
            {
                tcs.SetException(new Exception(message));
            }
            else
            {
                tcs.SetResult(stats);
            }
        };
        Task.Run(() =>
        {
            Ffi.get_index_statistics(_connectionHandle, _tableHandle, columnName, indexCallback, callback);
        }, token);
        return tcs.Task;
    }

    public Task AddAsync(IEnumerable<Dictionary<string, object>> data, WriteMode mode = WriteMode.Append,
        BadVectorHandling badVectorHandling = BadVectorHandling.Error, float fillValue = 0,
        CancellationToken token = default)
    {
        // Convert the dictionary into a record batch
        var batch = ArrayHelpers.ConcreteToArrowTable(data, Schema);
        return AddAsync(batch, mode, badVectorHandling, fillValue, token);
    }

    public Task AddAsync(IEnumerable<RecordBatch> data, WriteMode mode = WriteMode.Append,
        BadVectorHandling badVectorHandling = BadVectorHandling.Error, float fillValue = 0,
        CancellationToken token = default)
    {
        var tcs = new TaskCompletionSource();
        Task.Run(() =>
        {
            unsafe
            {
                foreach (var recordBatch in data)
                {
                    var batch = Ffi.SerializeRecordBatch(recordBatch);
                    fixed (byte* p = batch)
                    {
                        Ffi.add_record_batch(_connectionHandle, _tableHandle, p, (ulong)batch.Length, (uint)mode,
                            (uint)badVectorHandling, fillValue, (code, message) =>
                            {
                                if (code < 0)
                                {
                                    tcs.SetException(new Exception("Failed to add record batch: " + message));
                                }
                            });
                    }
                }
            }

            tcs.SetResult();
        }, token);
        return tcs.Task;
    }

    public Task AddAsync(Apache.Arrow.Table data, WriteMode mode = WriteMode.Append,
        BadVectorHandling badVectorHandling = BadVectorHandling.Error, float fillValue = 0,
        CancellationToken token = default)
    {
        return AddAsync(ArrayHelpers.ArrowTableToRecordBatch(data), mode, badVectorHandling, fillValue, token);
    }

}