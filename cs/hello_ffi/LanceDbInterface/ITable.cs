﻿using Apache.Arrow;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LanceDbInterface
{
    public interface ITable :IDisposable
    {
        void CreateScalarIndex(string columnName, IndexType indexType = IndexType.BTree, bool replace = true);
        Task CreateScalarIndexAsync(string columnName, IndexType indexType = IndexType.BTree, bool replace = true, CancellationToken token = default);

        void CreateIndex(string columnName, Metric metric = Metric.L2, int numPartitions = 256, int numSubVectors = 96, bool replace = true);
        Task CreateIndexAsync(string columnName, Metric metric = Metric.L2, int numPartitions = 256, int numSubVectors = 96, bool replace = true, CancellationToken token = default);

        void CreateFtsIndex(IEnumerable<string> columnNames, IEnumerable<string> orderingColumnNames, bool replace = false, bool withPosition = true, int writerHeapSize = 1024 * 1024 * 1024, string tokenizerName = "default", bool useTantivy = true);
        Task CreateFtsIndexAsync(IEnumerable<string> columnNames, IEnumerable<string> orderingColumnNames, bool replace = false, bool withPosition = true, int writerHeapSize = 1024 * 1024 * 1024, string tokenizerName = "default", bool useTantivy = true, CancellationToken token = default);

        int CountRows(string? filter = null);
        Task<int> CountRowsAsync(string? filter = null, CancellationToken token = default);

        ILanceMergeInsertBuilder MergeInsert(IEnumerable<string> on);
        Task<ILanceMergeInsertBuilder> MergeInsertAsync(IEnumerable<string> on, CancellationToken token = default);

        void Add(Apache.Arrow.Table data, WriteMode mode = WriteMode.Append, BadVectorHandling badVectorHandling = BadVectorHandling.Error, float fillValue = 0.0F);
        Task AddAsync(Apache.Arrow.Table data, WriteMode mode = WriteMode.Append, BadVectorHandling badVectorHandling = BadVectorHandling.Error, float fillValue = 0.0F, CancellationToken token = default);

        void Add(IEnumerable<Apache.Arrow.RecordBatch> data, WriteMode mode = WriteMode.Append, BadVectorHandling badVectorHandling = BadVectorHandling.Error, float fillValue = 0.0F);
        Task AddAsync(IEnumerable<Apache.Arrow.RecordBatch> data, WriteMode mode = WriteMode.Append, BadVectorHandling badVectorHandling = BadVectorHandling.Error, float fillValue = 0.0F, CancellationToken token = default);

        void Add(IEnumerable<Dictionary<string, object>> data, WriteMode mode = WriteMode.Append, BadVectorHandling badVectorHandling = BadVectorHandling.Error, float fillValue = 0.0F);
        Task AddAsync(IEnumerable<Dictionary<string, object>> data, WriteMode mode = WriteMode.Append, BadVectorHandling badVectorHandling = BadVectorHandling.Error, float fillValue = 0.0F, CancellationToken token = default);

        void Update(IDictionary<string, Object> updates, string? whereClause = null);
        Task UpdateAsync(IDictionary<string, Object> updates, string? whereClause = null, CancellationToken token = default);

        void UpdateSQL(IDictionary<string, string> updates, string? whereClause = null);
        Task UpdateSQLAsync(IDictionary<string, string> updates, string? whereClause = null, CancellationToken token = default);

        void Delete(string whereClause);
        Task DeleteAsync(string whereClause, CancellationToken token = default);

        void Close();
        Task CloseAsync(CancellationToken cancellationToken = default);

        void CompactFiles();
        Task CompactFilesAsync(CancellationToken token = default);

        ILanceQueryBuilder Search(Apache.Arrow.Array vector, string vectorColumnName, QueryType queryType = QueryType.Auto);
        ILanceQueryBuilder Search(Apache.Arrow.ChunkedArray vectors, string vectorColumnName, QueryType queryType = QueryType.Auto);

        //AsyncVectorQuery VectorSearch(Apache.Arrow.Array array);

        bool IsOpen { get;  }
        Schema Schema { get; }
        string Name { get; }
    }
}