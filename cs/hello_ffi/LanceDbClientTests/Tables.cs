using Apache.Arrow;
using LanceDbClient;
using LanceDbInterface;

namespace LanceDbClientTests;

public partial class Tests
{
    [Test]
    public void CloseTable()
    {
        var uri = new Uri("file:///tmp/test_table_close");
        try
        {
            using (var cnn = new Connection(uri))
            {
                Assert.That(cnn.IsOpen, Is.True);
                var table = cnn.CreateTable("table1", Helpers.GetSchema());
                Assert.That(table.IsOpen, Is.True);
                table.Close();
                Assert.That(table.IsOpen, Is.False);
            }
        }
        finally
        {
            Cleanup(uri);
        }
    }
    
    [Test]
    public async Task CloseTableAsync()
    {
        var uri = new Uri("file:///tmp/test_table_close_async");
        try
        {
            using (var cnn = new Connection(uri))
            {
                Assert.That(cnn.IsOpen, Is.True);
                var table = await cnn.CreateTableAsync("table1", Helpers.GetSchema());
                Assert.That(table.IsOpen, Is.True);
                await table.CloseAsync();
                Assert.That(table.IsOpen, Is.False);
            }
        }
        finally
        {
            Cleanup(uri);
        }
    }

    [Test]
    public void CloseTableAlreadyClosed()
    {
        var uri = new Uri("file:///tmp/test_table_close_already_closed");
        try
        {
            using (var cnn = new Connection(uri))
            {
                Assert.That(cnn.IsOpen, Is.True);
                var table = cnn.CreateTable("table1", Helpers.GetSchema());
                Assert.That(table.IsOpen, Is.True);
                table.Close();
                Assert.That(table.IsOpen, Is.False);
                table.Close();
                Assert.That(table.IsOpen, Is.False);
            }
        }
        finally
        {
            Cleanup(uri);
        }
    }
    
    [Test]
    public async Task CloseTableAlreadyClosedAsync()
    {
        var uri = new Uri("file:///tmp/test_table_close_already_closed_async");
        try
        {
            using (var cnn = new Connection(uri))
            {
                Assert.That(cnn.IsOpen, Is.True);
                var table = await cnn.CreateTableAsync("table1", Helpers.GetSchema());
                Assert.That(table.IsOpen, Is.True);
                await table.CloseAsync();
                Assert.That(table.IsOpen, Is.False);
                await table.CloseAsync();
                Assert.That(table.IsOpen, Is.False);
            }
        }
        finally
        {
            Cleanup(uri);
        }
    }

    [Test]
    public void AddRows()
    {
        var uri = new Uri("file:///tmp/test_table_add_rows");
        try
        {
            using (var cnn = new Connection(uri))
            {
                Assert.That(cnn.IsOpen, Is.True);
                var table = cnn.CreateTable("table1", Helpers.GetSchema());
                Assert.That(table.IsOpen, Is.True);
                var recordBatch = Helpers.CreateSampleRecordBatch(
                    Helpers.GetSchema(), 1, 128
                );
                // Note that the interface defines a list, so we'll use that
                var array = new List<RecordBatch>();
                array.Add(recordBatch);
                table.Add(array);
                Assert.That(table.CountRows(), Is.GreaterThan(0));
            }
        }
        finally
        {
            Cleanup(uri);
        }
    }
    
    [Test]
    public async Task AddRowsAsync()
    {
        var uri = new Uri("file:///tmp/test_table_add_rows");
        try
        {
            using (var cnn = new Connection(uri))
            {
                Assert.That(cnn.IsOpen, Is.True);
                var table = await cnn.CreateTableAsync("table1", Helpers.GetSchema());
                Assert.That(table.IsOpen, Is.True);
                var recordBatch = Helpers.CreateSampleRecordBatch(
                    Helpers.GetSchema(), 1, 128
                );
                // Note that the interface defines a list, so we'll use that
                var array = new List<RecordBatch>();
                array.Add(recordBatch);
                await table.AddAsync(array);
                Assert.That(await table.CountRowsAsync(), Is.GreaterThan(0));
            }
        }
        finally
        {
            Cleanup(uri);
        }
    }
    
    [Test]
    public void AddRowsObjectDictionary()
    {
        var uri = new Uri("file:///tmp/test_table_add_rows_obj");
        try
        {
            using (var cnn = new Connection(uri))
            {
                Assert.That(cnn.IsOpen, Is.True);
                var table = cnn.CreateTable("table1", Helpers.GetSchema());
                Assert.That(table.IsOpen, Is.True);
                
                var data = new List<Dictionary<string, object>>();
                var rowDict = new Dictionary<string, object>();
                rowDict.Add("id", new List<string>() { "0" });
                var rowList = new List<float>();
                for (int i = 0; i < 128; i++)
                {
                    rowList.Add(1.0f);
                }
                rowDict.Add("vector", rowList);
                data.Add(rowDict);
                table.Add(data);
                
                Assert.That(table.CountRows(), Is.GreaterThan(0));
            }
        }
        finally
        {
            Cleanup(uri);
        }
    }
    
    [Test]
    public async Task AddRowsObjectDictionaryAsync()
    {
        var uri = new Uri("file:///tmp/test_table_add_rows_obj_async");
        try
        {
            using (var cnn = new Connection(uri))
            {
                Assert.That(cnn.IsOpen, Is.True);
                var table = await cnn.CreateTableAsync("table1", Helpers.GetSchema());
                Assert.That(table.IsOpen, Is.True);
                
                var data = new List<Dictionary<string, object>>();
                var rowDict = new Dictionary<string, object>();
                rowDict.Add("id", new List<string>() { "0" });
                var rowList = new List<float>();
                for (int i = 0; i < 128; i++)
                {
                    rowList.Add(1.0f);
                }
                rowDict.Add("vector", rowList);
                data.Add(rowDict);
                await table.AddAsync(data);
                
                Assert.That(await table.CountRowsAsync(), Is.GreaterThan(0));
            }
        }
        finally
        {
            Cleanup(uri);
        }
    }
    
    [Test]
    public void AddRowsBadDim()
    {
        var uri = new Uri("file:///tmp/test_table_add_rows");
        try
        {
            using (var cnn = new Connection(uri))
            {
                Assert.That(cnn.IsOpen, Is.True);
                var table = cnn.CreateTable("table1", Helpers.GetSchema());
                Assert.That(table.IsOpen, Is.True);
                var recordBatch = Helpers.CreateSampleRecordBatch(
                    Helpers.GetSchema(), 1, 1
                );
                // Note that the interface defines a list, so we'll use that
                var array = new List<RecordBatch>();
                array.Add(recordBatch);
                Assert.Throws<Exception>(() => table.Add(array));
            }
        }
        finally
        {
            Cleanup(uri);
        }
    }
    
    [Test]
    public async Task AddRowsBadDimAsync()
    {
        var uri = new Uri("file:///tmp/test_table_add_rows_async");
        try
        {
            using (var cnn = new Connection(uri))
            {
                Assert.That(cnn.IsOpen, Is.True);
                var table = await cnn.CreateTableAsync("table1", Helpers.GetSchema());
                Assert.That(table.IsOpen, Is.True);
                var recordBatch = Helpers.CreateSampleRecordBatch(
                    Helpers.GetSchema(), 1, 1
                );
                // Note that the interface defines a list, so we'll use that
                var array = new List<RecordBatch>();
                array.Add(recordBatch);
                Assert.ThrowsAsync<Exception>(async () => await table.AddAsync(array));
            }
        }
        finally
        {
            Cleanup(uri);
        }
    }
    
    [Test]
    public void CountRowsEmpty()
    {
        var uri = new Uri("file:///tmp/test_table_empty_count");
        try
        {
            using (var cnn = new Connection(uri))
            {
                Assert.That(cnn.IsOpen, Is.True);
                var table = cnn.CreateTable("table1", Helpers.GetSchema());
                Assert.That(table.CountRows(), Is.Zero);
            }
        }
        finally
        {
            Cleanup(uri);
        }
        
        Assert.Pass();
    }
    
    [Test]
    public async Task CountRowsEmptyAsync()
    {
        var uri = new Uri("file:///tmp/test_table_empty_count_async");
        try
        {
            using (var cnn = new Connection(uri))
            {
                Assert.That(cnn.IsOpen, Is.True);
                var table = await cnn.CreateTableAsync("table1", Helpers.GetSchema());
                Assert.That(await table.CountRowsAsync(), Is.Zero);
            }
        }
        finally
        {
            Cleanup(uri);
        }
        
        Assert.Pass();
    }

    [Test]
    public void CountRows()
    {
        var uri = new Uri("file:///tmp/test_table_count");
        try
        {
            using (var cnn = new Connection(uri))
            {
                Assert.That(cnn.IsOpen, Is.True);
                var table = cnn.CreateTable("table1", Helpers.GetSchema());
                Assert.That(table.IsOpen, Is.True);
                var recordBatch = Helpers.CreateSampleRecordBatch(
                    Helpers.GetSchema(), 1, 128
                );
                // Note that the interface defines a list, so we'll use that
                var array = new List<RecordBatch>();
                array.Add(recordBatch);
                table.Add(array);
                Assert.That(table.CountRows(), Is.GreaterThan(0));
            }
        }
        finally
        {
            Cleanup(uri);
        }
    }
    
    [Test]
    public async Task CountRowsAsync()
    {
        var uri = new Uri("file:///tmp/test_table_count_async");
        try
        {
            using (var cnn = new Connection(uri))
            {
                Assert.That(cnn.IsOpen, Is.True);
                var table = await cnn.CreateTableAsync("table1", Helpers.GetSchema());
                Assert.That(table.IsOpen, Is.True);
                var recordBatch = Helpers.CreateSampleRecordBatch(
                    Helpers.GetSchema(), 1, 128
                );
                // Note that the interface defines a list, so we'll use that
                var array = new List<RecordBatch>();
                array.Add(recordBatch);
                await table.AddAsync(array);
                Assert.That(await table.CountRowsAsync(), Is.GreaterThan(0));
            }
        }
        finally
        {
            Cleanup(uri);
        }
    }
    
    [Test]
    public void CountRowsWithFilter()
    {
        var uri = new Uri("file:///tmp/test_table_count_filter");
        try
        {
            using (var cnn = new Connection(uri))
            {
                Assert.That(cnn.IsOpen, Is.True);
                var table = cnn.CreateTable("table1", Helpers.GetSchema());
                Assert.That(table.IsOpen, Is.True);
                var recordBatch = Helpers.CreateSampleRecordBatch(
                    Helpers.GetSchema(), 8, 128
                );
                // Note that the interface defines a list, so we'll use that
                var array = new List<RecordBatch>();
                array.Add(recordBatch);
                table.Add(array);
                Assert.That(table.CountRows("id = '0'"), Is.EqualTo(1));
            }
        }
        finally
        {
            Cleanup(uri);
        }
    }
    
    [Test]
    public void DeleteRowsWithFilter()
    {
        var uri = new Uri("file:///tmp/test_table_deleterows_filter");
        try
        {
            using (var cnn = new Connection(uri))
            {
                Assert.That(cnn.IsOpen, Is.True);
                var table = cnn.CreateTable("table1", Helpers.GetSchema());
                Assert.That(table.IsOpen, Is.True);
                var recordBatch = Helpers.CreateSampleRecordBatch(
                    Helpers.GetSchema(), 8, 128
                );
                // Note that the interface defines a list, so we'll use that
                var array = new List<RecordBatch>();
                array.Add(recordBatch);
                table.Add(array);
                Assert.That(table.CountRows("id = '0'"), Is.EqualTo(1));
                table.Delete("id = '0'");
                Assert.That(table.CountRows("id = '0'"), Is.EqualTo(0));
            }
        }
        finally
        {
            Cleanup(uri);
        }
    }
    
    [Test]
    public async Task DeleteRowsWithFilterAsync()
    {
        var uri = new Uri("file:///tmp/test_table_deleterows_filter_async");
        try
        {
            using (var cnn = new Connection(uri))
            {
                Assert.That(cnn.IsOpen, Is.True);
                var table = await cnn.CreateTableAsync("table1", Helpers.GetSchema());
                Assert.That(table.IsOpen, Is.True);
                var recordBatch = Helpers.CreateSampleRecordBatch(
                    Helpers.GetSchema(), 8, 128
                );
                // Note that the interface defines a list, so we'll use that
                var array = new List<RecordBatch>();
                array.Add(recordBatch);
                await table.AddAsync(array);
                Assert.That(await table.CountRowsAsync("id = '0'"), Is.EqualTo(1));
                await table.DeleteAsync("id = '0'");
                Assert.That(await table.CountRowsAsync("id = '0'"), Is.EqualTo(0));
            }
        }
        finally
        {
            Cleanup(uri);
        }
    }
    
    [Test]
    public void DeleteRowsWithoutFilter()
    {
        var uri = new Uri("file:///tmp/test_table_deleterows_nofilter");
        try
        {
            using (var cnn = new Connection(uri))
            {
                Assert.That(cnn.IsOpen, Is.True);
                var table = cnn.CreateTable("table1", Helpers.GetSchema());
                Assert.That(table.IsOpen, Is.True);
                var recordBatch = Helpers.CreateSampleRecordBatch(
                    Helpers.GetSchema(), 8, 128
                );
                // Note that the interface defines a list, so we'll use that
                var array = new List<RecordBatch>();
                array.Add(recordBatch);
                table.Add(array);
                Assert.That(table.CountRows(), Is.GreaterThan(0));
                table.Delete("0 = 0");
                Assert.That(table.CountRows("id = '0'"), Is.EqualTo(0));
            }
        }
        finally
        {
            Cleanup(uri);
        }
    }
    
    [Test]
    public async Task DeleteRowsWithoutFilterAsync()
    {
        var uri = new Uri("file:///tmp/test_table_deleterows_nofilter_async");
        try
        {
            using (var cnn = new Connection(uri))
            {
                Assert.That(cnn.IsOpen, Is.True);
                var table = await cnn.CreateTableAsync("table1", Helpers.GetSchema());
                Assert.That(table.IsOpen, Is.True);
                var recordBatch = Helpers.CreateSampleRecordBatch(
                    Helpers.GetSchema(), 8, 128
                );
                // Note that the interface defines a list, so we'll use that
                var array = new List<RecordBatch>();
                array.Add(recordBatch);
                await table.AddAsync(array);
                Assert.That(await table.CountRowsAsync(), Is.GreaterThan(0));
                await table.DeleteAsync("0 = 0");
                Assert.That(await table.CountRowsAsync("id = '0'"), Is.EqualTo(0));
            }
        }
        finally
        {
            Cleanup(uri);
        }
    }
    
    [Test]
    public void UpdateRows()
    {
        var uri = new Uri("file:///tmp/test_table_updaterows_nofilter");
        try
        {
            using (var cnn = new Connection(uri))
            {
                Assert.That(cnn.IsOpen, Is.True);
                var table = cnn.CreateTable("table1", Helpers.GetSchema());
                Assert.That(table.IsOpen, Is.True);
                var recordBatch = Helpers.CreateSampleRecordBatch(
                    Helpers.GetSchema(), 8, 128
                );
                // Note that the interface defines a list, so we'll use that
                var array = new List<RecordBatch> { recordBatch };
                table.Add(array);
                Assert.That(table.CountRows(), Is.GreaterThan(0));
                var updates = new Dictionary<string, object> { { "id", "test" } };
                var updatedCount = table.Update(updates, "id = '0'");
                Assert.That(table.CountRows("id = 'test'"), Is.EqualTo(1));
                Assert.That(updatedCount, Is.EqualTo(1));
            }
        }
        finally
        {
            Cleanup(uri);
        }
    }
    
    [Test]
    public async Task UpdateRowsAsync()
    {
        var uri = new Uri("file:///tmp/test_table_updaterows_nofilter_async");
        try
        {
            using (var cnn = new Connection(uri))
            {
                Assert.That(cnn.IsOpen, Is.True);
                var table = await cnn.CreateTableAsync("table1", Helpers.GetSchema());
                Assert.That(table.IsOpen, Is.True);
                var recordBatch = Helpers.CreateSampleRecordBatch(
                    Helpers.GetSchema(), 8, 128
                );
                // Note that the interface defines a list, so we'll use that
                var array = new List<RecordBatch> { recordBatch };
                await table.AddAsync(array);
                Assert.That(await table.CountRowsAsync(), Is.GreaterThan(0));
                var updates = new Dictionary<string, object> { { "id", "test" } };
                var updatedCount = await table.UpdateAsync(updates, "id = '0'");
                Assert.That(await table.CountRowsAsync("id = 'test'"), Is.EqualTo(1));
                Assert.That(updatedCount, Is.EqualTo(1));
            }
        }
        finally
        {
            Cleanup(uri);
        }
    }
    
    [Test]
    public void UpdateRowsSql()
    {
        var uri = new Uri("file:///tmp/test_table_updaterowssql_nofilter");
        try
        {
            using (var cnn = new Connection(uri))
            {
                Assert.That(cnn.IsOpen, Is.True);
                var table = cnn.CreateTable("table1", Helpers.GetSchema());
                Assert.That(table.IsOpen, Is.True);
                var recordBatch = Helpers.CreateSampleRecordBatch(
                    Helpers.GetSchema(), 8, 128
                );
                // Note that the interface defines a list, so we'll use that
                var array = new List<RecordBatch> { recordBatch };
                table.Add(array);
                Assert.That(table.CountRows(), Is.GreaterThan(0));
                var updates = new Dictionary<string, string> { { "id", "'test'" } };
                var updatedCount = table.UpdateSql(updates, "id = '0'");
                Assert.That(table.CountRows("id = 'test'"), Is.EqualTo(1));
                Assert.That(updatedCount, Is.EqualTo(1));
            }
        }
        finally
        {
            Cleanup(uri);
        }
    }
    
    [Test]
    public async Task UpdateRowsSqlAsync()
    {
        var uri = new Uri("file:///tmp/test_table_updaterowssql_nofilter_async");
        try
        {
            using (var cnn = new Connection(uri))
            {
                Assert.That(cnn.IsOpen, Is.True);
                var table = await cnn.CreateTableAsync("table1", Helpers.GetSchema());
                Assert.That(table.IsOpen, Is.True);
                var recordBatch = Helpers.CreateSampleRecordBatch(
                    Helpers.GetSchema(), 8, 128
                );
                // Note that the interface defines a list, so we'll use that
                var array = new List<RecordBatch> { recordBatch };
                await table.AddAsync(array);
                Assert.That(await table.CountRowsAsync(), Is.GreaterThan(0));
                var updates = new Dictionary<string, string> { { "id", "'test'" } };
                var updatedCount = await table.UpdateSqlAsync(updates, "id = '0'");
                Assert.That(await table.CountRowsAsync("id = 'test'"), Is.EqualTo(1));
                Assert.That(updatedCount, Is.EqualTo(1));
            }
        }
        finally
        {
            Cleanup(uri);
        }
    }
    
    [Test]
    public void CreateDefaultScalarIndexFailsOnEmpty()
    {
        var uri = new Uri("file:///tmp/test_table_empty_try_index");
        try
        {
            using (var cnn = new Connection(uri))
            {
                Assert.That(cnn.IsOpen, Is.True);
                var table = cnn.CreateTable("table1", Helpers.GetSchema());
                Assert.Throws<Exception>(() => table.CreateScalarIndex("id"));
            }
        }
        finally
        {
            Cleanup(uri);
        }

        Assert.Pass();
    }
    
    [Test]
    public async Task CreateDefaultScalarIndexFailsOnEmptyAsync()
    {
        var uri = new Uri("file:///tmp/test_table_empty_try_index_async");
        try
        {
            using (var cnn = new Connection(uri))
            {
                Assert.That(cnn.IsOpen, Is.True);
                var table = await cnn.CreateTableAsync("table1", Helpers.GetSchema());
                Assert.ThrowsAsync<Exception>(async () => await table.CreateScalarIndexAsync("id"));
            }
        }
        finally
        {
            Cleanup(uri);
        }

        Assert.Pass();
    }
    
    [Test]
    public void CreateDefaultScalarIndex()
    {
        var uri = new Uri("file:///tmp/test_table_try_index");
        try
        {
            using (var cnn = new Connection(uri))
            {
                Assert.That(cnn.IsOpen, Is.True);
                var table = cnn.CreateTable("table1", Helpers.GetSchema());
                var recordBatch = Helpers.CreateSampleRecordBatch(
                    Helpers.GetSchema(), 8, 128
                );
                // Note that the interface defines a list, so we'll use that
                var array = new List<RecordBatch>();
                array.Add(recordBatch);
                table.Add(array);
                table.CreateScalarIndex("id");
            }
        }
        finally
        {
            Cleanup(uri);
        }
    }
    
    [Test]
    public async Task CreateDefaultScalarIndexAsync()
    {
        var uri = new Uri("file:///tmp/test_table_try_index_async");
        try
        {
            using (var cnn = new Connection(uri))
            {
                Assert.That(cnn.IsOpen, Is.True);
                var table = await cnn.CreateTableAsync("table1", Helpers.GetSchema());
                var recordBatch = Helpers.CreateSampleRecordBatch(
                    Helpers.GetSchema(), 8, 128
                );
                // Note that the interface defines a list, so we'll use that
                var array = new List<RecordBatch>();
                array.Add(recordBatch);
                await table.AddAsync(array);
                await table.CreateScalarIndexAsync("id");
            }
        }
        finally
        {
            Cleanup(uri);
        }
    }
    
    [Test]
    public void ListIndices()
    {
        var uri = new Uri("file:///tmp/test_table_try_index_list");
        try
        {
            using (var cnn = new Connection(uri))
            {
                Assert.That(cnn.IsOpen, Is.True);
                var table = cnn.CreateTable("table1", Helpers.GetSchema());
                var recordBatch = Helpers.CreateSampleRecordBatch(
                    Helpers.GetSchema(), 8, 128
                );
                // Note that the interface defines a list, so we'll use that
                var array = new List<RecordBatch>();
                array.Add(recordBatch);
                table.Add(array);
                table.CreateScalarIndex("id");
                table.Optimize();
                var indices = table.ListIndices();
                Assert.That(indices.Count, Is.GreaterThan(0));
                Assert.That(indices.First().Name, Is.EqualTo("id_idx"));
                Assert.That(indices.First().IndexType, Is.EqualTo(IndexType.BTree));
                Assert.That(indices.First().Columns.Count(), Is.EqualTo(1));
                Assert.That(indices.First().Columns.First(), Is.EqualTo("id"));
            }
        }
        finally
        {
            Cleanup(uri);
        }
    }
    
    [Test]
    public async Task ListIndicesAsync()
    {
        var uri = new Uri("file:///tmp/test_table_try_index_list_async");
        try
        {
            using (var cnn = new Connection(uri))
            {
                Assert.That(cnn.IsOpen, Is.True);
                var table = await cnn.CreateTableAsync("table1", Helpers.GetSchema());
                var recordBatch = Helpers.CreateSampleRecordBatch(
                    Helpers.GetSchema(), 8, 128
                );
                // Note that the interface defines a list, so we'll use that
                var array = new List<RecordBatch>();
                array.Add(recordBatch);
                await table.AddAsync(array);
                await table.CreateScalarIndexAsync("id");
                await table.OptimizeAsync();
                var indices = await table.ListIndicesAsync();
                Assert.That(indices.Count, Is.GreaterThan(0));
                Assert.That(indices.First().Name, Is.EqualTo("id_idx"));
                Assert.That(indices.First().IndexType, Is.EqualTo(IndexType.BTree));
                Assert.That(indices.First().Columns.Count(), Is.EqualTo(1));
                Assert.That(indices.First().Columns.First(), Is.EqualTo("id"));
            }
        }
        finally
        {
            Cleanup(uri);
        }
    }
    
    [Test]
    public void IndexStats()
    {
        var uri = new Uri("file:///tmp/test_table_try_index_stats");
        try
        {
            using (var cnn = new Connection(uri))
            {
                Assert.That(cnn.IsOpen, Is.True);
                var table = cnn.CreateTable("table1", Helpers.GetSchema());
                var recordBatch = Helpers.CreateSampleRecordBatch(
                    Helpers.GetSchema(), 4096, 128
                );
                // Note that the interface defines a list, so we'll use that
                var array = new List<RecordBatch>();
                array.Add(recordBatch);
                table.Add(array);
                table.CreateScalarIndex("id");
                var stats = table.GetIndexStatistics("id_idx");
                Assert.That(stats.NumIndexedRows, Is.EqualTo(4096));
                Assert.That(stats.NumUnIndexedRows, Is.EqualTo(0));
            }
        }
        finally
        {
            Cleanup(uri);
        }
    }
    
    [Test]
    public async Task IndexStatsAsync()
    {
        var uri = new Uri("file:///tmp/test_table_try_index_stats_async");
        try
        {
            using (var cnn = new Connection(uri))
            {
                Assert.That(cnn.IsOpen, Is.True);
                var table = await cnn.CreateTableAsync("table1", Helpers.GetSchema());
                var recordBatch = Helpers.CreateSampleRecordBatch(
                    Helpers.GetSchema(), 4096, 128
                );
                // Note that the interface defines a list, so we'll use that
                var array = new List<RecordBatch>();
                array.Add(recordBatch);
                table.Add(array);
                await table.CreateScalarIndexAsync("id");
                var stats = await table.GetIndexStatisticsAsync("id_idx");
                Assert.That(stats.NumIndexedRows, Is.EqualTo(4096));
                Assert.That(stats.NumUnIndexedRows, Is.EqualTo(0));
            }
        }
        finally
        {
            Cleanup(uri);
        }
    }
    
    [Test]
    public void CreateDefaultFullTextSearchIndex()
    {
        var uri = new Uri("file:///tmp/test_table_try_fts_index");
        try
        {
            using (var cnn = new Connection(uri))
            {
                Assert.That(cnn.IsOpen, Is.True);
                var table = cnn.CreateTable("table1", Helpers.GetSchema());
                var recordBatch = Helpers.CreateSampleRecordBatch(
                    Helpers.GetSchema(), 8, 128
                );
                // Note that the interface defines a list, so we'll use that
                var array = new List<RecordBatch>();
                array.Add(recordBatch);
                table.Add(array);
                table.CreateFtsIndex(["id"], ["id"]);
                table.Optimize();
                var indices = table.ListIndices();
                Assert.That(indices.Count, Is.GreaterThan(0));
            }
        }
        finally
        {
            Cleanup(uri);
        }
    }
    
    [Test]
    public async Task CreateDefaultFullTextSearchIndexAsync()
    {
        var uri = new Uri("file:///tmp/test_table_try_fts_index_async");
        try
        {
            using (var cnn = new Connection(uri))
            {
                Assert.That(cnn.IsOpen, Is.True);
                var table = await cnn.CreateTableAsync("table1", Helpers.GetSchema());
                var recordBatch = Helpers.CreateSampleRecordBatch(
                    Helpers.GetSchema(), 8, 128
                );
                // Note that the interface defines a list, so we'll use that
                var array = new List<RecordBatch>();
                array.Add(recordBatch);
                await table.AddAsync(array);
                await table.CreateFtsIndexAsync(["id"], ["id"]);
                await table.OptimizeAsync();
                var indices = await table.ListIndicesAsync();
                Assert.That(indices.Count, Is.GreaterThan(0));
            }
        }
        finally
        {
            Cleanup(uri);
        }
    }
    
    [Test]
    public void FullTextSearchIndexStats()
    {
        var uri = new Uri("file:///tmp/test_table_try_fts_index_stats");
        try
        {
            using (var cnn = new Connection(uri))
            {
                Assert.That(cnn.IsOpen, Is.True);
                var table = cnn.CreateTable("table1", Helpers.GetSchema());
                var recordBatch = Helpers.CreateSampleRecordBatch(
                    Helpers.GetSchema(), 8, 128
                );
                // Note that the interface defines a list, so we'll use that
                var array = new List<RecordBatch>();
                array.Add(recordBatch);
                table.Add(array);
                table.CreateFtsIndex(["id"], ["id"]);
                table.Optimize();
                var indices = table.ListIndices();
                Assert.That(indices.Count, Is.GreaterThan(0));
                foreach (var indexConfig in indices)
                {
                    var indexStats = table.GetIndexStatistics(indexConfig.Name);
                    Assert.That(indexStats.NumIndexedRows, Is.EqualTo(8));
                    Assert.That(indexStats.IndexType, Is.EqualTo(IndexType.Fts));
                }
            }
        }
        finally
        {
            Cleanup(uri);
        }
    }
    
    [Test]
    public async Task FullTextSearchIndexStatsAsync()
    {
        var uri = new Uri("file:///tmp/test_table_try_fts_index_stats_async");
        try
        {
            using (var cnn = new Connection(uri))
            {
                Assert.That(cnn.IsOpen, Is.True);
                var table = await cnn.CreateTableAsync("table1", Helpers.GetSchema());
                var recordBatch = Helpers.CreateSampleRecordBatch(
                    Helpers.GetSchema(), 8, 128
                );
                // Note that the interface defines a list, so we'll use that
                var array = new List<RecordBatch>();
                array.Add(recordBatch);
                await table.AddAsync(array);
                await table.CreateFtsIndexAsync(["id"], ["id"]);
                await table.OptimizeAsync();
                var indices = await table.ListIndicesAsync();
                Assert.That(indices.Count, Is.GreaterThan(0));
                foreach (var indexConfig in indices)
                {
                    var indexStats = await table.GetIndexStatisticsAsync(indexConfig.Name);
                    Assert.That(indexStats.NumIndexedRows, Is.EqualTo(8));
                    Assert.That(indexStats.IndexType, Is.EqualTo(IndexType.Fts));
                }
            }
        }
        finally
        {
            Cleanup(uri);
        }
    }
    
    [Test]
    public void CreateDefaultVectorIndex()
    {
        var uri = new Uri("file:///tmp/test_table_try_vec_index");
        try
        {
            using (var cnn = new Connection(uri))
            {
                Assert.That(cnn.IsOpen, Is.True);
                var table = cnn.CreateTable("table1", Helpers.GetSchema());
                var recordBatch = Helpers.CreateSampleRecordBatch(
                    Helpers.GetSchema(), 256, 128
                );
                // Note that the interface defines a list, so we'll use that
                var array = new List<RecordBatch>();
                array.Add(recordBatch);
                table.Add(array);
                table.CreateIndex("vector", Metric.L2, 8, 8);
                var stats = table.Optimize();
            }
        }
        finally
        {
            Cleanup(uri);
        }
    }
    
    [Test]
    public async Task CreateDefaultVectorIndexAsync()
    {
        var uri = new Uri("file:///tmp/test_table_try_vec_index_async");
        try
        {
            using (var cnn = new Connection(uri))
            {
                Assert.That(cnn.IsOpen, Is.True);
                var table = await cnn.CreateTableAsync("table1", Helpers.GetSchema());
                var recordBatch = Helpers.CreateSampleRecordBatch(
                    Helpers.GetSchema(), 256, 128
                );
                // Note that the interface defines a list, so we'll use that
                var array = new List<RecordBatch>();
                array.Add(recordBatch);
                await table.AddAsync(array);
                await table.CreateIndexAsync("vector", Metric.L2, 8, 8);
                var stats = await table.OptimizeAsync();
            }
        }
        finally
        {
            Cleanup(uri);
        }
    }
    
    [Test]
    public void Compact()
    {
        var uri = new Uri("file:///tmp/test_table_compact");
        try
        {
            using (var cnn = new Connection(uri))
            {
                Assert.That(cnn.IsOpen, Is.True);
                var table = cnn.CreateTable("table1", Helpers.GetSchema());
                Assert.That(table.IsOpen, Is.True);
                var recordBatch = Helpers.CreateSampleRecordBatch(
                    Helpers.GetSchema(), 1, 128
                );
                // Note that the interface defines a list, so we'll use that
                var array = new List<RecordBatch>();
                array.Add(recordBatch);
                table.Add(array);
                Assert.That(table.CountRows(), Is.GreaterThan(0));
                var results = table.Optimize();
                Assert.That(table.CountRows(), Is.GreaterThan(0));
            }
        }
        finally
        {
            Cleanup(uri);
        }
    }
    
    [Test]
    public async Task CompactAsync()
    {
        var uri = new Uri("file:///tmp/test_table_compact_async");
        try
        {
            using (var cnn = new Connection(uri))
            {
                Assert.That(cnn.IsOpen, Is.True);
                var table = await cnn.CreateTableAsync("table1", Helpers.GetSchema());
                Assert.That(table.IsOpen, Is.True);
                var recordBatch = Helpers.CreateSampleRecordBatch(
                    Helpers.GetSchema(), 1, 128
                );
                // Note that the interface defines a list, so we'll use that
                var array = new List<RecordBatch>();
                array.Add(recordBatch);
                await table.AddAsync(array);
                Assert.That(await table.CountRowsAsync(), Is.GreaterThan(0));
                var results = await table.OptimizeAsync();
                Assert.That(await table.CountRowsAsync(), Is.GreaterThan(0));
            }
        }
        finally
        {
            Cleanup(uri);
        }
    }

    [Test]
    public void AddRowsFromArrowTable()
    {
        var uri = new Uri("file:///tmp/test_table_arrow_table");
        try
        {
            using (var cnn = new Connection(uri))
            {
                Assert.That(cnn.IsOpen, Is.True);
                var table = cnn.CreateTable("table1", Helpers.GetSchema());
                Assert.That(table.IsOpen, Is.True);
                var recordBatch = Helpers.CreateSampleRecordBatch(
                    Helpers.GetSchema(), 1, 128
                );

                // Put recordBatch into a list
                var array = new List<RecordBatch>() { recordBatch };;
                var arrowTable = Apache.Arrow.Table.TableFromRecordBatches(Helpers.GetSchema(), array);
                table.Add(arrowTable);
                Assert.That(table.CountRows(), Is.GreaterThan(0));
            }
        }
        finally
        {
            Cleanup(uri);
        }
        
        Assert.Pass();
    }
    
    [Test]
    public async Task AddRowsFromArrowTableAsync()
    {
        var uri = new Uri("file:///tmp/test_table_arrow_table_async");
        try
        {
            using (var cnn = new Connection(uri))
            {
                Assert.That(cnn.IsOpen, Is.True);
                var table = await cnn.CreateTableAsync("table1", Helpers.GetSchema());
                Assert.That(table.IsOpen, Is.True);
                var recordBatch = Helpers.CreateSampleRecordBatch(
                    Helpers.GetSchema(), 1, 128
                );

                // Put recordBatch into a list
                var array = new List<RecordBatch>() { recordBatch };;
                var arrowTable = Apache.Arrow.Table.TableFromRecordBatches(Helpers.GetSchema(), array);
                await table.AddAsync(arrowTable);
                Assert.That(table.CountRows(), Is.GreaterThan(0));
            }
        }
        finally
        {
            Cleanup(uri);
        }
        
        Assert.Pass();
    }

    [Test]
    public void TestOptimizeRows()
    {
        var uri = new Uri("file:///tmp/test_table_optimize");
        try
        {
            using (var cnn = new Connection(uri))
            {
                Assert.That(cnn.IsOpen, Is.True);
                var table = cnn.CreateTable("table1", Helpers.GetSchema());
                var recordBatch = Helpers.CreateSampleRecordBatch(
                    Helpers.GetSchema(), 8, 128
                );
                // Note that the interface defines a list, so we'll use that
                var array = new List<RecordBatch>();
                array.Add(recordBatch);
                table.Add(array);
                table.CreateScalarIndex("id");
                var stats = table.Optimize(TimeSpan.FromDays(0));
                Assert.That(stats.Compaction, Is.Not.Null);
                Assert.That(stats.Prune, Is.Not.Null);
                Assert.That(stats.Prune.OldVersionsRemoved, Is.EqualTo(2));
            }
        }
        finally
        {
            Cleanup(uri);
        }        
    }
    
    [Test]
    public async Task TestOptimizeRowsAsync()
    {
        var uri = new Uri("file:///tmp/test_table_optimize_async");
        try
        {
            using (var cnn = new Connection(uri))
            {
                Assert.That(cnn.IsOpen, Is.True);
                var table = await cnn.CreateTableAsync("table1", Helpers.GetSchema());

                // Count the files recursively underneath /tmp/test_table_optimize_async
                var countFilesAtStart = Directory.GetFiles("/tmp/test_table_optimize_async", "*.*", SearchOption.AllDirectories).Length;
                Assert.That(countFilesAtStart, Is.EqualTo(2));
                
                // Insert 10 batches of 8 rows each
                string[] columns = new[] { "id" };
                var numEntries = 8;
                for (int i = 0; i < 10; i++)
                {
                    var builder = await table.MergeInsertAsync(columns);
                    var rb = Helpers.CreateSampleRecordBatch(
                        Helpers.GetSchema(), numEntries, 128, i*numEntries
                    );
                    // Note that the interface defines a list, so we'll use that
                    var tmp = new List<RecordBatch>();
                    tmp.Add(rb);
                    await builder.WhenMatchedUpdateAll().WhenNotMatchedInsertAll().ExecuteAsync(tmp);
                    System.Console.WriteLine($"Table 1 row count (expected {numEntries * (i+1)}) actual {await table.CountRowsAsync()}" );
                    Assert.That(await table.CountRowsAsync(), Is.EqualTo(numEntries * (i+1)));
                }
                
                var countFilesPreOptimize = Directory.GetFiles("/tmp/test_table_optimize_async", "*.*", SearchOption.AllDirectories).Length;
                var stats = await table.OptimizeAsync(TimeSpan.FromSeconds(0));
                Assert.That(stats.Compaction, Is.Not.Null);
                Assert.That(stats.Prune, Is.Not.Null);
                // lancedb alway creates 2 more extra files, so it should be 12 files, we use greater than 10 here in case lancedb changes his implementation.
                Assert.That(stats.Prune.OldVersionsRemoved, Is.GreaterThan((10)));
                var countFilesPostOptimize = Directory.GetFiles("/tmp/test_table_optimize_async", "*.*", SearchOption.AllDirectories).Length;
                Assert.That(countFilesPostOptimize, Is.LessThan(countFilesPreOptimize));
                // make sure the data file is only one after compaction and prune.
                var countDataFilesAfterOptimize = Directory.GetFiles("/tmp/test_table_optimize_async/table1.lance/data", "*.*", SearchOption.AllDirectories).Length;
                Assert.That(countDataFilesAfterOptimize, Is.EqualTo(1));
            }
        }
        finally
        {
            Cleanup(uri);
        }        
    }
    
    [Test]
    public void AddRowsBadVectorDrop()
    {
        var uri = new Uri("file:///tmp/test_table_add_rows_bad_vec_drop");
        try
        {
            using (var cnn = new Connection(uri))
            {
                Assert.That(cnn.IsOpen, Is.True);
                var table = cnn.CreateTable("table1", Helpers.GetSchema());
                Assert.That(table.IsOpen, Is.True);

                var array = new List<Dictionary<string, object>>();
                var recordBatch = new Dictionary<string, object>();
                recordBatch.Add("id", new List<string>() { "0" });
                var rowList = new List<float>();
                for (int i = 0; i < 64; i++)
                {
                    rowList.Add(1.0f);
                }
                recordBatch.Add("vector", rowList);
                array.Add(recordBatch);
                table.Add(array, badVectorHandling: BadVectorHandling.Drop);
                Assert.That(table.CountRows(), Is.EqualTo(0));
            }
        }
        finally
        {
            Cleanup(uri);
        }
    }
    
    [Test]
    public void AddRowsBadVectorDropMixed()
    {
        var uri = new Uri("file:///tmp/test_table_add_rows_bad_vec_drop_mix");
        try
        {
            using (var cnn = new Connection(uri))
            {
                Assert.That(cnn.IsOpen, Is.True);
                var table = cnn.CreateTable("table1", Helpers.GetSchema());
                Assert.That(table.IsOpen, Is.True);

                var array = new List<Dictionary<string, object>>();
                
                // First Batch is bad
                var recordBatch = new Dictionary<string, object>();
                recordBatch.Add("id", new List<string>() { "0" });
                var rowList = new List<float>();
                for (int i = 0; i < 64; i++)
                {
                    rowList.Add(1.0f);
                }
                recordBatch.Add("vector", rowList);
                
                // Second Batch is bad
                var recordBatch2 = new Dictionary<string, object>();
                recordBatch2.Add("id", new List<string>() { "1" });
                var rowList2 = new List<float>();
                for (int i = 0; i < 128; i++)
                {
                    rowList2.Add(1.0f);
                }
                recordBatch2.Add("vector", rowList2);
                
                array.Add(recordBatch);
                array.Add(recordBatch2);
                table.Add(array, badVectorHandling: BadVectorHandling.Drop);
                Assert.That(table.CountRows(), Is.EqualTo(1));
            }
        }
        finally
        {
            Cleanup(uri);
        }
    }
    
    [Test]
    public async Task AddRowsBadVectorDropMixedAsync()
    {
        var uri = new Uri("file:///tmp/test_table_add_rows_bad_vec_drop_mix_async");
        try
        {
            using (var cnn = new Connection(uri))
            {
                Assert.That(cnn.IsOpen, Is.True);
                var table = await cnn.CreateTableAsync("table1", Helpers.GetSchema());
                Assert.That(table.IsOpen, Is.True);

                var array = new List<Dictionary<string, object>>();
                
                // First Batch is bad
                var recordBatch = new Dictionary<string, object>();
                recordBatch.Add("id", new List<string>() { "0" });
                var rowList = new List<float>();
                for (int i = 0; i < 64; i++)
                {
                    rowList.Add(1.0f);
                }
                recordBatch.Add("vector", rowList);
                
                // Second Batch is bad
                var recordBatch2 = new Dictionary<string, object>();
                recordBatch2.Add("id", new List<string>() { "1" });
                var rowList2 = new List<float>();
                for (int i = 0; i < 128; i++)
                {
                    rowList2.Add(1.0f);
                }
                recordBatch2.Add("vector", rowList2);
                
                array.Add(recordBatch);
                array.Add(recordBatch2);
                await table.AddAsync(array, badVectorHandling: BadVectorHandling.Drop);
                Assert.That(await table.CountRowsAsync(), Is.EqualTo(1));
            }
        }
        finally
        {
            Cleanup(uri);
        }
    }
    
    [Test]
    public async Task AddRowsBadVectorDropAsync()
    {
        var uri = new Uri("file:///tmp/test_table_add_rows_bad_vec_drop_async");
        try
        {
            using (var cnn = new Connection(uri))
            {
                Assert.That(cnn.IsOpen, Is.True);
                var table = await cnn.CreateTableAsync("table1", Helpers.GetSchema());
                Assert.That(table.IsOpen, Is.True);

                var array = new List<Dictionary<string, object>>();
                var recordBatch = new Dictionary<string, object>();
                recordBatch.Add("id", new List<string>() { "0" });
                var rowList = new List<float>();
                for (int i = 0; i < 64; i++)
                {
                    rowList.Add(1.0f);
                }
                recordBatch.Add("vector", rowList);
                array.Add(recordBatch);
                await table.AddAsync(array, badVectorHandling: BadVectorHandling.Drop);
                Assert.That(await table.CountRowsAsync(), Is.EqualTo(0));
            }
        }
        finally
        {
            Cleanup(uri);
        }
    }
    
    [Test]
    public void AddRowsBadVectorFill()
    {
        var uri = new Uri("file:///tmp/test_table_add_rows_bad_vec_drop");
        try
        {
            using (var cnn = new Connection(uri))
            {
                Assert.That(cnn.IsOpen, Is.True);
                var table = cnn.CreateTable("table1", Helpers.GetSchema());
                Assert.That(table.IsOpen, Is.True);

                var array = new List<Dictionary<string, object>>();
                var recordBatch = new Dictionary<string, object>();
                recordBatch.Add("id", new List<string>() { "0" });
                var rowList = new List<float>();
                for (int i = 0; i < 64; i++)
                {
                    rowList.Add(1.0f);
                }
                recordBatch.Add("vector", rowList);
                array.Add(recordBatch);
                table.Add(array, badVectorHandling: BadVectorHandling.Fill, fillValue: 0.0f);
                Assert.That(table.CountRows(), Is.EqualTo(1));
            }
        }
        finally
        {
            Cleanup(uri);
        }
    }
    
    [Test]
    public async Task AddRowsBadVectorFillAsync()
    {
        var uri = new Uri("file:///tmp/test_table_add_rows_bad_vec_drop_async");
        try
        {
            using (var cnn = new Connection(uri))
            {
                Assert.That(cnn.IsOpen, Is.True);
                var table = await cnn.CreateTableAsync("table1", Helpers.GetSchema());
                Assert.That(table.IsOpen, Is.True);

                var array = new List<Dictionary<string, object>>();
                var recordBatch = new Dictionary<string, object>();
                recordBatch.Add("id", new List<string>() { "0" });
                var rowList = new List<float>();
                for (int i = 0; i < 64; i++)
                {
                    rowList.Add(1.0f);
                }
                recordBatch.Add("vector", rowList);
                array.Add(recordBatch);
                await table.AddAsync(array, badVectorHandling: BadVectorHandling.Fill, fillValue: 0.0f);
                Assert.That(await table.CountRowsAsync(), Is.EqualTo(1));
            }
        }
        finally
        {
            Cleanup(uri);
        }
    }
}