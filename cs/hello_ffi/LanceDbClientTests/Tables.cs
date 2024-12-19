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
                Assert.Throws<Exception>(() => table.Close());
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
                var stats = table.Optimize();
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
                Assert.That(stats.Compaction, Is.Null);
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
        var uri = new Uri("file:///tmp/test_table_optimize");
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
                table.Add(array);
                await table.CreateScalarIndexAsync("id");
                var stats = await table.OptimizeAsync(TimeSpan.FromDays(0));
                Assert.That(stats.Compaction, Is.Null);
                Assert.That(stats.Prune, Is.Not.Null);
                Assert.That(stats.Prune.OldVersionsRemoved, Is.EqualTo(2));
            }
        }
        finally
        {
            Cleanup(uri);
        }        
    }
}