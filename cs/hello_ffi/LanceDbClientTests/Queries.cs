using Apache.Arrow;
using LanceDbClient;

namespace LanceDbClientTests;

public partial class Tests
{
    [Test]
    public void CreateEmptyQuery()
    {
        var uri = new Uri("file:///tmp/test_open_table_empty_query");
        try
        {
            using (var cnn = new Connection(uri))
            {
                Assert.That(cnn.IsOpen, Is.True);
                var table = cnn.CreateTable("table1", Helpers.GetSchema());
                Assert.Multiple(() =>
                {
                    Assert.That(table, Is.Not.Null);
                    Assert.That(cnn.TableNames(), Does.Contain("table1"));
                });
            }

            using (var cnn = new Connection(uri))
            {
                Assert.That(cnn.IsOpen, Is.True);
                var table = cnn.OpenTable("table1");
                Assert.That(table, Is.Not.Null);
                Assert.That(table.Name, Is.EqualTo("table1"));
                Assert.That(table.Search(), Is.Not.Null);
            }
        }
        finally
        {
            Cleanup(uri);
        }

        Assert.Pass();
    }
    
    [Test]
    public void MinimalDumpQuery()
    {
        var uri = new Uri("file:///tmp/test_open_table_dump_query");
        try
        {
            using (var cnn = new Connection(uri))
            {
                Assert.That(cnn.IsOpen, Is.True);
                var table = cnn.CreateTable("table1", Helpers.GetSchema());
                Assert.Multiple(() =>
                {
                    Assert.That(table, Is.Not.Null);
                    Assert.That(cnn.TableNames(), Does.Contain("table1"));
                });
                
                var recordBatch = Helpers.CreateSampleRecordBatch(
                    Helpers.GetSchema(), 8, 128
                );
                // Note that the interface defines a list, so we'll use that
                var array = new List<RecordBatch>();
                array.Add(recordBatch);
                table.Add(array);

                var q = table.Search();
                Assert.That(q, Is.Not.Null);
                var batches = q.ToBatches(0);
                Assert.That(batches, Is.Not.Empty);
                Assert.Multiple(() =>
                {
                    Assert.That(batches.Count(), Is.EqualTo(1));
                    Assert.That(batches.First().Column(0).Length, Is.EqualTo(8));
                });
                var length = batches.Sum(batch => batch.Length);
                Assert.That(length, Is.EqualTo(8));
            }
        }
        finally
        {
            Cleanup(uri);
        }

        Assert.Pass();
    }
    
    [Test]
    public void MinimalDumpQueryWithLimit()
    {
        var uri = new Uri("file:///tmp/test_open_table_dump_query");
        try
        {
            using (var cnn = new Connection(uri))
            {
                Assert.That(cnn.IsOpen, Is.True);
                var table = cnn.CreateTable("table1", Helpers.GetSchema());
                Assert.Multiple(() =>
                {
                    Assert.That(table, Is.Not.Null);
                    Assert.That(cnn.TableNames(), Does.Contain("table1"));
                });
                
                var recordBatch = Helpers.CreateSampleRecordBatch(
                    Helpers.GetSchema(), 200, 128
                );
                // Note that the interface defines a list, so we'll use that
                var array = new List<RecordBatch>();
                array.Add(recordBatch);
                table.Add(array);

                var q = table.Search().Limit(1);
                Assert.That(q, Is.Not.Null);
                var batches = q.ToBatches(0);
                Assert.That(batches, Is.Not.Empty);
                Assert.Multiple(() =>
                {
                    Assert.That(batches.Count(), Is.EqualTo(1));
                    Assert.That(batches.First().Column(0).Length, Is.EqualTo(1));
                });
                var length = batches.Sum(batch => batch.Length);
                Assert.That(length, Is.EqualTo(1));
            }
        }
        finally
        {
            Cleanup(uri);
        }

        Assert.Pass();
    }
    
    [Test]
    public void MinimalDumpQueryWithWhere()
    {
        var uri = new Uri("file:///tmp/test_open_table_dump_query_where");
        try
        {
            using (var cnn = new Connection(uri))
            {
                Assert.That(cnn.IsOpen, Is.True);
                var table = cnn.CreateTable("table1", Helpers.GetSchema());
                Assert.Multiple(() =>
                {
                    Assert.That(table, Is.Not.Null);
                    Assert.That(cnn.TableNames(), Does.Contain("table1"));
                });
                
                var recordBatch = Helpers.CreateSampleRecordBatch(
                    Helpers.GetSchema(), 8, 128
                );
                // Note that the interface defines a list, so we'll use that
                var array = new List<RecordBatch>();
                array.Add(recordBatch);
                table.Add(array);

                var q = table.Search().WhereClause("id = 1");
                Assert.That(q, Is.Not.Null);
                var batches = q.ToBatches(0);
                Assert.That(batches, Is.Not.Empty);
                Assert.Multiple(() =>
                {
                    Assert.That(batches.Count(), Is.EqualTo(1));
                    Assert.That(batches.First().Column(0).Length, Is.EqualTo(1));
                });
                var length = batches.Sum(batch => batch.Length);
                Assert.That(length, Is.EqualTo(1));
            }
        }
        finally
        {
            Cleanup(uri);
        }

        Assert.Pass();
    }
    
    [Test]
    public void MinimalDumpQueryWithRowId()
    {
        // Currently always fails. Running `.with_rowid()` on the server side
        // appears to ONLY return row IDs at present. The result fails to
        // deserialize as a batch - so there's an edge case here?
        //
        // Currently, this asserts that the edge case throws correctly.
        
        var uri = new Uri("file:///tmp/test_open_table_dump_query_where");
        try
        {
            using (var cnn = new Connection(uri))
            {
                Assert.That(cnn.IsOpen, Is.True);
                var table = cnn.CreateTable("table1", Helpers.GetSchema());
                Assert.Multiple(() =>
                {
                    Assert.That(table, Is.Not.Null);
                    Assert.That(cnn.TableNames(), Does.Contain("table1"));
                });
                
                var recordBatch = Helpers.CreateSampleRecordBatch(
                    Helpers.GetSchema(), 8, 128
                );
                // Note that the interface defines a list, so we'll use that
                var array = new List<RecordBatch>();
                array.Add(recordBatch);
                table.Add(array);

                var q = table.Search().WithRowId(true);
                Assert.That(q, Is.Not.Null);
                Assert.Throws<Exception>(() =>
                {
                    var batches = q.ToBatches(0);
                });
            }
        }
        finally
        {
            Cleanup(uri);
        }
        
        Assert.Pass();
    }
    
    [Test]
    public void MinimalAsTableQuery()
    {
        var uri = new Uri("file:///tmp/test_open_table_dump_query_table");
        try
        {
            using (var cnn = new Connection(uri))
            {
                Assert.That(cnn.IsOpen, Is.True);
                var table = cnn.CreateTable("table1", Helpers.GetSchema());
                Assert.Multiple(() =>
                {
                    Assert.That(table, Is.Not.Null);
                    Assert.That(cnn.TableNames(), Does.Contain("table1"));
                });
                
                var recordBatch = Helpers.CreateSampleRecordBatch(
                    Helpers.GetSchema(), 8, 128
                );
                // Note that the interface defines a list, so we'll use that
                var array = new List<RecordBatch>();
                array.Add(recordBatch);
                table.Add(array);

                var q = table.Search();
                Assert.That(q, Is.Not.Null);
                var newTable = q.ToArrow();
                Assert.That(newTable.ColumnCount, Is.EqualTo(2));
                Assert.That(newTable.RowCount, Is.EqualTo(8));
            }
        }
        finally
        {
            Cleanup(uri);
        }

        Assert.Pass();
    }
    
    [Test]
    public void Explain()
    {
        var uri = new Uri("file:///tmp/test_open_table_dump_query_explain");
        try
        {
            using (var cnn = new Connection(uri))
            {
                Assert.That(cnn.IsOpen, Is.True);
                var table = cnn.CreateTable("table1", Helpers.GetSchema());
                Assert.Multiple(() =>
                {
                    Assert.That(table, Is.Not.Null);
                    Assert.That(cnn.TableNames(), Does.Contain("table1"));
                });
                
                var recordBatch = Helpers.CreateSampleRecordBatch(
                    Helpers.GetSchema(), 8, 128
                );
                // Note that the interface defines a list, so we'll use that
                var array = new List<RecordBatch>();
                array.Add(recordBatch);
                table.Add(array);

                var explanation = table.Search().WhereClause("id = 1").Limit(1).ExplainPlan();
                TestContext.Out.WriteLine(explanation);
                Assert.That(explanation, Is.Not.Null);
                Assert.That(explanation, Is.Not.EqualTo("No explanation returned"));
                // NUnit print the explanation
            }
        }
        finally
        {
            Cleanup(uri);
        }

        Assert.Pass();
    }
    
    [Test]
    public void MinimalDumpQueryWithSelect()
    {
        var uri = new Uri("file:///tmp/test_open_table_dump_query_select");
        try
        {
            using (var cnn = new Connection(uri))
            {
                Assert.That(cnn.IsOpen, Is.True);
                var table = cnn.CreateTable("table1", Helpers.GetSchema());
                Assert.Multiple(() =>
                {
                    Assert.That(table, Is.Not.Null);
                    Assert.That(cnn.TableNames(), Does.Contain("table1"));
                });
                
                var recordBatch = Helpers.CreateSampleRecordBatch(
                    Helpers.GetSchema(), 8, 128
                );
                // Note that the interface defines a list, so we'll use that
                var array = new List<RecordBatch>();
                array.Add(recordBatch);
                table.Add(array);

                var q = table.Search().SelectColumns(["id"]);
                Assert.That(q, Is.Not.Null);
                var batches = q.ToBatches(0);
                Assert.That(batches, Is.Not.Empty);
                Assert.Multiple(() =>
                {
                    Assert.That(batches.Count(), Is.EqualTo(1));
                    Assert.That(batches.First().Column(0).Length, Is.EqualTo(8));
                });
                var length = batches.Sum(batch => batch.Length);
                Assert.That(length, Is.EqualTo(8));
                
                foreach (var batch in batches)
                {
                    Assert.That(batch.ColumnCount, Is.EqualTo(1));
                }
            }
        }
        finally
        {
            Cleanup(uri);
        }

        Assert.Pass();
    }
}