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
                Assert.That(batches.Count(), Is.EqualTo(1));
                Assert.That(batches.First().Column(0).Length, Is.EqualTo(8));
            }
        }
        finally
        {
            Cleanup(uri);
        }

        Assert.Pass();
    }
}