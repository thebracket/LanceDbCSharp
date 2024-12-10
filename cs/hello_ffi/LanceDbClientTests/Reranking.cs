using Apache.Arrow;
using Apache.Arrow.Types;
using LanceDbClient;
using LanceDbInterface;
using MathNet.Numerics.LinearAlgebra;
using MathNet.Numerics.LinearAlgebra.Single;
using Array = Apache.Arrow.Array;

namespace LanceDbClientTests;

public partial class Tests
{
    [Test]
    public void RerankLimit()
    {
        var uri = new Uri("file:///tmp/test_open_table_vec_query_select");
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
                table.CreateFtsIndex(["id"], ["id"]);
                var stats = table.Optimize();

                var vector = new List<float>();
                for (var i = 0; i < 128; i++)
                {
                    vector.Add(1.0f);
                }

                var testRrf = table
                    .Search(vector, "vector", queryType: QueryType.Hybrid)
                    .SelectColumns(["id", "vector"]);
                var testRrf2 = testRrf
                    .Rerank(new RrfReranker())
                    .Text("'1'")
                    .Limit(3);
                var testRrf3 = testRrf2.ToList();
                Assert.That(testRrf3.Count(), Is.EqualTo(3));
            }
        }
        finally
        {
            Cleanup(uri);
        }

        Assert.Pass();
    }
    
    [Test]
    // Creating a text element and THEN promoting to vector wasn't being passed along correctly
    public async Task RerankLimitAsync()
    {
        var uri = new Uri("file:///tmp/test_open_table_vec_query_select");
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
                table.CreateFtsIndex(["id"], ["id"]);
                var stats = table.Optimize();

                var vector = new List<float>();
                for (var i = 0; i < 128; i++)
                {
                    vector.Add(1.0f);
                }

                var testRrf = await table
                    .Search()
                    .Text("'1'")
                    .Vector(vector)
                    .SelectColumns(["id", "vector"])
                    .Rerank(new RrfReranker())
                    .Limit(3)
                    .ToListAsync();
                Assert.That(testRrf.Count(), Is.EqualTo(3));
            }
        }
        finally
        {
            Cleanup(uri);
        }

        Assert.Pass();
    }
    
    [Test]
    public void RerankFieldOrder()
    {
        var uri = new Uri("file:///tmp/test_open_table_vec_query_select");
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
                table.CreateFtsIndex(["id"], ["id"]);
                var stats = table.Optimize();

                var vector = new List<float>();
                for (var i = 0; i < 128; i++)
                {
                    vector.Add(1.0f);
                }

                var testHybrid = ((HybridQueryBuilder)(table.Search(vector, "vector", queryType: QueryType.Hybrid)))
                    .Metric(Metric.Dot)
                    .NProbes(10)
                    .RefineFactor(10)
                    .SelectColumns(["vector", "id" ])
                    .Text("'1'")
                    .Rerank(new RrfReranker())
                    .Limit(7)
                    .ToBatches(3);
            }
        }
        finally
        {
            Cleanup(uri);
        }

        Assert.Pass();
    }
}