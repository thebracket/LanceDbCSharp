using Apache.Arrow;
using Apache.Arrow.Types;
using LanceDbClient;
using MathNet.Numerics.LinearAlgebra;
using MathNet.Numerics.LinearAlgebra.Single;
using Array = Apache.Arrow.Array;

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
    public void MinimalArrowArrayQuery()
    {
        var uri = new Uri("file:///tmp/test_open_table_arrow_array_query");
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

                // Build an Arrow Array of 128 floats equal to 1.0
                var builder = new FloatArray.Builder();

                // Append 128 values of 1.0f to the builder
                for (int i = 0; i < 128; i++)
                {
                    builder.Append(1.0f);
                }

                // Build the FloatArray from the builder
                var arrowVector = builder.Build();
                var result = table.Search(arrowVector, "vector").ToBatches(0);
                Assert.That(result, Is.Not.Null);
                Assert.That(result, Is.Not.Empty);
            }
        }
        finally
        {
            Cleanup(uri);
        }

        Assert.Pass();
    }
    
    [Test]
    public void MinimalListQuery()
    {
        var uri = new Uri("file:///tmp/test_open_table_list_query");
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

                // Build a list of 128 floats equal to 1.0
                var target = new List<float>();
                for (int i = 0; i < 128; i++)
                {
                    target.Add(1.0f);
                }
                var result = table.Search(target, "vector").ToBatches(0);
                Assert.That(result, Is.Not.Null);
                Assert.That(result, Is.Not.Empty);
            }
        }
        finally
        {
            Cleanup(uri);
        }

        Assert.Pass();
    }
    
    [Test]
    public void MinimalVectorQuery()
    {
        var uri = new Uri("file:///tmp/test_open_table_vector_query");
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

                // Build a C# Vector of 128 values, all equal to 1.0f
                var target = new List<float>();
                for (int i = 0; i < 128; i++)
                {
                    target.Add(1.0f);
                }
                Vector<float> vector = new DenseVector(target.ToArray());
                var result = table.Search(vector, "vector").ToBatches(0);
                Assert.That(result, Is.Not.Null);
                Assert.That(result, Is.Not.Empty);
            }
        }
        finally
        {
            Cleanup(uri);
        }

        Assert.Pass();
    }
    
    [Test]
    public void MinimalMatrixQuery()
    {
        var uri = new Uri("file:///tmp/test_open_table_matrix_query");
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

                // Build a C# Vector of 128 values, all equal to 1.0f
                var target = new List<float>();
                for (int i = 0; i < 128; i++)
                {
                    target.Add(1.0f);
                }
                // Convert to a mathnet Matrix type
                Matrix<float> matrix = new DenseMatrix(1, 128, target.ToArray());
                var result = table.Search(matrix, "vector").ToBatches(0);
                Assert.That(result, Is.Not.Null);
                Assert.That(result, Is.Not.Empty);
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

                var q = table.Search().WhereClause("id = '1'");
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

                var q = table.Search().WithRowId(true).ToBatches(0);
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
    public void MinimalListWithLimit()
    {
        var uri = new Uri("file:///tmp/test_open_table_dump_query_list_limit");
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

                for (var limit = 1; limit < 8; limit++)
                {
                    var q = table.Search().Limit(limit);
                    Assert.That(q, Is.Not.Null);
                    var newList = q.ToList();
                    Assert.That(newList, Is.Not.Null);
                    Assert.That(newList, Is.Not.Empty);
                    Assert.That(newList.Count(), Is.EqualTo(limit));
                }
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

                var explanation = table.Search().WhereClause("id = '1'").Limit(1).ExplainPlan();
                TestContext.Out.WriteLine(explanation);
                Assert.That(explanation, Is.Not.Null);
                Assert.That(explanation, Is.Not.EqualTo("No explanation returned"));
                // NUnit print the explanation
                TestContext.Out.WriteLine(explanation);
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
    
    [Test]
    public void FullTextSearchWithIndex()
    {
        var uri = new Uri("file:///tmp/test_table_try_fts_index_search");
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
                var search = table.Search().Text("'1'").ToList();
                foreach (var row in search)
                {
                    // Print out each key and value
                    foreach (var keyValuePair in row)
                    {
                        TestContext.Out.WriteLine(keyValuePair.Key + ": " + keyValuePair.Value);
                    }
                }
                Assert.That(search, Is.Not.Null);
            }
        }
        finally
        {
            Cleanup(uri);
        }
    }
    
    [Test]
    public void BasicVectorQuery()
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

                var target = new List<float>();
                for (var i=0; i<128; i++) target.Add(1.0f);
                var batches = table.Search().Vector(target).SelectColumns(["id", "vector"]).ToBatches(0);
                Assert.That(batches, Is.Not.Empty);
            }
        }
        finally
        {
            Cleanup(uri);
        }

        Assert.Pass();
    }
    
    [Test]
    public async Task BatchSizeVectorQuery()
    {
        var uri = new Uri("file:///tmp/test_open_table_vec_query_select_batchsize");
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
                    Helpers.GetSchema(), 8, 4096
                );
                // Note that the interface defines a list, so we'll use that
                var array = new List<RecordBatch>();
                array.Add(recordBatch);
                table.Add(array);

                var target = new List<float>();
                for (var i=0; i<128; i++) target.Add(1.0f);
                int batchCount = 0;
                await foreach (var batch in table.Search().Vector(target).SelectColumns(["id"]).ToBatchesAsync(1))
                {
                    batchCount++;
                }
                Assert.That(batchCount, Is.EqualTo(1));
            }
        }
        finally
        {
            Cleanup(uri);
        }

        Assert.Pass();
    }
    
    [Test]
    public void ExplainVecQuery()
    {
        var uri = new Uri("file:///tmp/test_open_table_vec_query_explain");
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

                var target = new List<float>();
                for (var i=0; i<128; i++) target.Add(1.0f);
                var explanation = table.Search().Vector(target).SelectColumns(["id", "vector"]).ExplainPlan();
                TestContext.Out.WriteLine(explanation);
                Assert.That(explanation, Is.Not.Null);
                Assert.That(explanation, Is.Not.EqualTo("No explanation returned"));
                // NUnit print the explanation
                TestContext.Out.WriteLine(explanation);
            }
        }
        finally
        {
            Cleanup(uri);
        }

        Assert.Pass();
    }
}