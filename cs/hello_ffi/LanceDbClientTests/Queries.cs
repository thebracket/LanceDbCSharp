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
    public async Task CreateEmptyQueryAsync()
    {
        var uri = new Uri("file:///tmp/test_open_table_empty_query_async");
        try
        {
            using (var cnn = new Connection(uri))
            {
                Assert.That(cnn.IsOpen, Is.True);
                var table = await cnn.CreateTableAsync("table1", Helpers.GetSchema());
                Assert.Multiple(() =>
                {
                    Assert.That(table, Is.Not.Null);
                    Assert.That(cnn.TableNames(), Does.Contain("table1"));
                });
            }

            using (var cnn = new Connection(uri))
            {
                Assert.That(cnn.IsOpen, Is.True);
                var table = await cnn.OpenTableAsync("table1");
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
    public async Task MinimalDumpQueryAsync()
    {
        var uri = new Uri("file:///tmp/test_open_table_dump_query_async");
        try
        {
            using (var cnn = new Connection(uri))
            {
                Assert.That(cnn.IsOpen, Is.True);
                var table = await cnn.CreateTableAsync("table1", Helpers.GetSchema());
                Assert.Multiple(async () =>
                {
                    Assert.That(table, Is.Not.Null);
                    Assert.That(await cnn.TableNamesAsync(), Does.Contain("table1"));
                });
                
                var recordBatch = Helpers.CreateSampleRecordBatch(
                    Helpers.GetSchema(), 8, 128
                );
                // Note that the interface defines a list, so we'll use that
                var array = new List<RecordBatch>();
                array.Add(recordBatch);
                await table.AddAsync(array);

                var q = table.Search();
                Assert.That(q, Is.Not.Null);
                // Async enumerable, to a concereate list
                var batches = new List<RecordBatch>();
                await foreach (var batch in q.ToBatchesAsync(0))
                {
                    batches.Add(batch);
                }
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
    public async Task MinimalArrowArrayQueryAsync()
    {
        var uri = new Uri("file:///tmp/test_open_table_arrow_array_query_async");
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

                // Build an Arrow Array of 128 floats equal to 1.0
                var builder = new FloatArray.Builder();

                // Append 128 values of 1.0f to the builder
                for (int i = 0; i < 128; i++)
                {
                    builder.Append(1.0f);
                }

                // Build the FloatArray from the builder
                var arrowVector = builder.Build();
                var result = new List<RecordBatch>();
                await foreach (var batch in table.Search(arrowVector, "vector").ToBatchesAsync(0))
                {
                    result.Add(batch);
                }
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
    public async Task MinimalListQueryAsync()
    {
        var uri = new Uri("file:///tmp/test_open_table_list_query_async");
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

                // Build a list of 128 floats equal to 1.0
                var target = new List<float>();
                for (int i = 0; i < 128; i++)
                {
                    target.Add(1.0f);
                }
                var result = new List<RecordBatch>();
                await foreach (var batch in table.Search(target, "vector").ToBatchesAsync(0))
                {
                    result.Add(batch);
                }
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
    public async Task MinimalVectorQueryAsync()
    {
        var uri = new Uri("file:///tmp/test_open_table_vector_query_async");
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

                // Build a C# Vector of 128 values, all equal to 1.0f
                var target = new List<float>();
                for (int i = 0; i < 128; i++)
                {
                    target.Add(1.0f);
                }
                Vector<float> vector = new DenseVector(target.ToArray());
                var result = new List<RecordBatch>();
                await foreach (var batch in table.Search(vector, "vector").ToBatchesAsync(0))
                {
                    result.Add(batch);
                }
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
                
                // This is deliberately throwing a NotImplementedException
                // because the current LanceDB system does not support
                // searching for multiple vectors at once.
                Assert.Throws<NotImplementedException>(() =>
                {
                    var result = table.Search(matrix, "vector").ToBatches(0);
                });
                //Assert.That(result, Is.Not.Null);
                //Assert.That(result, Is.Not.Empty);
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
    public async Task MinimalDumpQueryWithLimitAsync()
    {
        var uri = new Uri("file:///tmp/test_open_table_dump_query_async");
        try
        {
            using (var cnn = new Connection(uri))
            {
                Assert.That(cnn.IsOpen, Is.True);
                var table = await cnn.CreateTableAsync("table1", Helpers.GetSchema());
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
                await table.AddAsync(array);

                var q = table.Search().Limit(1);
                Assert.That(q, Is.Not.Null);
                var batches = new List<RecordBatch>();
                await foreach (var batch in q.ToBatchesAsync(0))
                {
                    batches.Add(batch);
                }
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
    public async Task MinimalDumpQueryWithWhereAsync()
    {
        var uri = new Uri("file:///tmp/test_open_table_dump_query_where_async");
        try
        {
            using (var cnn = new Connection(uri))
            {
                Assert.That(cnn.IsOpen, Is.True);
                var table = await cnn.CreateTableAsync("table1", Helpers.GetSchema());
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
                await table.AddAsync(array);

                var q = table.Search().WhereClause("id = '1'");
                Assert.That(q, Is.Not.Null);
                var batches = new List<RecordBatch>();
                await foreach (var batch in q.ToBatchesAsync(0))
                {
                    batches.Add(batch);
                }
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
                foreach (var q2 in q)
                {
                    Assert.That(q2.Schema.GetFieldByName("_rowid"), Is.Not.Null);
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
    public async Task MinimalDumpQueryWithRowIdAsync()
    {
        // Currently always fails. Running `.with_rowid()` on the server side
        // appears to ONLY return row IDs at present. The result fails to
        // deserialize as a batch - so there's an edge case here?
        //
        // Currently, this asserts that the edge case throws correctly.
        
        var uri = new Uri("file:///tmp/test_open_table_dump_query_where_async");
        try
        {
            using (var cnn = new Connection(uri))
            {
                Assert.That(cnn.IsOpen, Is.True);
                var table = await cnn.CreateTableAsync("table1", Helpers.GetSchema());
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
                await table.AddAsync(array);

                await foreach(var q in table.Search().WithRowId(true).ToBatchesAsync(0))
                {
                    Assert.That(q.Schema.GetFieldByName("_rowid"), Is.Not.Null);
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
    public async Task MinimalAsTableQueryAsync()
    {
        var uri = new Uri("file:///tmp/test_open_table_dump_query_table_async");
        try
        {
            using (var cnn = new Connection(uri))
            {
                Assert.That(cnn.IsOpen, Is.True);
                var table = await cnn.CreateTableAsync("table1", Helpers.GetSchema());
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
                await table.AddAsync(array);

                var q = table.Search();
                Assert.That(q, Is.Not.Null);
                var newTable = await q.ToArrowAsync();
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
    public async Task MinimalListWithLimitAsync()
    {
        var uri = new Uri("file:///tmp/test_open_table_dump_query_list_limit_async");
        try
        {
            using (var cnn = new Connection(uri))
            {
                Assert.That(cnn.IsOpen, Is.True);
                var table = await cnn.CreateTableAsync("table1", Helpers.GetSchema());
                Assert.Multiple(async () =>
                {
                    Assert.That(table, Is.Not.Null);
                    Assert.That(await cnn.TableNamesAsync(), Does.Contain("table1"));
                });
                
                var recordBatch = Helpers.CreateSampleRecordBatch(
                    Helpers.GetSchema(), 8, 128
                );
                // Note that the interface defines a list, so we'll use that
                var array = new List<RecordBatch>();
                array.Add(recordBatch);
                await table.AddAsync(array);

                for (var limit = 1; limit < 8; limit++)
                {
                    var q = table.Search().Limit(limit);
                    Assert.That(q, Is.Not.Null);
                    var newList = await q.ToListAsync();
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
    public async Task MinimalDumpQueryWithSelectAsync()
    {
        var uri = new Uri("file:///tmp/test_open_table_dump_query_select_async");
        try
        {
            using (var cnn = new Connection(uri))
            {
                Assert.That(cnn.IsOpen, Is.True);
                var table = await cnn.CreateTableAsync("table1", Helpers.GetSchema());
                Assert.Multiple(async () =>
                {
                    Assert.That(table, Is.Not.Null);
                    Assert.That(await cnn.TableNamesAsync(), Does.Contain("table1"));
                });
                
                var recordBatch = Helpers.CreateSampleRecordBatch(
                    Helpers.GetSchema(), 8, 128
                );
                // Note that the interface defines a list, so we'll use that
                var array = new List<RecordBatch>();
                array.Add(recordBatch);
                await table.AddAsync(array);

                var q = table.Search().SelectColumns(["id"]);
                Assert.That(q, Is.Not.Null);
                var batches = new List<RecordBatch>();
                await foreach (var batch in q.ToBatchesAsync(0))
                {
                    batches.Add(batch);
                }
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
    public async Task FullTextSearchWithIndexAsync()
    {
        var uri = new Uri("file:///tmp/test_table_try_fts_index_search_async");
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
                var search = await table.Search().Text("'1'").ToListAsync();
                foreach (var row in search)
                {
                    // Print out each key and value
                    foreach (var keyValuePair in row)
                    {
                        await TestContext.Out.WriteLineAsync(keyValuePair.Key + ": " + keyValuePair.Value);
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
    
    /// <summary>
    /// Check that basic vector queries include the _distance field.
    /// </summary>
    [Test]
    public void BasicVectorQueryIncludesDistance()
    {
        var uri = new Uri("file:///tmp/test_open_table_vec_query_select_distance");
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
                var array = new List<RecordBatch>();
                array.Add(recordBatch);
                table.Add(array);

                var target = new List<float>();
                for (var i=0; i<128; i++) target.Add(1.0f);
                var batches = table.Search().Vector(target).SelectColumns(["id", "vector"]).ToBatches(0);
                Assert.That(batches, Is.Not.Empty);
                foreach (var batch in batches)
                {
                    Assert.That(batch, Is.Not.Null);
                    var foundDistance = false;
                    foreach (var field in batch.Schema.FieldsList)
                    {
                        if (field.Name == "_distance")
                        {
                            foundDistance = true;
                            break;
                        }
                    }
                    Assert.That(foundDistance, Is.True);
                }
            }
        }
        finally
        {
            Cleanup(uri);
        }

        Assert.Pass();
    }
    
    /// <summary>
    /// Check that basic vector queries include the _distance field in the
    /// C# mapping.
    /// </summary>
    [Test]
    public void BasicVectorQueryIncludesDistanceMap()
    {
        var uri = new Uri("file:///tmp/test_open_table_vec_query_select_distance");
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
                var array = new List<RecordBatch>();
                array.Add(recordBatch);
                table.Add(array);

                var target = new List<float>();
                for (var i = 0; i < 128; i++) target.Add(1.0f);
                var listDict = table.Search().Vector(target).SelectColumns(["id", "vector"]).ToList();
                Assert.That(listDict, Is.Not.Empty);
                foreach (var row in listDict)
                {
                    Assert.That(row.ContainsKey("_distance"));
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
    public async Task BasicVectorQueryAsync()
    {
        var uri = new Uri("file:///tmp/test_open_table_vec_query_select_async");
        try
        {
            using (var cnn = new Connection(uri))
            {
                Assert.That(cnn.IsOpen, Is.True);
                var table = await cnn.CreateTableAsync("table1", Helpers.GetSchema());
                Assert.Multiple(async () =>
                {
                    Assert.That(table, Is.Not.Null);
                    Assert.That(await cnn.TableNamesAsync(), Does.Contain("table1"));
                });
                
                var recordBatch = Helpers.CreateSampleRecordBatch(
                    Helpers.GetSchema(), 8, 128
                );
                // Note that the interface defines a list, so we'll use that
                var array = new List<RecordBatch>();
                array.Add(recordBatch);
                await table.AddAsync(array);

                var target = new List<float>();
                for (var i=0; i<128; i++) target.Add(1.0f);
                var batches = new List<RecordBatch>();
                await foreach (var batch in table.Search().Vector(target).SelectColumns(["id", "vector"]).ToBatchesAsync(0))
                {
                    batches.Add(batch);
                }
                Assert.That(batches, Is.Not.Empty);
            }
        }
        finally
        {
            Cleanup(uri);
        }

        Assert.Pass();
    }
    
    /// <summary>
    /// Check that basic vector queries include the _distance field in
    /// the C# mapping.
    /// </summary>
    [Test]
    public async Task BasicVectorQueryAsyncIncludesDistanceMap()
    {
        var uri = new Uri("file:///tmp/test_open_table_vec_query_select_async_distance");
        try
        {
            using (var cnn = new Connection(uri))
            {
                Assert.That(cnn.IsOpen, Is.True);
                var table = await cnn.CreateTableAsync("table1", Helpers.GetSchema());
                Assert.Multiple(async () =>
                {
                    Assert.That(table, Is.Not.Null);
                    Assert.That(await cnn.TableNamesAsync(), Does.Contain("table1"));
                });
                
                var recordBatch = Helpers.CreateSampleRecordBatch(
                    Helpers.GetSchema(), 8, 128
                );
                // Note that the interface defines a list, so we'll use that
                var array = new List<RecordBatch>();
                array.Add(recordBatch);
                await table.AddAsync(array);

                var target = new List<float>();
                for (var i=0; i<128; i++) target.Add(1.0f);

                var rows = await table.Search().Vector(target).SelectColumns(["id", "vector"]).ToListAsync();
                Assert.That(rows, Is.Not.Null);
                foreach (var row in rows)
                {
                    Assert.That(row.ContainsKey("_distance"));
                }
            }
        }
        finally
        {
            Cleanup(uri);
        }

        Assert.Pass();
    }
    
    /// <summary>
    /// Check that basic vector queries include the _distance field.
    /// </summary>
    [Test]
    public async Task BasicVectorQueryAsyncIncludesDistance()
    {
        var uri = new Uri("file:///tmp/test_open_table_vec_query_select_async_distance");
        try
        {
            using (var cnn = new Connection(uri))
            {
                Assert.That(cnn.IsOpen, Is.True);
                var table = await cnn.CreateTableAsync("table1", Helpers.GetSchema());
                Assert.Multiple(async () =>
                {
                    Assert.That(table, Is.Not.Null);
                    Assert.That(await cnn.TableNamesAsync(), Does.Contain("table1"));
                });
                
                var recordBatch = Helpers.CreateSampleRecordBatch(
                    Helpers.GetSchema(), 8, 128
                );
                // Note that the interface defines a list, so we'll use that
                var array = new List<RecordBatch>();
                array.Add(recordBatch);
                await table.AddAsync(array);

                var target = new List<float>();
                for (var i=0; i<128; i++) target.Add(1.0f);
                var batches = new List<RecordBatch>();
                await foreach (var batch in table.Search().Vector(target).SelectColumns(["id", "vector"]).ToBatchesAsync(0))
                {
                    batches.Add(batch);
                }
                Assert.That(batches, Is.Not.Empty);
                foreach (var batch in batches)
                {
                    Assert.That(batch, Is.Not.Null);
                    var foundDistance = false;
                    foreach (var field in batch.Schema.FieldsList)
                    {
                        if (field.Name == "_distance")
                        {
                            foundDistance = true;
                            break;
                        }
                    }
                    Assert.That(foundDistance, Is.True);
                }
            }
        }
        finally
        {
            Cleanup(uri);
        }

        Assert.Pass();
    }
    
    /// <summary>
    /// Check that the distance range feature works.
    /// </summary>
    [Test]
    public async Task BasicVectorQueryAsyncDistanceRange()
    {
        var uri = new Uri("file:///tmp/test_open_table_vec_query_select_async_distance_range");
        try
        {
            using (var cnn = new Connection(uri))
            {
                Assert.That(cnn.IsOpen, Is.True);
                var table = await cnn.CreateTableAsync("table1", Helpers.GetSchema());
                Assert.Multiple(async () =>
                {
                    Assert.That(table, Is.Not.Null);
                    Assert.That(await cnn.TableNamesAsync(), Does.Contain("table1"));
                });
                
                var recordBatch = Helpers.CreateSampleRecordBatch(
                    Helpers.GetSchema(), 8, 128, increaseSample:true
                );
                // Note that the interface defines a list, so we'll use that
                var array = new List<RecordBatch>();
                array.Add(recordBatch);
                await table.AddAsync(array);

                var target = new List<float>();
                for (var i=0; i<128; i++) target.Add(4.0f);
                var list = await table.Search().Vector(target).DistanceRange(0.0f, 1.0f).SelectColumns(["id", "vector"]).ToListAsync();
                Assert.That(list, Is.Not.Null);
                Assert.That(list, Is.Not.Empty);
                Assert.That(list.Count, Is.EqualTo(1));
                
                list = await table.Search().Vector(target).DistanceRange(Single.NaN, Single.NaN).SelectColumns(["id", "vector"]).ToListAsync();
                Assert.That(list, Is.Not.Null);
                Assert.That(list, Is.Not.Empty);
                Assert.That(list.Count, Is.EqualTo(8));
                
                list = await table.Search().Vector(target).DistanceRange(Single.NaN, 1520.0f).SelectColumns(["id", "vector"]).ToListAsync();
                Assert.That(list, Is.Not.Null);
                Assert.That(list, Is.Not.Empty);
                Assert.That(list.Count, Is.EqualTo(7));
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
                    Helpers.GetSchema(), 4096, 128
                );
                // Note that the interface defines a list, so we'll use that
                var array = new List<RecordBatch>();
                array.Add(recordBatch);
                table.Add(array);

                var target = new List<float>();
                for (var i=0; i<128; i++) target.Add(1.0f);
                int batchCount = 0;
                await foreach (var batch in table.Search().Vector(target).Limit(4096).SelectColumns(["id"]).ToBatchesAsync(1))
                {
                    batchCount++;
                }
                Assert.That(batchCount, Is.EqualTo(4096));
            }
        }
        finally
        {
            Cleanup(uri);
        }

        Assert.Pass();
    }
    
    [Test]
    public async Task BatchSizeVectorQueryCancel()
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
                    Helpers.GetSchema(), 4096, 128
                );
                // Note that the interface defines a list, so we'll use that
                var array = new List<RecordBatch>();
                array.Add(recordBatch);
                table.Add(array);

                var target = new List<float>();
                for (var i=0; i<128; i++) target.Add(1.0f);
                int batchCount = 0;
                var cts = new CancellationTokenSource();
                try
                {
                    await foreach (var batch in table.Search().Vector(target).Limit(4096).SelectColumns(["id"])
                                       .ToBatchesAsync(1, cts.Token))
                    {
                        if (batchCount == 10)
                        {
                            await cts.CancelAsync();
                        }

                        batchCount++;
                    }
                }
                catch (Exception e)
                {
                    Assert.That(e, Is.TypeOf<TaskCanceledException>());
                }

                // 11 because we increment even after the cancel
                Assert.That(batchCount, Is.EqualTo(11));
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