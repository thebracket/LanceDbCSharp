using Apache.Arrow;
using LanceDbClient;

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
                Assert.That(table.CountRows("id = 0"), Is.EqualTo(1));
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
                Assert.That(table.CountRows("id = 0"), Is.EqualTo(1));
                table.Delete("id = 0");
                Assert.That(table.CountRows("id = 0"), Is.EqualTo(0));
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
                Assert.That(table.CountRows("id = 0"), Is.EqualTo(0));
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
}