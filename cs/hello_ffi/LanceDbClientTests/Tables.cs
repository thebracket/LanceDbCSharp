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
        // This will fail until we add data
        Assert.Fail();
    }
    
    [Test]
    public void CountRowsWithFilter()
    {
        // This will fail until we add data and write a filter
        Assert.Fail();
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
}