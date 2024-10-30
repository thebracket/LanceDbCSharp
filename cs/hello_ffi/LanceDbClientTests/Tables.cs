using LanceDbClient;

namespace LanceDbClientTests;

public partial class Tests
{
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