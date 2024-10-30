using LanceDbClient;

namespace LanceDbClientTests;

public partial class Tests
{
    [SetUp]
    public void Setup()
    {
    }

    private static void Cleanup(Uri uri)
    {
        using var cnn = new Connection(uri);
        cnn.DropDatabase();
    }
    
    [Test]
    public void TestNewDatabaseCreationAndDropping()
    {
        var uri = new Uri("file:///tmp/test_new_db");
        using (var cnn = new Connection(uri))
        {
            Assert.That(cnn.IsOpen, Is.True);
            Assert.That(cnn.Uri, Is.EqualTo(uri));
        }
        Assert.That(Directory.Exists(uri.LocalPath), Is.True);
        using (var cnn = new Connection(uri))
        {
            Assert.That(cnn.IsOpen, Is.True);
            cnn.DropDatabase();
            Assert.That(cnn.IsOpen, Is.False);
        }
        
        Assert.That(Directory.Exists(uri.LocalPath), Is.False);
        Assert.Pass();
    }
    
    [Test]
    public void TestCreateEmptyTable()
    {
        var uri = new Uri("file:///tmp/test_empty_table");
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
        }
        finally
        {
            Cleanup(uri);
        }

        Assert.Pass();
    }
    
    [Test]
    public void TestDropTable()
    {
        var uri = new Uri("file:///tmp/test_emptydrop_table");
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
                cnn.DropTable("table1");
                Assert.That(cnn.TableNames(), Does.Not.Contain("table1"));
            }
        }
        finally
        {
            Cleanup(uri);
        }

        Assert.Pass();
    }

    [Test]
    public void TestListTablesEmpty()
    {
        var uri = new Uri("file:///tmp/test_list_table_empty");
        try
        {
            using (var cnn = new Connection(uri))
            {
                Assert.That(cnn.IsOpen, Is.True);
                Assert.That(cnn.TableNames(), Is.Empty);
            }
        }
        finally
        {
            Cleanup(uri);
        }
    }

    [Test]
    public void TestListTables()
    {
        var uri = new Uri("file:///tmp/test_list_table_empty");
        try
        {
            using (var cnn = new Connection(uri))
            {
                Assert.That(cnn.IsOpen, Is.True);
                Assert.That(cnn.TableNames(), Is.Empty);
                cnn.CreateTable("test", Helpers.GetSchema());
                Assert.That(cnn.TableNames(), Contains.Item("test"));
            }
        }
        finally
        {
            Cleanup(uri);
        }
    }
    
    [Test]
    public void TestOpenTable()
    {
        var uri = new Uri("file:///tmp/test_open_table");
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
            }
        }
        finally
        {
            Cleanup(uri);
        }

        Assert.Pass();
    }

    [Test]
    public void TestOpenTableThatDoesntExist()
    {
        var uri = new Uri("file:///tmp/test_open_table_that_doesnt_exist");
        try
        {
            using (var cnn = new Connection(uri))
            {
                Assert.That(cnn.IsOpen, Is.True);
                Assert.Throws<Exception>(() => cnn.OpenTable("table1"));
            }
        }
        finally
        {
            Cleanup(uri);
        }
        Assert.Pass();
    }
}
