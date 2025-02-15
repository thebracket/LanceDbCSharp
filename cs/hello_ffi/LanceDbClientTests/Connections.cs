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

    /*[Test]
    public void TestLocalMinio()
    {
        // Connect with options
        var uri = new Uri("s3://lance");
        var options = new Dictionary<String, String>()
        {
            { "AWS_ACCESS_KEY_ID", "MyKey" },
            { "AWS_SECRET_ACCESS_KEY", "MySecretKey" },
            { "AWS_REGION", "can-1" },
            { "AWS_ENDPOINT", "http://localhost:9000" },
            { "AWS_DEFAULT_REGION", "true" },
            { "allow_http", "true" },
        };
        // Connect
        using (var cnn = new Connection(uri, options))
        {
            Assert.That(cnn.IsOpen, Is.True);
            Assert.That(cnn.Uri, Is.EqualTo(uri));
            
            // There should be no tables
            Assert.That(cnn.TableNames(), Is.Empty);
            
            // Create a table and make sure it's present
            var table = cnn.CreateTable("table1", Helpers.GetSchema());
            Assert.Multiple(() =>
            {
                Assert.That(table, Is.Not.Null);
                Assert.That(cnn.TableNames(), Does.Contain("table1"));
            });
            
            // Remove the table and make sure it's gone
            cnn.DropTable("table1");
            Assert.That(cnn.TableNames(), Is.Empty);
            
            // Clean up
            cnn.DropDatabase();
            Assert.That(cnn.IsOpen, Is.False);
        }
    }*/
    
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
    public void TestNewDatabaseCreationAndDroppingWithStorageOptions()
    {
        var uri = new Uri("file:///tmp/test_new_db_opts");
        var options = new Dictionary<string, string>() { { "timeout", "30s" } };
        using (var cnn = new Connection(uri, options))
        {
            Assert.That(cnn.IsOpen, Is.True);
            Assert.That(cnn.Uri, Is.EqualTo(uri));
        }
        Assert.That(Directory.Exists(uri.LocalPath), Is.True);
        using (var cnn = new Connection(uri, options))
        {
            Assert.That(cnn.IsOpen, Is.True);
            cnn.DropDatabase();
            Assert.That(cnn.IsOpen, Is.False);
        }
        
        Assert.That(Directory.Exists(uri.LocalPath), Is.False);
        Assert.Pass();
    }
    
    [Test]
    public async Task TestNewDatabaseCreationAndDroppingAsync()
    {
        var uri = new Uri("file:///tmp/test_new_db_async");
        using (var cnn = new Connection(uri))
        {
            Assert.That(cnn.IsOpen, Is.True);
            Assert.That(cnn.Uri, Is.EqualTo(uri));
        }
        Assert.That(Directory.Exists(uri.LocalPath), Is.True);
        using (var cnn = new Connection(uri))
        {
            Assert.That(cnn.IsOpen, Is.True);
            await cnn.DropDatabaseAsync();
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
    public async Task TestCreateEmptyTableAsync()
    {
        var uri = new Uri("file:///tmp/test_empty_table_async");
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
    public async Task TestDropTableAsync()
    {
        var uri = new Uri("file:///tmp/test_emptydrop_table_async");
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
                await cnn.DropTableAsync("table1");
                Assert.That(await cnn.TableNamesAsync(), Does.Not.Contain("table1"));
            }
        }
        finally
        {
            Cleanup(uri);
        }

        Assert.Pass();
    }
    
    [Test]
    public void TestRenameTable()
    {
        // Implemented for completeness, I didn't realize that the OSS version of LanceDB doesn't
        // support renaming tables.
        var uri = new Uri("file:///tmp/test_rename_table");
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
                Assert.Throws<Exception>(() => cnn.RenameTable("table1", "table2"));
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
    public async Task TestListTablesEmptyAsync()
    {
        var uri = new Uri("file:///tmp/test_list_table_empty_async");
        try
        {
            using (var cnn = new Connection(uri))
            {
                Assert.That(cnn.IsOpen, Is.True);
                Assert.That(await cnn.TableNamesAsync(), Is.Empty);
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
    public async Task TestListTablesAsync()
    {
        var uri = new Uri("file:///tmp/test_list_table_empty_async");
        try
        {
            using (var cnn = new Connection(uri))
            {
                Assert.That(cnn.IsOpen, Is.True);
                Assert.That(await cnn.TableNamesAsync(), Is.Empty);
                await cnn.CreateTableAsync("test", Helpers.GetSchema());
                Assert.That(await cnn.TableNamesAsync(), Contains.Item("test"));
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
    public async Task TestOpenTableAsync()
    {
        var uri = new Uri("file:///tmp/test_open_table_async");
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
            }

            using (var cnn = new Connection(uri))
            {
                Assert.That(cnn.IsOpen, Is.True);
                var table = await cnn.OpenTableAsync("table1");
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
    
    [Test]
    public Task TestOpenTableThatDoesntExistAsync()
    {
        var uri = new Uri("file:///tmp/test_open_table_that_doesnt_exist_async");
        try
        {
            using (var cnn = new Connection(uri))
            {
                Assert.That(cnn.IsOpen, Is.True);
                Assert.ThrowsAsync<Exception>(async () => await cnn.OpenTableAsync("table1"));
            }
        }
        finally
        {
            Cleanup(uri);
        }
        Assert.Pass();
        return Task.CompletedTask;
    }
    
    [Test]
    public void TestCloseConnectionTwice()
    {
        var uri = new Uri("file:///tmp/test_close_twice");
        try
        {
            using (var cnn = new Connection(uri))
            {
                Assert.That(cnn.IsOpen, Is.True);
                cnn.Close();
                Assert.That(cnn.IsOpen, Is.False);
                cnn.Close();
                Assert.That(cnn.IsOpen, Is.False);
            }
        }
        finally
        {
            Cleanup(uri);
        }
        Assert.Pass();
    }
    
    [Test]
    public async Task TestCloseConnectionTwiceAsync()
    {
        var uri = new Uri("file:///tmp/test_close_twice_async");
        try
        {
            using (var cnn = new Connection(uri))
            {
                Assert.That(cnn.IsOpen, Is.True);
                await cnn.CloseAsync();
                Assert.That(cnn.IsOpen, Is.False);
                await cnn.CloseAsync();
                Assert.That(cnn.IsOpen, Is.False);
            }
        }
        finally
        {
            Cleanup(uri);
        }
        Assert.Pass();
    }
}
