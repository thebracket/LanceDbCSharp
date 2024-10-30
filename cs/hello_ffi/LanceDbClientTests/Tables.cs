using LanceDbClient;

namespace LanceDbClientTests;

public partial class Tests
{
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