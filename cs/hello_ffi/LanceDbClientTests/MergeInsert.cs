using Apache.Arrow;
using LanceDbClient;

namespace LanceDbClientTests;

public partial class Tests
{
    [Test]
    public void SimpleMergeInsert()
    {
        var uri = new Uri("file:///tmp/test_table_mi");
        try
        {
            using (var cnn = new Connection(uri))
            {
                Assert.That(cnn.IsOpen, Is.True);
                var table = cnn.CreateTable("table1", Helpers.GetSchema());
                Assert.That(table.IsOpen, Is.True);
                var recordBatch = Helpers.CreateSampleRecordBatch(
                    Helpers.GetSchema(), 6, 128
                );
                // Note that the interface defines a list, so we'll use that
                var array = new List<RecordBatch>();
                array.Add(recordBatch);
                
                table.MergeInsert(["id"])
                    .WhenMatchedUpdateAll()
                    .WhenNotMatchedInsertAll()
                    .Execute(array);
                var rowCount = table.CountRows();
                Assert.That(rowCount, Is.GreaterThan(0));
                Assert.That(rowCount, Is.EqualTo(6));
                
                // Make sure that nothing happens when we insert the same again
                table.MergeInsert(["id"])
                    .WhenMatchedUpdateAll()
                    .WhenNotMatchedInsertAll()
                    .Execute(array);
                var rowCount2 = table.CountRows();
                Assert.That(rowCount2, Is.EqualTo(rowCount));
                
                // Now add a few and make sure it only added the new ones
                var recordBatch2 = Helpers.CreateSampleRecordBatch(
                    Helpers.GetSchema(), 10, 128
                );
                // Note that the interface defines a list, so we'll use that
                var array2 = new List<RecordBatch>();
                array2.Add(recordBatch2);
                table.MergeInsert(["id"])
                    .WhenMatchedUpdateAll()
                    .WhenNotMatchedInsertAll()
                    .Execute(array2);
                var rowCount3 = table.CountRows();
                Assert.That(rowCount3, Is.EqualTo(rowCount + 4));
            }
        }
        finally
        {
            Cleanup(uri);
        }
    }
}