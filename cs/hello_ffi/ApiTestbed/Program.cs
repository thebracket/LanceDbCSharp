// Setup the LanceDB client (creates a command processor)

using System.Diagnostics;
using Apache.Arrow;
using Apache.Arrow.Types;
using ApiTestbed;
using LanceDbClient;
using LanceDbInterface;

using (var cnn = new Connection(new Uri("file:///tmp/test_lance")))
{
    System.Console.WriteLine("Connection Opened. There should be 0 tables.");
    // It's expected that the database is empty
    ListTables(cnn);

    // We create an empty table
    var table1 = cnn.CreateTable("table1", GetSchema());
    var table2 = cnn.CreateTable("table2", Helpers.GetSchema());
    System.Console.WriteLine("Tables Created: " + table1 + ", " + table2);
    // So its now expected to see 2 tables
    ListTables(cnn);
    
    // Now we'll drop table1
    cnn.DropTable("table1");
    System.Console.WriteLine("Table 1 Dropped");
    
    // Now we'll open table2
    var table2Opened = cnn.OpenTable("table2");
    System.Console.WriteLine("Table 2 Opened: " + table2Opened);
    System.Console.WriteLine("Table 2 row count (expect 0): " + table2Opened.CountRows());

    // Let's add some data
    var recordBatch = Helpers.CreateSampleRecordBatch(
        Helpers.GetSchema(), 4096, 8
    );
    table2.Add([recordBatch]);
    
    // Let's do a quick full-text search
    Console.WriteLine("Searching for '12'");
    table2.CreateFtsIndex(["id"], ["id"]);
    var search = table2.Search().Text("'12'").ToList();
    PrintDictList(search);
    
    // Let's do a quick vector search
    var vector = new List<float>
    {
        12, 12.125f, 12.25f, 12.375f, 12.5f, 12.625f, 12.75f, 12.875f,
    };
    Console.WriteLine("Searching for vector: " + vector);
    var vectorSearch = table2.Search().Vector(vector).Metric(Metric.Cosine).ToList();
    PrintDictList(vectorSearch);
    
    // Reranking simplest case
    Console.WriteLine("Reranking '12' and '7' with the simplest merge rerank");
    IReranker rrf = new RRFReranker();
    var arrow1 = table2.Search().Text("'12'").WithRowId(true).ToArrow();
    var arrow2 = table2.Search().Text("'7'").WithRowId(true).ToArrow();
    var merged = rrf.MergeResults(arrow1, arrow2);
    var mergedTable = ArrayHelpers.ArrowTableToListOfDicts(merged);
    PrintDictList(mergedTable);
    
    // Now we'll drop table2
    cnn.DropTable("table2");
    System.Console.WriteLine("Table 2 Dropped");
    ListTables(cnn);
    
    // Now we'll drop the database
    cnn.DropDatabase();
    System.Console.WriteLine("Database Dropped");
}
System.Console.WriteLine("Complete");

// Helper functions

void ListTables(Connection cnn)
{
    System.Console.Write("Tables: ");
    var tables = cnn.TableNames();
    var count = 0;
    foreach (var table in tables)
    {
        System.Console.Write(table + " ");
        count++;
    }
    if (count == 0)
    {
        System.Console.Write("None");
    }
    System.Console.WriteLine();
}

// Data generation functions

Schema GetSchema()
{
    // Define the "id" field (Int32, not nullable)
    var idField = new Field("id", Int32Type.Default, nullable: false);

    // Define the "item" field for the FixedSizeList (Float32, nullable)
    var itemField = new Field("item", FloatType.Default, nullable: true);

    // Define the FixedSizeListType with the "item" field and a fixed size of 128
    var vectorType = new FixedSizeListType(itemField, listSize: 128);

    // Define the "vector" field (FixedSizeList, nullable)
    var vectorField = new Field("vector", vectorType, nullable: true);

    // Create the schema with the "id" and "vector" fields
    var fields = new List<Field> { idField, vectorField };
    
    // Since metadata is required, but we don't have any, pass an empty dictionary
    var metadata = new Dictionary<string, string>();
    
    var schema = new Schema(fields, metadata);

    return schema;
}

void PrintDictList(IEnumerable<IDictionary<string, object>> enumerable)
{
    foreach (var row in enumerable)
    {
        // Print out each key and value
        foreach (var keyValuePair in row)
        {
            Console.Write(keyValuePair.Key + ": ");
            if (keyValuePair.Value is IList<float> inner)
            {
                Console.Write("[");
                foreach (var item in inner)
                {
                    Console.Write(item + ", ");
                }
                Console.WriteLine("]");
            }
            else if (keyValuePair.Value is IList<ulong> innerLong)
            {
                Console.Write("[");
                foreach (var item in innerLong)
                {
                    Console.Write(item + ", ");
                }
                Console.WriteLine("]");
            }
            else
            {
                Console.WriteLine(keyValuePair.Value);
            }
        }
    }
}