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
    await ListTables(cnn);

    // We create an empty table
    var table1 = await cnn.CreateTableAsync("table1", GetSchema());
    var table2 = await cnn.CreateTableAsync("table2", Helpers.GetSchema());
    System.Console.WriteLine("Tables Created (Asynchronously): " + table1 + ", " + table2);
    // So it's now expected to see 2 tables
    await ListTables(cnn);
    
    // Now we'll drop table1
    await cnn.DropTableAsync("table1");
    System.Console.WriteLine("Table 1 Dropped (Asynchronously)");
    
    // Now we'll open table2
    var table2Opened = await cnn.OpenTableAsync("table2");
    System.Console.WriteLine("Table 2 Opened (Async): " + table2Opened);
    System.Console.WriteLine("Table 2 row count (expect 0): " + await table2Opened.CountRowsAsync());

    // Let's add some data
    var recordBatch = Helpers.CreateSampleRecordBatch(
        Helpers.GetSchema(), 4096, 8
    );
    await table2.AddAsync([recordBatch]);
    System.Console.WriteLine("Table 2 row count (expect 4096): " + await table2.CountRowsAsync());
    
    // Let's do a quick full-text search
    Console.WriteLine("Searching for '12'");
    await table2.CreateFtsIndexAsync(["id"], ["id"]);
    await table2.OptimizeAsync();
    var search = await table2.Search().Text("'12'").ToListAsync();
    PrintDictList(search);
    
    // Let's do a quick vector search
    var vector = new List<float>
    {
        12, 12.125f, 12.25f, 12.375f, 12.5f, 12.625f, 12.75f, 12.875f,
    };
    Console.WriteLine("Searching for vector: " + vector);
    var vectorSearch = await table2.Search().Vector(vector).Metric(Metric.Cosine).Limit(4).ToListAsync();
    PrintDictList(vectorSearch);

    // Sync vs Async Comparison
    var v1 = table2.Search().Vector(vector).ToBatches(0);
    var v2 = new List<RecordBatch>();
    await foreach (var batch in table2.Search().Vector(vector).ToBatchesAsync(0))
    {
        v2.Add(batch);
    }
    Debug.Assert(v1.Count() == v2.Count);
    
    // Reranking simplest case
    Console.WriteLine("Reranking '12' and '7' with the simplest merge rerank");
    IReranker rrf = new RrfReranker();
    var arrow1 = table2.Search().Text("'12'").WithRowId(true).ToArrow();
    var arrow2 = table2.Search().Text("'7'").WithRowId(true).ToArrow();
    var merged = rrf.MergeResults(arrow1, arrow2);
    var mergedTable = ArrayHelpers.ArrowTableToListOfDicts(merged);
    PrintDictList(mergedTable);
    
    // Reranking with RRF (This is broken out into steps for debugging)
    Console.WriteLine("Reranking '12' with RRF");
    var testRrf = table2
        .Search(vector, "vector", queryType: QueryType.Hybrid)
        .SelectColumns(["id", "vector"]);
    var testRrf2 = testRrf
        .Text("'12'")
        .Rerank(new RrfReranker());
    var testRrf3 = testRrf2.ToList();
    PrintDictList(testRrf3);
    
    // Now we'll drop table2
    await cnn.DropTableAsync("table2");
    System.Console.WriteLine("Table 2 Dropped (Asynchronously)");
    await ListTables(cnn);
    
    // Now we'll drop the database
    //await cnn.DropDatabaseAsync();
    cnn.DropDatabase();
    System.Console.WriteLine("Database Dropped (Asynchronously)");
}
System.Console.WriteLine("Complete");

// Helper functions

async Task ListTables(Connection cnn)
{
    Console.WriteLine("--------------------------------------------------------");
    Console.Write("Tables: ");
    var tables = await cnn.TableNamesAsync();
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
    Console.WriteLine("--------------------------------------------------------");
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
    Console.WriteLine("--------------------------------------------------------");
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
            else if (keyValuePair.Value is IList<string> innerString)
            {
                Console.Write("[");
                foreach (var item in innerString)
                {
                    Console.Write("\"" + item + "\", ");
                }
                Console.WriteLine("]");
            }
            else
            {
                Console.WriteLine(keyValuePair.Value);
            }
        }
        Console.WriteLine("________________");
    }
    Console.WriteLine("--------------------------------------------------------");
}