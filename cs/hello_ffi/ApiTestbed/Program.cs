// Setup the LanceDB client (creates a command processor)

using Apache.Arrow;
using Apache.Arrow.Types;
using LanceDbClient;

using (var cnn = new Connection(new Uri("file:///tmp/test_lance")))
{
    System.Console.WriteLine("Connection Opened. There should be 0 tables.");
    // It's expected that the database is empty
    ListTables(cnn);

    // We create an empty table
    var table1 = cnn.CreateTable("table1", GetSchema());
    var table2 = cnn.CreateTable("table2", GetSchema());
    System.Console.WriteLine("Tables Created: " + table1 + ", " + table2);
    // So its now expected to see 2 tables
    ListTables(cnn);
    
    // Now we'll drop table1
    cnn.DropTable("table1");
    System.Console.WriteLine("Table 1 Dropped");
    ListTables(cnn);
    
    // Now we'll open table2
    var table2Opened = cnn.OpenTable("table2");
    System.Console.WriteLine("Table 2 Opened: " + table2Opened);
    System.Console.WriteLine("Table 2 row count (expect 0): " + table2Opened.CountRows());

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