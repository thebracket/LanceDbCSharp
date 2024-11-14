// Setup the LanceDB client (creates a command processor)

using System.Collections;
using Apache.Arrow;
using Apache.Arrow.Types;
using System.Collections.Generic;
using LanceDbClient;
using LanceDbInterface;
using Array = Apache.Arrow.Array;

const int Dimension = 128;

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

    int numEntries = 3;
    System.Console.WriteLine($"{numEntries} records added.");
    table1.Add(GetBatches(numEntries));
    System.Console.WriteLine($"Table 1 row count (expect {numEntries}): {table1.CountRows()}" );
    
    string[] columns = new[] { "id" };    
    ILanceMergeInsertBuilder builder = table1.MergeInsert(columns);
    numEntries = 4;
    builder.WhenMatchedUpdateAll().WhenNotMatchedInsertAll().Execute(GetBatches(numEntries));
    System.Console.WriteLine($"Table 1 row count (expect {numEntries}): {table1.CountRows()}" );
    
    //table1.CreateIndex("vector", Metric.Cosine, 256, 16);
    //table1.CreateScalarIndex("id");

    var queryBuilder = table1.Search();
    Console.WriteLine(queryBuilder.Limit(3).ExplainPlan());
    //var result = queryBuilder.Limit(3).WhereClause("id > 0").WithRowId(true).ToList();

    List<float> vector = new List<float>();
    for (int i = 0; i < Dimension; i++)
    {
        vector.Add(0.3f);
    }   
    var result = queryBuilder.Vector(vector).Limit(3).WithRowId(true).ToList();
    
    foreach (var row in result)
    {
        foreach (string key in row.Keys)
        {
            Console.Write($"{key}:");
            foreach (var item in (IList)row[key])
            {
                Console.Write(item + " "); // Prints each integer in the list
            }
            Console.WriteLine();    
        }
        Console.WriteLine();
    }
    
    var resultTable = queryBuilder.Vector(vector).Limit(3).WithRowId(true).ToArrow();
    for (int i = 0; i < resultTable.ColumnCount; i++)
    {
        var column = resultTable.Column(i);
        Console.WriteLine($"Column {i}: {column.Name} {column.Type.Name}");
        for (int j = 0; j < column.Data.ArrayCount; j++)
        {
            Console.WriteLine($"size: {column.Data.ArrayCount}");
            // it seems it usually has only 1 element in this ChunkedArray
            var array = ArrayHelpers.ArrowArrayDataToConcrete(column.Data.ArrowArray(j));
            // Cast the object to List<int> or List<type>
            IList typedList = (IList)array;

            // You can now access the elements as a list of integers
            foreach (var item in typedList)
            {
                Console.Write(item + " "); // Prints each integer in the list
            }
            Console.WriteLine();
        }
    }

    // Now we'll drop table1
    cnn.DropTable("table1");
    System.Console.WriteLine("Table 1 Dropped");
    
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
    var vectorType = new FixedSizeListType(itemField, listSize: Dimension);

    // Define the "vector" field (FixedSizeList, nullable)
    var vectorField = new Field("vector", vectorType, nullable: true);

    // Create the schema with the "id" and "vector" fields
    var fields = new List<Field> { idField, vectorField };
    
    // Since metadata is required, but we don't have any, pass an empty dictionary
    var metadata = new Dictionary<string, string>();
    
    var schema = new Schema(fields, metadata);

    return schema;
}

IEnumerable<RecordBatch> GetBatches(int numEntries)
{
    
    var idArray = new Int32Array.Builder().AppendRange(Enumerable.Range(1, numEntries)).Build();

    Random random = new Random();
    float[] randomFloatArray = new float[Dimension * numEntries];
    float step = 0f;
    int dimensionIndex = 0;
    for (int i = 0; i < randomFloatArray.Length; i++)
    {
        if (dimensionIndex == Dimension)
        {
            step += 1/(float)numEntries;
            dimensionIndex = 0;  
        }
        else
            dimensionIndex++; 
        randomFloatArray[i] = step; 
    }
    
    var floatBuffer = new ArrowBuffer.Builder<float>()
        .AppendRange(randomFloatArray)
        .Build();
    ArrayData arrayData = new ArrayData(FloatType.Default, Dimension * numEntries, 0, 0, new [] {ArrowBuffer.Empty, floatBuffer});
    var floatArray = new FloatArray(arrayData);
    
    var listArrayData = new ArrayData(
        new FixedSizeListType(new Field("item", FloatType.Default, nullable: true), Dimension),
        length: numEntries,
        nullCount: 0,
        offset: 0,
        buffers: new[] { ArrowBuffer.Empty },
        children: new[] { floatArray.Data }
    );
    var fixedSizeListArray = new FixedSizeListArray(listArrayData);

    RecordBatch recordBatch = new RecordBatch(GetSchema(), new IArrowArray[] {idArray, fixedSizeListArray},  idArray.Length);
    RecordBatch[] batches = new[] { recordBatch };

    return batches;
}

FloatArray GetFloatArray(float value)
{
    var builder = new FloatArray.Builder();
    for (int i = 0; i < Dimension; i++)
    {
        builder.Append(value);
    }
    return builder.Build();
}