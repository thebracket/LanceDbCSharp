// Setup the LanceDB client (creates a command processor)

using System.Collections;
using Apache.Arrow;
using Apache.Arrow.Types;
using System.Collections.Generic;
using ApiTestbed;
using LanceDbClient;
using LanceDbInterface;
using Array = Apache.Arrow.Array;
using Table = Apache.Arrow.Table;

const int Dimension = 128;

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

    int numEntries = 5;
    System.Console.WriteLine($"{numEntries} records added.");
    table1.Add(GetBatches(numEntries));
    System.Console.WriteLine($"Table 1 row count (expect {numEntries}): {table1.CountRows()}" );
    
    string[] columns = new[] { "id" };    
    ILanceMergeInsertBuilder builder = table1.MergeInsert(columns);
    numEntries = 1000;
    builder.WhenMatchedUpdateAll().WhenNotMatchedInsertAll().Execute(GetBatches(numEntries));
    System.Console.WriteLine($"Table 1 row count (expect {numEntries}): {table1.CountRows()}" );
    
    table1.CreateIndex("vector", Metric.Cosine, 256, 16);
    table1.CreateScalarIndex("id");

    var indexes = table1.ListIndices();
    foreach (var index in indexes)
    {
        var statistics = table1.GetIndexStatistics(index.Name);
        Console.WriteLine($"Index: {index.Name}, Type: {index.IndexType}, Statistics: {statistics}");
    }
    
    var queryBuilder = table1.Search();
    Console.WriteLine(queryBuilder.Limit(3).ExplainPlan());

    System.Console.WriteLine("======  Search id < 4================================");
    var resultWithWhere = queryBuilder.Limit(2).WhereClause("id < 4").WithRowId(true).ToList();
    PrintResults(resultWithWhere);
    
    table1.CreateFtsIndex(["text"], ["text"]);
    System.Console.WriteLine("======  Search Text(apple)================================");
    PrintResults(table1.Search().Text("apple").Limit(5).ToList());
    
    List<float> vector1 = new List<float>();
    for (int i = 0; i < Dimension; i++)
    {
        vector1.Add(0.3f);
    }

    System.Console.WriteLine("======  Search vector 0.3....Return List===============================");
    var resultList = ((VectorQueryBuilder)table1.Search().Vector(vector1)).Metric(Metric.Cosine).NProbes(10).RefineFactor(10).Limit(3).WithRowId(true).ToList();
    PrintResults(resultList);
    
    System.Console.WriteLine("======  Search vector 0.3....Return Batches===============================");
    var resultBatches = ((VectorQueryBuilder)table1.Search().Vector(vector1)).Metric(Metric.Cosine).NProbes(10).RefineFactor(10).Limit(3).WithRowId(true).ToBatches(2);
    PrintBatches(resultBatches);

    System.Console.WriteLine("======  Search vector 0.3...., return Table===============================");
    var resultTable = table1.Search().Vector(vector1).Limit(3).WithRowId(true).ToArrow();
    PrintTable(resultTable);

    System.Console.WriteLine($"Rows before delete: {table1.CountRows()}");
    table1.Delete("id < 100");
    System.Console.WriteLine($"Rows after delete: {table1.CountRows()}");
    
    var optimizeResult = table1.Optimize();
    System.Console.WriteLine("Optimize Result: " + optimizeResult.Compaction + optimizeResult.Prune);
    System.Console.WriteLine($"Rows after optimize(): {table1.CountRows()}");

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
    IReranker rrf = new RrfReranker();
    var arrow1 = table2.Search().Text("'12'").WithRowId(true).ToArrow();
    var arrow2 = table2.Search().Text("'7'").WithRowId(true).ToArrow();
    var merged = rrf.MergeResults(arrow1, arrow2);
    var mergedTable = ArrayHelpers.ArrowTableToListOfDicts(merged);
    PrintDictList(mergedTable);
    
    // Reranking with RRF
    Console.WriteLine("Reranking '12' with RRF");
    var testRrf = table2.Search(vector, "vector", queryType: QueryType.Hybrid)
        .Text("'12'")
        .Rerank(new RrfReranker())
        .ToList();
    PrintDictList(testRrf);
    
    // Now we'll drop table2
    cnn.DropTable("table2");
    System.Console.WriteLine("Table 2 Dropped");
    ListTables(cnn);
    
    // Now we'll drop the database
    //System.Console.WriteLine("Database Dropped");
    cnn.Close();
    var open = cnn.IsOpen ? "open" : "closed";
    System.Console.WriteLine($"connection is: {open}");
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
    
    var textField = new Field("text", StringType.Default, nullable: false);

    // Create the schema with the "id" and "vector" fields
    var fields = new List<Field> { idField, textField, vectorField };
    
    // Since metadata is required, but we don't have any, pass an empty dictionary
    var metadata = new Dictionary<string, string>();
    
    var schema = new Schema(fields, metadata);

    return schema;
}

IEnumerable<RecordBatch> GetBatches(int numEntries)
{
    
    var idArray = new Int32Array.Builder().AppendRange(Enumerable.Range(1, numEntries)).Build();

    var textBuilder = new StringArray.Builder();
    string[] stringValues = new string[5] { "orange", "apple", "pear", "peach", "avocado" };
    for (int i = 0; i < numEntries; i++)
    {
        textBuilder.Append(stringValues[i%5]);
    }
    var textArray = textBuilder.Build();
    
    Random random = new Random();
    float[] randomFloatArray = new float[Dimension * numEntries];
    float step = 0f;
    int dimensionIndex = 0;
    // generate array like this:
    // 0.01, 0.01, ..... 0.01,
    // 0.02, 0.02, ..... 0.02,
    // and so on...
    // until we have filled up the array with numEntries * Dimension floats.
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
    
    var listArrayData = new ArrayData(
        new FixedSizeListType(new Field("item", FloatType.Default, nullable: true), Dimension),
        length: numEntries,
        nullCount: 0,
        offset: 0,
        buffers: new[] { ArrowBuffer.Empty },
        children: new[] { arrayData }
    );
    var fixedSizeListArray = new FixedSizeListArray(listArrayData);

    RecordBatch recordBatch = new RecordBatch(GetSchema(), new IArrowArray[] {idArray, textArray, fixedSizeListArray},  idArray.Length);
    RecordBatch[] batches = new[] { recordBatch };

    return batches;
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
    }
}

void PrintResults(IEnumerable<IDictionary<string, object>> result)
{
    foreach (var row in result)
    {   
        foreach (string key in row.Keys)
        {
            Console.Write($"{key}:");
            if (row[key] is IList)
            {
                foreach (var item in (IList)row[key])
                {
                    Console.Write(item + " "); // Prints each integer in the list
                }
            }
            else
            {
                Console.Write(row[key] + " ");
            }
            Console.WriteLine();    
        }
        Console.WriteLine();
    }
}

void PrintTable(Table table)
{
    for (int i = 0; i < table.ColumnCount; i++)
    {
        var column = table.Column(i);
        Console.WriteLine($"Column {i}: {column.Name} {column.Type.Name}");
        for (int j = 0; j < column.Data.ArrayCount; j++)
        {
            Console.WriteLine($"size: {column.Data.ArrayCount}");
            // it seems it usually has only 1 element in this ChunkedArray
            // the intention of ChunkedArray seems for scalability,
            // however, if we try to merge all chunks into one, will it break the scalability? unless now it is a single field so it doesn't matter?
            var array = ArrayHelpers.ArrowArrayDataToConcrete(column.Data.ArrowArray(j));
            // Cast the object to List<int> or List<type>

            var list = array as IList;
            if (list != null)
            {
                foreach (var item in list)
                {
                    Console.Write(item + " "); // Prints each integer in the list
                }
            }
            else
            {
                Console.Write(array + " ");
            }
            Console.WriteLine();
        }
    }
}

void PrintBatches(IEnumerable<RecordBatch> recordBatches)
{
    foreach (var batch in recordBatches)
    {
        for (int i = 0; i < batch.Length; i++)
        {
            for (int j = 0; j < batch.ColumnCount; j++)
            {
                var column = batch.Column(j);
                var field = batch.Schema.FieldsList[j];
                Console.Write(field.Name + ": ");
                Console.WriteLine(field.DataType.GetType());

                if (column.Data.DataType.TypeId == ArrowTypeId.FixedSizeList)
                {
                    var fixedSizeListArray = (FixedSizeListArray)column;
                    int k = 0;
                    foreach (var value in (IEnumerable)(fixedSizeListArray.Values))
                    {
                        Console.Write(value + " ");
                        k++;
                        if (k == fixedSizeListArray.Values.Length / fixedSizeListArray.Length)
                        {
                            Console.WriteLine("total elements:" + k);
                            k = 0;
                            Console.WriteLine();
                        }
                    }

                }
                else
                {
                    foreach (var x in (IEnumerable)column)
                    {
                        Console.WriteLine(x);
                    }
                }

                // var array = ArrayHelpers.ArrowArrayDataToConcrete(column);
                // foreach (var item in (IList)array)
                // {
                //     foreach (var y in (IEnumerable)item)
                //         Console.Write(y + " ");
                // }
                Console.WriteLine();
            }
            Console.WriteLine();
        }
    }
}

