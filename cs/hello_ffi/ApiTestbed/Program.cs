// Setup the LanceDB client (creates a command processor)

using System.Collections;
using Apache.Arrow;
using Apache.Arrow.Types;
using System.Collections.Generic;
using System.Diagnostics;
using ApiTestbed;
using LanceDbClient;
using LanceDbInterface;
using MathNet.Numerics.LinearAlgebra;
using Array = Apache.Arrow.Array;
using Table = Apache.Arrow.Table;

const int Dimension = 4;

using (var cnn = new Connection(new Uri("file:///tmp/test_lance")))
{
    System.Console.WriteLine("Connection Opened. There should be 0 tables.");
    Console.WriteLine($"The URI is: {cnn.Uri.AbsoluteUri}");
    // It's expected that the database is empty
    await ListTables(cnn);

    // We create an empty table
    var table1 = await cnn.CreateTableAsync("table1", GetSchema());
    var table2 = await cnn.CreateTableAsync("table2", Helpers.GetSchema());
    System.Console.WriteLine("Tables Created: " + table1 + ", " + table2);
    // So its now expected to see 2 tables
    await ListTables(cnn);

    int numEntries = 5;
    System.Console.WriteLine($"{numEntries} records added using Batches.");
    await table1.AddAsync(GetBatches(numEntries));
    System.Console.WriteLine($"Table 1 row count (expected {numEntries}), actual {table1.CountRows()}" );

    int dictionaryRows = 2;
    await table1.AddAsync(GetDictionary(dictionaryRows, numEntries));
    System.Console.WriteLine($"Table 1 row count (expected {numEntries + dictionaryRows}) actual {table1.CountRows()}" );
    
    string[] columns = new[] { "id" };    
    ILanceMergeInsertBuilder builder = await table1.MergeInsertAsync(columns);
    numEntries = 100000;
    for (int i = 0; i < 10; i++)
    {
        IEnumerable<RecordBatch> records = GetBatches(numEntries, i * numEntries);
        await builder.WhenMatchedUpdateAll().WhenNotMatchedInsertAll().ExecuteAsync(records);
        System.Console.WriteLine($"Table 1 row count (expected {numEntries * (i+1)}) actual {table1.CountRows()}" );
    }

    Metric metric = Metric.Dot;
    //table1.CreateIndex("vector", metric, 33, 2);
    //table1.CreateScalarIndex("id");
    await table1.CreateIndexAsync("vector", metric, 33, 2);
    await table1.CreateScalarIndexAsync("id");

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
    
    //table1.CreateFtsIndex(["text"], ["text"]);
    await table1.CreateFtsIndexAsync(["text"], ["text"]);
    /*
    // GetIndexStatistics doesn't work with fts indexes
    indexes = await table1.ListIndicesAsync();
    foreach (var index in indexes)
    {
        var statistics = table1.GetIndexStatistics(index.Name);
        Console.WriteLine($"Index: {index.Name}, Type: {index.IndexType}, Statistics: {statistics}");
    }
    System.Console.WriteLine("======  Search Text(apple)================================");
    PrintResults(table1.Search().Text("apple").Limit(2).ToList());
    */
    
    List<float> vector1 = new List<float>();
    for (int i = 0; i < Dimension; i++)
    {
        vector1.Add(0.3f);
    }

    System.Console.WriteLine("======  Search vector 0.3....Return List===============================");
    var resultList = ((VectorQueryBuilder)table1.Search().Vector(vector1)).Metric(metric).NProbes(10).RefineFactor(10).Limit(2).WithRowId(true).ToList();
    PrintResults(resultList);
    
    System.Console.WriteLine("======  Search vector 0.3....Return Batches Sync ===============================");
    var resultBatches = ((VectorQueryBuilder)table1.Search().Vector(vector1)).Metric(metric).NProbes(10).RefineFactor(10).Limit(4).WithRowId(true).ToBatches(2);
    PrintBatches(resultBatches);

    System.Console.WriteLine("======  Search vector 0.3...., return Table sync===============================");
    var resultTable = table1.Search().Vector(vector1).Limit(3).WithRowId(true).ToArrow();
    PrintTable(resultTable);

    System.Console.WriteLine("======  Search Vector<float> 0.3...., return List sync===============================");
    Vector<float> vectorValues = Vector<float>.Build.Dense(vector1.ToArray());
    var vectorValuesResult = await table1.Search(vectorValues, "vector").Limit(2).ToListAsync();
    PrintResults(vectorValuesResult);

    /*
    System.Console.WriteLine("======  Search Matrix<float> 0.3...., return List sync===============================");
    int rows = 2;
    float[,] array2d = new float[rows, Dimension];
    for (int i = 0; i < rows; i++)
    {
        for (int j = 0; j < Dimension; j++)
        {
            array2d[i, j] = 0.3f;
        }
    }
    // we use matrix to query multiple vectors, the internal code seems to convert matrix to 1 dimensional array
    Matrix<float> matrixValues = Matrix<float>.Build.DenseOfArray(array2d);
    var matrixValuesResult = table1.Search(matrixValues, "vector").Limit(2).ToList();
    PrintResults(matrixValuesResult);
    */
    
    System.Console.WriteLine("======  Search vector 0.3....Return Batches Async ===============================");
    var resultBatchesAsync = ((VectorQueryBuilder)table1.Search().Vector(vector1)).Metric(metric).NProbes(10).RefineFactor(10).Limit(2).WithRowId(true).ToBatchesAsync(1);
    //the result will be displayed together with the next query due to the nature of asynchronous
    PrintBatchesAsync(resultBatchesAsync);

    
    Console.WriteLine("======hybrid, reranker, return batches, sync =================");
    // The limit is not respected.================
    var testHybrid = ((HybridQueryBuilder)(table1.Search(vector1, "vector", queryType: QueryType.Hybrid)))
        .Metric(metric)
        .NProbes(10)
        .RefineFactor(10)
        .SelectColumns(["id", "text", "vector" ])
        .Text("apple")
        .Rerank(new RrfReranker())
        .Limit(7)
        .ToBatches(3);
    PrintBatches(testHybrid);

    System.Console.WriteLine($"Rows before delete: {table1.CountRows()}");
    await table1.DeleteAsync("id < 100");
    System.Console.WriteLine($"Rows after delete: {table1.CountRows()}");
    
    //cleanup_older_than=timedelta(days=0
    var optimizeResult = await table1.OptimizeAsync(TimeSpan.FromDays(0));
    System.Console.WriteLine("Optimize Result: " + optimizeResult.Compaction + optimizeResult.Prune);
    System.Console.WriteLine($"Rows after optimize(): {table1.CountRows()}");

    // Now we'll drop table1
    await table1.CloseAsync();
    //table1.Close();
    var isTableOpen = table1.IsOpen;
    
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
    var vectorSearch = await table2.Search().Vector(vector).Metric(metric).Limit(4).ToListAsync();
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
    var mergedTable = ArrayHelpers.ArrowTableToListOfDictionaries(merged);
    PrintDictList(mergedTable);
    
    // Reranking with RRF (This is broken out into steps for debugging)
    Console.WriteLine("Reranking '12' with RRF");
    var testRrf = table2
        .Search(vector, "vector", queryType: QueryType.Hybrid)
        .SelectColumns(["id", "vector"]);
    var testRrf2 = testRrf
        .Text("'12'")
        .Limit((15))
        .Rerank(new RrfReranker())
        .ToList();
    PrintDictList(testRrf2);

    
    Console.WriteLine("Reranking '12' with RRF, sync version and return batches");
    var testRrfSync = table2.Search(vector, "vector", queryType: QueryType.Hybrid)
        .Text("'12'")
        .SelectColumns(["id", "vector"])
        .Rerank(new RrfReranker())
        .ToBatches(2);
    PrintBatches(testRrfSync);

    // Now we'll drop table2
    await cnn.DropTableAsync("table2");
    System.Console.WriteLine("Table 2 Dropped (Asynchronously)");
    await ListTables(cnn);
    
    // Now we'll drop the database
    //await cnn.DropDatabaseAsync();
    //cnn.DropDatabase();
    System.Console.WriteLine("Database Dropped (Asynchronously)");
    //System.Console.WriteLine("Database Dropped");
    await cnn.CloseAsync();
    var open = cnn.IsOpen ? "open" : "closed";
    System.Console.WriteLine($"connection is: {open}");
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

    var textField = new Field("text", StringType.Default, nullable: false);

    // Define the "item" field for the FixedSizeList (Float32, nullable)
    var itemField = new Field("item", FloatType.Default, nullable: true);

    // Define the FixedSizeListType with the "item" field and a fixed size of 128
    var vectorType = new FixedSizeListType(itemField, listSize: Dimension);

    // Define the "vector" field (FixedSizeList, nullable)
    var vectorField = new Field("vector", vectorType, nullable: true);
    
    // Create the schema with the "id" and "vector" fields
    var fields = new List<Field> { idField, textField, vectorField };
    
    // Since metadata is required, but we don't have any, pass an empty dictionary
    var metadata = new Dictionary<string, string>();
    
    var schema = new Schema(fields, metadata);

    return schema;
}

IEnumerable<RecordBatch> GetBatches(int numEntries, int startIndex = 0)
{
    
    var idArray = new Int32Array.Builder().AppendRange(Enumerable.Range(startIndex + 1, numEntries)).Build();

    var textBuilder = new StringArray.Builder();
    string[] stringValues = new string[5] { "orange", "apple", "pear", "peach", "avocado" };
    for (int i = 0; i < numEntries; i++)
    {
        textBuilder.Append(stringValues[i%5]);
    }
    var textArray = textBuilder.Build();
    
    float[] randomFloatArray = new float[Dimension * numEntries];
    Random random = new Random();
    for (int i = 0; i < randomFloatArray.Length; i++)
    {
        randomFloatArray[i] = random.NextSingle(); 
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

IEnumerable<Dictionary<string, object>> GetDictionary(int numEntries, int indexStart)
{
    var data = new List<Dictionary<string, object>>();
    for (int i = indexStart; i < indexStart+numEntries; i++)
    {
        List<int> idList = new List<int>() { i + 1};
        List<string> textList = new List<string> { "item " + (i + 1) };
        
        var row = new Dictionary<string, object>
        {
            {"id",  idList},
            {"text", textList},
            {"vector", Enumerable.Repeat(0.7f, Dimension).ToList()}
        };
        data.Add(row);
    }
    /*
     This will not work
        int i = 0;
        List<int> idList = new List<int>() { i + 1, i+2};
        List<string> textList = new List<string> { "item " + (i + 1), "item " + (i + 2) };
        List<List<float>> vectorList = new List<List<float>>
            { Enumerable.Repeat(0.7f, Dimension).ToList(), Enumerable.Repeat(0.7f, Dimension).ToList() };
        var row = new Dictionary<string, object>
        {
            {"id",  idList},
            {"text", textList},
            {"vector", vectorList}
        };
     */
    
    return data;

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
                    Console.Write(item + ", "); // Prints each integer in the list
                }
            }
            else
            {
                Console.Write(row[key] + ", ");
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
        Console.WriteLine($"\nColumn {i}: {column.Name} {column.Type.Name}");
        for (int j = 0; j < column.Data.ArrayCount; j++)
        {
            Console.WriteLine($"table truncated array size: {column.Data.ArrayCount}");
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
                    Console.Write(item + "-"); // Prints each integer in the list
                }
            }
            else
            {
                Console.Write(array + "-");
            }
            Console.WriteLine();
        }
    }
}


async void PrintBatchesAsync(IAsyncEnumerable<RecordBatch> recordBatches)
{
    Console.WriteLine("=====print async batch start ======");
    await foreach (var batch in recordBatches)
    {
        PrintBatch(batch);
    }
    Console.WriteLine("=====print async batch end ======");
}

void PrintBatches(IEnumerable<RecordBatch> recordBatches)
{
    foreach (var batch in recordBatches)
    {
        PrintBatch(batch);
    }
}

void PrintBatch(RecordBatch batch)
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
                Console.Write(value + ", ");
                k++;
                if (k == fixedSizeListArray.Values.Length / fixedSizeListArray.Length)
                {
                    Console.WriteLine();
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

