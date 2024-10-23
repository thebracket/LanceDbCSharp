// Note that this is a temporary console app - it'll become a proper
// library in the future.

// BEFORE RUNNING, PLEASE ERASE THE "DATA" DIRECTORY!

using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;

// Path to the shared library
//const string DllName = "../../../../../../rust/target/debug/liblance_sync_client.so";
const string DllName = "liblance_sync_client.so";

[DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
static extern int setup();

[DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
static extern int shutdown();

[DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
static extern long connect(string uri);

[DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
static extern long disconnect(long handle);

[DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
unsafe static extern long submit_record_batch(byte* data, ulong length);

[DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
static extern long free_record_batch(long handle);

[DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
static extern long create_table(string name, long connectionHandle, long recordHandle);

[DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
unsafe static extern long query_nearest_to(long connectionHandle, long limit, float* vector, ulong vectorLength, string tableName);

[DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
static extern long free_blob(long blobHandle);

[DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
static extern long blob_len(long blobHandle);

// Is there a tag for "really unsafe"? Because this is it.
[DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
unsafe static extern byte* get_blob_data(long blobHandle);

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

byte[] serialize()
{
    var schema = GetSchema();
    using (MemoryStream ms = new MemoryStream())
    {
        using (var writer = new ArrowFileWriter(ms, schema))
        {
            // Write the schema to the file
            writer.WriteRecordBatch(CreateRecordBatch(schema, 10, 128));
            writer.WriteEnd();
        }

        return ms.ToArray();
    }
}

// Setup the LanceDB client (creates a command processor)
var r = setup();
System.Console.WriteLine("Setup: " + r);

// Connect to the database
Int64 conn = connect("data/sample_db");
System.Console.WriteLine("Connection: " + conn);

// Serialize a recordbatch and submit it.
var bytes = serialize(); // Sample
var recordHandle = -1L;
unsafe {
    // Get a pointer to the start of the bytes array
    fixed (byte* p = bytes)
    {
        // Call the FFI function with the pointer and the length of the array
        recordHandle = submit_record_batch(p, (ulong)bytes.Length);
    }
}
System.Console.WriteLine("Record Handle: " + recordHandle);

// Add a "sample_table" to the database, including the test data
var t = create_table("sample_table", conn, recordHandle);
System.Console.WriteLine("Add Table: " + t);

// Inform the Rust side that it can free the record batch (if it needs to)
var freeResult = free_record_batch(recordHandle);
System.Console.WriteLine("Free Result: " + freeResult);

// Perform a nearest neighbour query
var queryResult = -1L;
float[] vector = new float[128];
for (int i = 0; i < 128; i++)
{
    vector[i] = 1.0f; // Sample value as 1.0
}

unsafe
{
    fixed (float* p = vector)
    {
        queryResult = query_nearest_to(conn, 5, p, 128, "sample_table");
    }
}
System.Console.WriteLine("Query Result: " + queryResult);

// The query result is now in the data-blob with the handle from "queryResult"
// At this point, we're not doing streaming - so it's all in memory.
// So we retrieve it with another API call.

// Obtain the blob length
var blobLength = blob_len(queryResult);
System.Console.WriteLine("Blob Length: " + blobLength);

// Now we retrieve the blob data as a pointer to a byte array in RAM
// and deserialize it. Here be dragons.
unsafe
{
    var blobPointer = get_blob_data(queryResult);
    byte[] blobData = new byte[blobLength];
    // TODO: Can we do this zero-copy?
    Marshal.Copy((IntPtr)blobPointer, blobData, 0, (int)blobLength);
    // Deserialize the blobData
    using (MemoryStream ms = new MemoryStream(blobData))
    {
        using (var reader = new ArrowFileReader(ms))
        {
            // Read the schema
            var schema = reader.GetSchema();
            // Read the record batch
            var recordBatch = reader.ReadNextRecordBatch();
            // Do something with the record batch
            System.Console.WriteLine(recordBatch);
        }
    }
}

// Free the blob!
freeResult = free_blob(queryResult);
System.Console.WriteLine("Free Blob Result: " + freeResult);

// Disconnect the connection handle, freeing up resources
var d = disconnect(conn);
System.Console.WriteLine("Disconnect: " + d);

// Shutdown the LanceDB client (ending the command loop/environment)
r = shutdown();
System.Console.WriteLine("Shutdown: " + r);

RecordBatch CreateRecordBatch(Schema schema, int total, int dim)
{
    // Step 1: Create Int32Array for the "id" field
    var idBuilder = new Int32Array.Builder();
    for (int i = 0; i < total; i++)
    {
        idBuilder.Append(i);
    }
    var idArray = idBuilder.Build();

    // Step 2: Create FixedSizeListArray for the "vector" field

    // a. Create the child float array for the FixedSizeListArray
    var floatBuilder = new FloatArray.Builder();

    for (int i = 0; i < total * dim; i++)
    {
        floatBuilder.Append(1.0f); // Sample value as 1.0
    }

    var floatArray = floatBuilder.Build();

    // b. Create the FixedSizeListArray
    var vectorType = new FixedSizeListType(new Field("item", FloatType.Default, nullable: true), listSize: dim);
    var vectorArrayData = new ArrayData(
        vectorType,
        length: total,
        nullCount: 0,
        buffers: new[] { ArrowBuffer.Empty }, // No null bitmap buffer, assuming all are valid
        children: new[] { floatArray.Data });

    var vectorArray = new FixedSizeListArray(vectorArrayData);

    // Step 3: Create RecordBatch
    var arrays = new IArrowArray[] { idArray, vectorArray };
    var recordBatch = new RecordBatch(schema, arrays, length: total);

    return recordBatch;
}
