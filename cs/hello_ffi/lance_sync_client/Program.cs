// Note that this is a temporary console app - it'll become a proper
// library in the future.

using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;

// Path to the shared library
const string dllName = "../../../../../../rust/target/debug/liblance_sync_client.so";

[DllImport(dllName, CallingConvention = CallingConvention.Cdecl)]
static extern int setup();

[DllImport(dllName, CallingConvention = CallingConvention.Cdecl)]
static extern int shutdown();

[DllImport(dllName, CallingConvention = CallingConvention.Cdecl)]
static extern long connect(string uri);

[DllImport(dllName, CallingConvention = CallingConvention.Cdecl)]
static extern long disconnect(long handle);

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



var r = setup();
System.Console.WriteLine("Setup: " + r);
Int64 conn = connect("data/sample_db");
System.Console.WriteLine("Connection: " + conn);
var d = disconnect(conn);

var schema = GetSchema();

// Now save this to a temporary file in /tmp/schematest
using (var fileStream = new FileStream("/tmp/schematest", FileMode.Create))
{
    using (var writer = new ArrowFileWriter(fileStream, schema))
    {
        // Write the schema to the file
        writer.WriteEnd();
    }
    //fileStream.Write(schemaAsBytes, 0, schemaAsBytes.Length);
}

System.Console.WriteLine("Disconnect: " + d);
r = shutdown();
System.Console.WriteLine("Shutdown: " + r);

