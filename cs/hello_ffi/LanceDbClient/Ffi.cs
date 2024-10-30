using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Ipc;

namespace LanceDbClient;

static partial class Ffi
{
    // Use this one for local builds
    //private const string DllName = "../../../../../../rust/target/debug/liblance_sync_client.so";
    // Use this one for Docker 
    private const string DllName = "liblance_sync_client.so";
    
    /* Delegate types */
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal unsafe delegate void SetSchemaCallback(byte* schema, ulong schemaLength);

    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void ResultCallback(long code, string? message);

    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void StringCallback(string s);

    /* FFI functions */
    [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern void connect(string uri, ResultCallback onResult);
    
    [LibraryImport(DllName)]
    [UnmanagedCallConv(CallConvs = new Type[] { typeof(System.Runtime.CompilerServices.CallConvCdecl) })]
    internal static partial void disconnect(long handle, ResultCallback onResult);
    
    [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern unsafe void create_empty_table(string name, long connectionHandle, byte* schema, ulong schemaLength, ResultCallback onResult);
    
    [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern void open_table(string name, long connectionHandle, SetSchemaCallback callback, ResultCallback onResult);
    
    [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern void drop_table(string name, long connectionHandle, ResultCallback onResult);
    
    [LibraryImport(DllName)]
    [UnmanagedCallConv(CallConvs = new Type[] { typeof(System.Runtime.CompilerServices.CallConvCdecl) })]
    internal static partial void list_table_names(long connectionHandle, StringCallback stringCallback, ResultCallback onResult);

    [LibraryImport(DllName)]
    [UnmanagedCallConv(CallConvs = new Type[] { typeof(System.Runtime.CompilerServices.CallConvCdecl) })]
    internal static partial void close_table(long connectionId, long tableId, ResultCallback onResult);

    [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern void count_rows(long connectionHandle, long tableHandle, ResultCallback onResult);
    
    [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern void create_scalar_index(long connectionHandle, long tableHandle, string columnName,
        uint indexType, bool replace, ResultCallback onResult);
    
    [LibraryImport(DllName)]
    [UnmanagedCallConv(CallConvs = new Type[] { typeof(System.Runtime.CompilerServices.CallConvCdecl) })]
    internal static partial void drop_database(long connectionHandle, ResultCallback onResult);
    
    internal static byte[] SerializeSchemaOnly(Schema schema)
    {
        using var ms = new MemoryStream();
        using (var writer = new ArrowFileWriter(ms, schema))
        {
            writer.WriteEnd();
        }

        return ms.ToArray();
    }
    
    internal static byte[] SerializeRecordBatch(RecordBatch recordBatch)
    {
        using var ms = new MemoryStream();
        using (var writer = new ArrowStreamWriter(ms, recordBatch.Schema))
        {
            writer.WriteRecordBatch(recordBatch);
        }

        return ms.ToArray();
    }
    
    internal static Schema DeserializeSchema(byte[] schemaBytes)
    {
        using var ms = new MemoryStream(schemaBytes);
        using var reader = new ArrowFileReader(ms);
        return reader.Schema;
    }
}

