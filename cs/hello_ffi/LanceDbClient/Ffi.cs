using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Ipc;

namespace LanceDbClient;

static partial class Ffi
{
    // Use this one for local builds
    private const string DllName = "../../../../../../rust/target/debug/liblance_sync_client.so";
    // Use this one for Docker 
    //private const string DllName = "liblance_sync_client.so";
    
    [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern void connect(string uri, ResultCallback onResult);
    
    [LibraryImport(DllName)]
    [UnmanagedCallConv(CallConvs = new Type[] { typeof(System.Runtime.CompilerServices.CallConvCdecl) })]
    internal static partial void disconnect(long handle, ResultCallback onResult);
    
    [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern unsafe long create_empty_table(string name, long connectionHandle, byte* schema, ulong schemaLength);
    
    [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern unsafe long create_table(string name, long connectionHandle,byte* records, ulong recordsLength);
    
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal unsafe delegate void SetSchemaCallback(byte* schema, ulong schemaLength);

    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal unsafe delegate void ResultCallback(long code, string? message);

    
    [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern long open_table(string name, long connectionHandle, SetSchemaCallback callback);
    
    [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern long drop_table(string name, long connectionHandle);

    [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern unsafe long add_rows(long connectionHandle, long tableHandle, byte* data, ulong dataLength);
    
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void StringCallback(string s);
    
    [LibraryImport(DllName)]
    [UnmanagedCallConv(CallConvs = new Type[] { typeof(System.Runtime.CompilerServices.CallConvCdecl) })]
    internal static partial void list_table_names(long connectionHandle, StringCallback stringCallback, ResultCallback onResult);

    [LibraryImport(DllName)]
    [UnmanagedCallConv(CallConvs = new Type[] { typeof(System.Runtime.CompilerServices.CallConvCdecl) })]
    internal static partial long close_table(long connectionId, long tableId);

    [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern long count_rows(long connectionHandle, long tableHandle);
    
    [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern long create_scalar_index(long connectionHandle, long tableHandle, string columnName,
        uint indexType, bool replace);
    
    [LibraryImport(DllName)]
    [UnmanagedCallConv(CallConvs = new Type[] { typeof(System.Runtime.CompilerServices.CallConvCdecl) })]
    internal static partial void drop_database(long connectionHandle, ResultCallback onResult);
    
    
    [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
    private static extern string get_error_message(long index);
    
    [LibraryImport(DllName)]
    [UnmanagedCallConv(CallConvs = new Type[] { typeof(System.Runtime.CompilerServices.CallConvCdecl) })]
    private static partial void free_error_message(long index);
    
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

    internal static string GetErrorMessageOnce(long index)
    {
        var message = get_error_message(index);
        free_error_message(index);
        return message;
    }
}

