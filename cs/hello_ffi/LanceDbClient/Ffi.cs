using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Ipc;

namespace LanceDbClient;

static partial class Ffi
{
    private const string DllName = "../../../../../../rust/target/debug/liblance_sync_client.so";
    
    [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern long connect(string uri);
    
    [LibraryImport(DllName)]
    [UnmanagedCallConv(CallConvs = new Type[] { typeof(System.Runtime.CompilerServices.CallConvCdecl) })]
    internal static partial long disconnect(long handle);
    
    [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern unsafe long submit_record_batch(byte* data, ulong length);

    [LibraryImport(DllName)]
    [UnmanagedCallConv(CallConvs = new Type[] { typeof(System.Runtime.CompilerServices.CallConvCdecl) })]
    internal static partial long free_record_batch(long handle);

    [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern unsafe long create_empty_table(string name, long connectionHandle, byte* schema, ulong schemaLength);
    
    [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern unsafe long create_table(string name, long connectionHandle,byte* records, ulong recordsLength);
    
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal unsafe delegate void SetSchemaCallback(byte* schema, ulong schemaLength);
    
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
    internal static partial long list_table_names(long connectionHandle, StringCallback stringCallback);

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
    internal static partial long drop_database(long connectionHandle);

    [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern unsafe long query_nearest_to(long limit, float* vector, ulong vectorLength, long tableHandle);

    [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern long free_blob(long blobHandle);

    [LibraryImport(DllName)]
    [UnmanagedCallConv(CallConvs = new Type[] { typeof(System.Runtime.CompilerServices.CallConvCdecl) })]
    internal static partial long blob_len(long blobHandle);

    // Is there a tag for "really unsafe"? Because this is it.
    [LibraryImport(DllName)]
    [UnmanagedCallConv(CallConvs = new Type[] { typeof(System.Runtime.CompilerServices.CallConvCdecl) })]
    internal static unsafe partial byte* get_blob_data(long blobHandle);
    
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

