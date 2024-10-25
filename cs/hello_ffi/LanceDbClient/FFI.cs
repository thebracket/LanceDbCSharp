using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Ipc;

static class FFI
{
    private const string DllName = "../../../../../../rust/target/debug/liblance_sync_client.so";
    
    [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int setup();
    
    [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int shutdown();
    
    [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern long connect(string uri);
    
    [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern long disconnect(long handle);
    
    [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern unsafe long submit_record_batch(byte* data, ulong length);

    [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern long free_record_batch(long handle);

    [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern long create_table(string name, long connectionHandle, long recordHandle);
    
    [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern long open_table(string name, long connectionHandle);
    
    [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern long drop_table(string name, long connectionHandle);
    
    [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern long drop_database(long connectionHandle);

    [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern unsafe long query_nearest_to(long limit, float* vector, ulong vectorLength, long tableHandle);

    [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern long free_blob(long blobHandle);

    [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern long blob_len(long blobHandle);

    // Is there a tag for "really unsafe"? Because this is it.
    [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern unsafe byte* get_blob_data(long blobHandle);
    
    [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
    private static extern string get_error_message(long index);
    
    [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
    private static extern void free_error_message(long index);
    
    internal static byte[] SerializeSchemaOnly(Schema schema)
    {
        using var ms = new MemoryStream();
        using (var writer = new ArrowFileWriter(ms, schema))
        {
            writer.WriteEnd();
        }

        return ms.ToArray();
    }

    internal static string GetErrorMessageOnce(long index)
    {
        var message = get_error_message(index);
        free_error_message(index);
        return message;
    }
}

