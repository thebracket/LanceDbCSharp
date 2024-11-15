using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using LanceDbInterface;
using Array = Apache.Arrow.Array;

namespace LanceDbClient;

static partial class Ffi
{
    // Use this one for local builds
    //private const string DllName = "../../../../../../rust/target/debug/liblance_sync_client.so";
    // Use this one for Docker 
    private const string DllName = "liblance_sync_client.so";
    
    /* Delegate types */
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal unsafe delegate void BlobCallback(byte* schema, ulong schemaLength);

    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void ResultCallback(long code, string? message);

    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void StringCallback(string s);
    
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void CompactCallback(ulong fragmentsRemoved, ulong fragmentsAdded, ulong filesRemoved, ulong filesAdded);
    
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void PruneCallback(ulong removed, ulong added);

    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void UpdateCalback(ulong updatedRows);

    /* FFI functions */
    [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern void connect(string uri, ResultCallback onResult);
    
    [LibraryImport(DllName)]
    [UnmanagedCallConv(CallConvs = new Type[] { typeof(System.Runtime.CompilerServices.CallConvCdecl) })]
    internal static partial void disconnect(long handle, ResultCallback onResult);
    
    [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern unsafe void create_empty_table(string name, long connectionHandle, byte* schema, ulong schemaLength, ResultCallback onResult);
    
    [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern void open_table(string name, long connectionHandle, BlobCallback callback, ResultCallback onResult);
    
    [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern void drop_table(string name, long connectionHandle, bool ignore_missing, ResultCallback onResult);
    
    [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern void rename_table(long connectionHandle, string oldName, string newName, ResultCallback onResult);
    
    [LibraryImport(DllName)]
    [UnmanagedCallConv(CallConvs = new Type[] { typeof(System.Runtime.CompilerServices.CallConvCdecl) })]
    internal static partial void list_table_names(long connectionHandle, StringCallback stringCallback, ResultCallback onResult);

    [LibraryImport(DllName)]
    [UnmanagedCallConv(CallConvs = new Type[] { typeof(System.Runtime.CompilerServices.CallConvCdecl) })]
    internal static partial void close_table(long connectionId, long tableId, ResultCallback onResult);

    [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
    [UnmanagedCallConv(CallConvs = new Type[] { typeof(System.Runtime.CompilerServices.CallConvCdecl) })]
    internal static extern unsafe void add_record_batch(long connectionId, long TableId, byte* data, ulong len, uint write_mode, uint bad_vector_handling, float fill_value, ResultCallback onResult);
    
    [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern void count_rows(long connectionHandle, long tableHandle, string? filter, ResultCallback onResult);
    
    [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern unsafe void merge_insert_with_record_batch(long connectionId, long tableId, string[] columns, ulong columnsLength, bool when_not_matched_insert_all,
        string? where_clause, string? when_not_matched_by_source_delete, byte* data, ulong batch_len, ResultCallback onResult);
    
    [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern void delete_rows(long connectionHandle, long tableHandle, string? filter, ResultCallback onResult);
    
    [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern void optimize_table(long connectionHandle, long tableHandle, ResultCallback onResult, CompactCallback compactCallback, PruneCallback pruneCallback);
    
    [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern void query(long connectionHandle, long tableHandle, BlobCallback onRecBatch, 
        ResultCallback onResult, ulong limit, string? whereClause, bool withRowId,
        string[] columns, ulong columnsLength, string? fullTextSearch, uint batchSize);
    
    [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern unsafe void vector_query(long connectionHandle, long tableHandle, BlobCallback onRecBatch, 
        ResultCallback onResult, ulong limit, string? whereClause, bool withRowId,
        string[] columns, ulong columnsLength, uint vectorType, byte* vectorBlob,
        ulong vectorBlogLength, ulong numElements, uint metric, ulong nProbes, uint refineFactor,
        uint batchSize);
    
    [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern unsafe void explain_vector_query(long connectionHandle, long tableHandle, 
        ResultCallback onResult, ulong limit, string? whereClause, bool withRowId,
        bool verbose, StringCallback stringCallback,
        string[] columns, ulong columnsLength,
        uint vectorType, byte* vectorBlob,
        ulong vectorBlogLength, ulong numElements,
        uint metric, ulong nProbes, uint refineFactor);
    
    [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern void explain_query(long connectionHandle, long tableHandle, ulong limit, 
        string? whereClause, bool withRowId, bool verbose, StringCallback stringCallback, 
        ResultCallback onResult, string[] columns, ulong columnsLength, string? fullTextSearch);

    
    [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern void create_scalar_index(long connectionHandle, long tableHandle, string columnName,
        uint indexType, bool replace, ResultCallback onResult);
    
    [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern void create_full_text_index(long connectionHandle, long tableHandle, 
        string[] columns, ulong columnsLength, bool withPosition, bool replace, string tokenizerName, 
        ResultCallback onResult);
    
    [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern void create_index(long connectionHandle, long tableHandle, string columnName, uint metric, uint numPartitions, uint numSubVectors, bool replace, ResultCallback onResult);
    
    [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
    internal static extern void update_rows(long connectionHandle, long tableHandle, string[] updates, ulong updatesLength, string? where, ResultCallback onResult, UpdateCalback updateCalback);
    
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
        using (var writer = new ArrowFileWriter(ms, recordBatch.Schema))
        {
            writer.WriteRecordBatch(recordBatch);
            writer.WriteEnd();
        }

        return ms.ToArray();
    }
    
    internal static byte[] SerializeArrowArray(Array array)
    {
        // Define the schema for the array, using its data type and a field name
        var field = new Field("data", array.Data.DataType, nullable: true);
        var schema = new Schema.Builder().Field(field).Build();

        // Wrap the array in a RecordBatch
        var recordBatch = new RecordBatch(schema, new List<IArrowArray> { array }, array.Length);

        // Serialize the RecordBatch to bytes
        using var memoryStream = new MemoryStream();
        using (var writer = new ArrowFileWriter(memoryStream, recordBatch.Schema))
        {
            writer.WriteRecordBatch(recordBatch);
            writer.WriteEnd();
        }
    
        return memoryStream.ToArray();
    }
    
    internal static Schema DeserializeSchema(byte[] schemaBytes)
    {
        using var ms = new MemoryStream(schemaBytes);
        using var reader = new ArrowFileReader(ms);
        return reader.Schema;
    }
    
    internal static RecordBatch DeserializeRecordBatch(byte[] batch)
    {
        using var ms = new MemoryStream(batch);
        using var reader = new ArrowFileReader(ms);
        return reader.ReadNextRecordBatch();
    }
}

