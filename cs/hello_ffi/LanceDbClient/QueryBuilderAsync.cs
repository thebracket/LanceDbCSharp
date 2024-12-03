using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading.Channels;
using Apache.Arrow;
using LanceDbInterface;
using MathNet.Numerics.LinearAlgebra;

namespace LanceDbClient;

public partial class QueryBuilder
{
    public async Task<Apache.Arrow.Table> ToArrowAsync(CancellationToken token = default)
    {
        var batches = new List<RecordBatch>();
        await foreach (var batch in ToBatchesAsync(0, token))
        {
            batches.Add(batch);
        }
        var schema = batches.First().Schema;
        var table = Apache.Arrow.Table.TableFromRecordBatches(schema, batches);
        return table;
    }

    public async Task<IEnumerable<IDictionary<string, object>>> ToListAsync(CancellationToken token = default)
    {
        var table = await ToArrowAsync(token);
        var result = ArrayHelpers.ArrowTableToListOfDictionaries(table);
        return result;
    }
    
    public async IAsyncEnumerable<RecordBatch> ToBatchesAsync(int batchSize, [EnumeratorCancellation] CancellationToken token = default)
    {
        string[]? selectColumns = null;
        if (SelectColumnsList.Count > 0)
        {
            selectColumns = SelectColumnsList.ToArray();
        }

        var channel = Channel.CreateUnbounded<RecordBatch>();
        
        Ffi.ResultCallback resultCallback = (code, message) =>
        {
            // If an error occurred, turn it into an exception
            if (code < 0 && message != null)
            {
                throw new Exception("Failed to compact files: " + message);
            }
        };
        
        _ = Task.Run(() =>
        {
            unsafe
            {
                Ffi.BlobCallback blobCallback = (bytes, len) =>
                {
                    // Marshall schema/length into a managed object
                    var schemaBytes = new byte[len];
                    Marshal.Copy((IntPtr)bytes, schemaBytes, 0, (int)len);
                    var batch = Ffi.DeserializeRecordBatch(schemaBytes);
                    channel.Writer.TryWrite(batch);
                    return !token.IsCancellationRequested;
                };

                Ffi.query(ConnectionId, TableId, blobCallback, resultCallback, LimitCount, WhereSql, WithRowIdent,
                    selectColumns!, (ulong)SelectColumnsList.Count,
                    FullTextSearch, (uint)batchSize);
                channel.Writer.Complete();
            }


        }, token);

        while (await channel.Reader.WaitToReadAsync(token))
        {
            while (channel.Reader.TryRead(out var batch))
            {
                yield return batch;
            }
        }
    }

}