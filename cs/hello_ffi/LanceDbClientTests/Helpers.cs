// Helpers

using Apache.Arrow;
using Apache.Arrow.Types;

namespace LanceDbClientTests;

internal static class Helpers
{
    internal static Schema GetSchema()
    {
        // Define the "id" field (Int32, not nullable)
        var idField = new Field("id", StringType.Default, nullable: false);

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
    
    internal static RecordBatch CreateSampleRecordBatch(Schema schema, int total, int dim)
    {
        // Step 1: Create Int32Array for the "id" field
        var idBuilder = new StringArray.Builder();
        for (int i = 0; i < total; i++)
        {
            idBuilder.Append(i.ToString());
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
}