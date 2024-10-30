// Helpers

using Apache.Arrow;
using Apache.Arrow.Types;

namespace LanceDbClientTests;

internal static class Helpers
{
    internal static Schema GetSchema()
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
}