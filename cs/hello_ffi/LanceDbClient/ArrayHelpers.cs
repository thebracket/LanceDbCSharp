using System.Collections;
using System.Data.Common;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using Apache.Arrow;
using Apache.Arrow.Types;
using Array = Apache.Arrow.Array;

namespace LanceDbClient;

public static class ArrayHelpers
{
    private static object GetValue<T>(Array array, long rowCount)
        where T: struct, IEquatable<T>
    {
        var length = array.Length;
        // Uncomment if you want to support single values
        /*if (length == 1)
        {
            return ((PrimitiveArray<T>)array).GetValue(0).Value;
        }*/

        var val = new List<T>();
        for (var k=0; k<length; k++)
        {
            var value = ((PrimitiveArray<T>)array).GetValue(k);
            if (value == null) continue;
            val.Add(value.Value);
        }

        if (length == 1) return val;
        
        // Set the chunk size to equal the number of elements in the array divided by rowCount
        var chunkSize = (int)(length / rowCount);
        var chunks = new ArrayList { val.Chunk(chunkSize) };
        return chunks;
    }
    
    public static object ArrowArrayDataToConcrete(object array, int depth=0, long rowCount=1)
    {
        object val;
        switch (array)
        {
            case Int32Array intArray: val = GetValue<int>(intArray, rowCount); break;
            case FloatArray floatArray: val = GetValue<float>(floatArray, rowCount); break;
            case DoubleArray doubleArray: val = GetValue<double>(doubleArray, rowCount); break;
            case StringArray stringArray:
                var stringList = new List<string>();
                for (var k = 0; k < stringArray.Length; k++)
                {
                    stringList.Add(stringArray.GetString(k));
                }

                if (stringList.Count == 1)
                {
                    val = stringList;
                }
                else
                {
                    var chunkSize = (int)(stringList.Count / rowCount);
                    var chunks = new ArrayList { stringList.Chunk(chunkSize) };
                    val = chunks;
                }
                break;
            case BooleanArray boolArray: val = GetValue<bool>(boolArray, rowCount); break;
            case Int8Array int8Array: val = GetValue<sbyte>(int8Array, rowCount); break;
            case Int16Array int16Array: val = GetValue<short>(int16Array, rowCount); break;
            case Int64Array int64Array: val = GetValue<long>(int64Array, rowCount); break;
            case UInt8Array uint8Array: val = GetValue<byte>(uint8Array, rowCount); break;
            case UInt16Array uint16Array: val = GetValue<ushort>(uint16Array, rowCount); break;
            case UInt32Array uint32Array: val = GetValue<uint>(uint32Array, rowCount); break;
            case UInt64Array uint64Array: val = GetValue<ulong>(uint64Array, rowCount); break;
            case Date32Array date32Array: val = GetValue<int>(date32Array, rowCount); break;
            case Date64Array date64Array: val = GetValue<long>(date64Array, rowCount); break;
            case TimestampArray timestampArray: val = GetValue<long>(timestampArray, rowCount); break;
            case Time32Array time32Array: val = GetValue<int>(time32Array, rowCount); break;
            case Time64Array time64Array: val = GetValue<long>(time64Array, rowCount); break;
            case Decimal128Array decimal128Array: val = GetValue<decimal>(decimal128Array, rowCount); break;
            case Decimal256Array decimal256Array: val = GetValue<decimal>(decimal256Array, rowCount); break;
            case ListArray listArray:
                if (depth > 1) throw new NotSupportedException("List depth > 1 is not supported.");
                val = ArrowArrayDataToConcrete(listArray.Values, depth + 1, rowCount); // Returns as Apache.Arrow.Array
                break;
            
            case BinaryArray binaryArray:
                var length = binaryArray.Length;
                var tmp = new List<byte[]>();
                for (var k=0; k<length; k++)
                {
                    var value = binaryArray.GetBytes(k);
                    if (value == null) continue;
                    tmp.Add(value.ToArray());
                }
                val = tmp;
                break;
            
            // We have to repeat on the fixed size array type - it contains an inner array
            case FixedSizeListArray fixedSizeListArray:
                if (depth > 1) throw new NotSupportedException("Fixed size list depth > 1 is not supported.");
                val = ArrowArrayDataToConcrete(fixedSizeListArray.Values, depth+1, rowCount); // Returns as Apache.Arrow.Array
                break;
            default:
                throw new NotSupportedException($"Array type {array.GetType()} is not supported.");
        }

        return val;
    }

    private static bool DoesTypeMatchSchema(object? o, ArrowTypeId s)
    {
        if (o == null) return false;
        // TODO: Check for completeness
        if (o is List<int> && s == ArrowTypeId.Int32) return true;
        if (o is List<float> && s == ArrowTypeId.Float) return true;
        if (o is List<double> && s == ArrowTypeId.Double) return true;
        if (o is List<string> && s == ArrowTypeId.String) return true;
        if (o is List<bool> && s == ArrowTypeId.Boolean) return true;
        if (o is List<sbyte> && s == ArrowTypeId.Int8) return true;
        if (o is List<short> && s == ArrowTypeId.Int16) return true;
        if (o is List<long> && s == ArrowTypeId.Int64) return true;
        if (o is List<byte> && s == ArrowTypeId.UInt8) return true;
        if (o is List<ushort> && s == ArrowTypeId.UInt16) return true;
        if (o is List<uint> && s == ArrowTypeId.UInt32) return true;
        if (o is List<ulong> && s == ArrowTypeId.UInt64) return true;
        if (o is List<int> && s == ArrowTypeId.Date32) return true;
        if (o is List<long> && s == ArrowTypeId.Date64) return true;
        if (o is List<long> && s == ArrowTypeId.Timestamp) return true;
        if (o is List<int> && s == ArrowTypeId.Time32) return true;
        if (o is List<long> && s == ArrowTypeId.Time64) return true;
        if (o is List<decimal> && s == ArrowTypeId.Decimal128) return true;
        if (o is List<decimal> && s == ArrowTypeId.Decimal256) return true;
        if (o is List<byte[]> && s == ArrowTypeId.Binary) return true;
        return false;
    }

    private static FixedSizeListArray ToFixedListArray(IArrowType type, ArrayData? data)
    {
        var fixedSizeListType = (FixedSizeListType)type;
        var fixedSizeListArrayData = new ArrayData(
            fixedSizeListType,
            length: 1,
            nullCount: 0,
            buffers: [ArrowBuffer.Empty], // No null bitmap buffer, assuming all are valid
            children: [data]);
        return new FixedSizeListArray(fixedSizeListArrayData);
    }
    
    private static void AddToArrayOrWrapInList(List<IArrowArray> arrayRows, IArrowType t, IArrowArray array)
    {
        if (t.TypeId == ArrowTypeId.FixedSizeList)
        {
            arrayRows.Add(ToFixedListArray(t, array.Data));
        }
        else
        {
            arrayRows.Add(array);
        }
    }

    private static object? ArrayBuilderFactory(ArrowTypeId t)
    {
        if (t == ArrowTypeId.Int32) return new Int32Array.Builder();
        if (t == ArrowTypeId.Float) return new FloatArray.Builder();
        if (t == ArrowTypeId.Double) return new DoubleArray.Builder();
        if (t == ArrowTypeId.String) return new StringArray.Builder();
        if (t == ArrowTypeId.Boolean) return new BooleanArray.Builder();
        if (t == ArrowTypeId.Int8) return new Int8Array.Builder();
        if (t == ArrowTypeId.Int16) return new Int16Array.Builder();
        if (t == ArrowTypeId.Int64) return new Int64Array.Builder();
        if (t == ArrowTypeId.UInt8) return new UInt8Array.Builder();
        if (t == ArrowTypeId.UInt16) return new UInt16Array.Builder();
        if (t == ArrowTypeId.UInt32) return new UInt32Array.Builder();
        if (t == ArrowTypeId.UInt64) return new UInt64Array.Builder();
        if (t == ArrowTypeId.Date32) return new Date32Array.Builder();
        if (t == ArrowTypeId.Date64) return new Date64Array.Builder();
        if (t == ArrowTypeId.Timestamp) return new TimestampArray.Builder();
        if (t == ArrowTypeId.Time32) return new Time32Array.Builder();
        if (t == ArrowTypeId.Time64) return new Time64Array.Builder();
        if (t == ArrowTypeId.Binary) return new BinaryArray.Builder();
        
        return null;
    }

    // Helper type for the ConcreteToArrowTable method
    private struct FieldData
    {
        internal object builder;
        internal MethodInfo appendMethod;
        internal Type subType;
        internal bool isFixedSizeArray;
        internal IArrowType typeForBuilder;
        internal List<IArrowArray> fixedTypeArrays;
        internal IArrowType parentType;
    }
    
    public static List<RecordBatch> ConcreteToArrowTable(IEnumerable<Dictionary<string, object>> data, Schema schema)
    {
        var result = new List<RecordBatch>();

        // Build a column-first dictionary. Every item contains a builder, so we end up with a big array
        // containing all rows of for that field.
        var columns = new Dictionary<string, FieldData>();
        foreach (var field in schema.Fields)
        {
            // Construct the builder
            bool isFixedSizedArray = false;
            var typeForBuilder = field.Value.DataType;
            var parentType = typeForBuilder;
            if (typeForBuilder.TypeId == ArrowTypeId.FixedSizeList)
            {
                typeForBuilder = ((FixedSizeListType)typeForBuilder).ValueDataType;
                isFixedSizedArray = true;
            }
            var builder = ArrayBuilderFactory(typeForBuilder.TypeId);
            if (builder == null) throw new NotImplementedException("Type builder for " + typeForBuilder + " not implemented.");

            // Determine the enumerable subtype
            // Set val to the first value in the data
            var val = data.FirstOrDefault()[field.Value.Name];
            var dataType = val.GetType();
            var subType = dataType;
            if (dataType.GetGenericArguments().Length > 0)
            {
                // It's a list
                subType = dataType.GetGenericArguments()[0];
            }
            else
            {
                if (val is IEnumerable innerList)
                {
                    var first = innerList.Cast<object>().FirstOrDefault();
                    if (first != null)
                    {
                        subType = first.GetType();
                    }
                    else
                    {
                        throw new Exception("Could not determine subType for " + field.Key + ".");
                    }
                }
                else
                {
                    subType = dataType;
                }
            }
            
            // Construct the append method
            MethodInfo? appendMethod = builder.GetType().GetMethod("Append", subType == typeof(string) ? [typeof(string), typeof(Encoding)]
                : [subType]);
            if (appendMethod == null) throw new NotImplementedException("Append method for " + subType + " not implemented.");
            
            columns[field.Value.Name] = new FieldData()
            {
                builder = builder,
                appendMethod = appendMethod,
                subType = subType,
                isFixedSizeArray = isFixedSizedArray,
                typeForBuilder = typeForBuilder,
                fixedTypeArrays = [],
                parentType = parentType
            };
        }
        
        // Iterate all the rows
        foreach (var row in data)
        {
            // Iterate each column
            foreach (var col in row)
            {
                // Check that the field exists in the columns dictionary
                var targetBuilder = columns[col.Key];
                
                // Check that the field exists in the schema
                var schemaField = schema.GetFieldByName(col.Key);
                if (schemaField == null)
                {
                    throw new Exception("Field " + col.Key + " not found in schema.");
                }
                
                // Append the value(s) to the builder
                var val = col.Value;
                if (val is IEnumerable list)
                {
                    foreach (var value in list)
                    {
                        /*if (targetBuilder.isFixedSizeArray)
                        {
                            // We've constructed a fixed sized array rather than what we actually want.
                            // So we need to extract the array for further processing later, and then
                            // reset the builder.
                            var fixedArray = (IArrowArray)targetBuilder.builder.GetType().GetMethod("Build")!.Invoke(targetBuilder.builder, [null])!;
                            targetBuilder.fixedTypeArrays.Add(fixedArray);
                            
                            // (We already checked for null when we constructed it)
                            targetBuilder.builder = ArrayBuilderFactory(targetBuilder.typeForBuilder.TypeId)!;
                        }
                        else
                        {*/
                            // It's not a fixed size array, so just keep on appending
                            if (targetBuilder.subType == typeof(string))
                            {
                                targetBuilder.appendMethod.Invoke(targetBuilder.builder, [value, null]);
                            }
                            else
                            {
                                targetBuilder.appendMethod.Invoke(targetBuilder.builder, [value]);
                            }
                        //}
                    }
                }
                else
                {
                    // Handle naked types here
                    throw new Exception("Naked types not in enumerable are not supported.");
                }
            }
        }
        
        // Transform the dictionary to a list
        var allEntries = new List<IArrowArray>();
        foreach (var column in columns)
        {
            var array = (IArrowArray)column.Value.builder.GetType().GetMethod("Build")!.Invoke(column.Value.builder,
                [null])!;
            if (column.Value.isFixedSizeArray)
            {
                var total = data.Count();
                var fixedSizeListType = (FixedSizeListType)column.Value.parentType;
                var fixedSizeListArrayData = new ArrayData(
                    fixedSizeListType,
                    length: total,
                    nullCount: 0,
                    buffers: [ArrowBuffer.Empty], // No null bitmap buffer, assuming all are valid
                    children: [array.Data]);
                var fixedSizeListArray = new FixedSizeListArray(fixedSizeListArrayData);
                allEntries.Add(fixedSizeListArray);
            }
            else
            {
                AddToArrayOrWrapInList(allEntries, schema.GetFieldByName(column.Key).DataType, array);
            }
        }
        var recordBatch = new RecordBatch(schema, allEntries, length: data.Count());
        return [recordBatch];

        /*
        /// OLD CODE
        foreach (var row in data)
        {
            List<IArrowArray> arrayRows = new List<IArrowArray>();

            // Foreach item in the row dictionary
            foreach (var item in row) {
                // Get the field from the schema
                var field = schema.GetFieldByName(item.Key);
                if (field == null) throw new Exception("Field " + item.Key + " not found in schema.");

                // Get the type from the schema
                var type = field.DataType;
                if (type == null) throw new Exception("Type not found in schema.");

                // Check if the type matches the schema
                if (type.TypeId == ArrowTypeId.FixedSizeList)
                {
                    // Get the inner type
                    var innerType = ((FixedSizeListType)type).ValueDataType;
                    //if (!DoesTypeMatchSchema(item.Value, innerType.TypeId)) throw new Exception("Type mismatch for " + item.Key + ". Expected " + innerType.TypeId + ", got " + item.Value.GetType() + ".");

                } else if (item.Value is string[] || item.Value is System.Single[] || item.Value is UInt64[] )
                {
                    // String[] isn't a list
                }
                else if (!DoesTypeMatchSchema(item.Value, type.TypeId)) throw new Exception("Type mismatch for " + item.Key + ". Expected " + type.TypeId + ", got " + item.Value.GetType() + ".");

                // Get the array builder
                // TODO: Many more builders required
                var val = item.Value;
                if (val is string[] strings)
                {
                    val = strings.ToList();
                } else if (val is float[] floats)
                {
                    val = floats.ToList();
                } else if (val is ulong[] ulongs)
                {
                    val = ulongs.ToList();
                }
                var baseType = val.GetType();
                var subType = baseType.GetGenericArguments()[0];
                object? builder;
                if (type.TypeId == ArrowTypeId.FixedSizeList)
                {
                    // Get the inner type from the schema
                    var innerType = ((FixedSizeListType)type).ValueDataType;
                    builder = ArrayBuilderFactory(innerType.TypeId);
                }
                else
                {
                    builder = ArrayBuilderFactory(type.TypeId);
                }
                if (builder == null) throw new NotImplementedException("Type builder for " + type.TypeId + " not implemented.");

                MethodInfo? appendMethod = builder.GetType().GetMethod("Append", subType == typeof(string) ? [typeof(string), typeof(Encoding)]
                    : [subType]);
                if (appendMethod == null) throw new NotImplementedException("Append method for " + subType + " not implemented.");

                var list = item.Value as IList;
                if (list != null)
                {
                    foreach (var value in list)
                    {
                        if (subType == typeof(string))
                        {
                            appendMethod.Invoke(builder, [value, null]);
                        }
                        else
                        {
                            appendMethod.Invoke(builder, [value]);
                        }
                    }
                }

                var array = (IArrowArray)builder.GetType().GetMethod("Build")!.Invoke(builder, [null])!;
                AddToArrayOrWrapInList(arrayRows, type, array);
            }
            var recordBatch = new RecordBatch(schema, arrayRows.ToArray(), length: 1);
            result.Add(recordBatch);
        }

        return result;*/
    }
    
    public enum TypeIndex
    {
        Half = 1,
        Float = 2,
        Double = 3,
        ArrowArray = 4,
    }

    public class VectorDataImpl
    {
        public byte[] Data;
        public ulong Length;
        public TypeIndex DataType;
    }
    
    internal static VectorDataImpl CastVectorList<T>(List<T> vector)
    {
        // Calculate the buffer size
        var bufferSize = vector.Count * Unsafe.SizeOf<T>();
        // Adjust size to ensure 32-bit alignment
        if (bufferSize % 4 != 0)
        {
            bufferSize += 4 - (bufferSize % 4);
        }
        // Allocate byte array
        var data = new byte[bufferSize];
        Buffer.BlockCopy(vector.ToArray(), 0, data, 0, data.Length);

        if (typeof(T) == typeof(Half))
        {
            return new VectorDataImpl
            {
                Data = data,
                Length = (ulong)vector.Count,
                DataType = TypeIndex.Half
            };
        }
        if (typeof(T) == typeof(float))
        {
            return new VectorDataImpl
            {
                Data = data,
                Length = (ulong)vector.Count,
                DataType = TypeIndex.Float
            };
        }
        if (typeof(T) == typeof(double))
        {
            return new VectorDataImpl
            {
                Data = data,
                Length = (ulong)vector.Count,
                DataType = TypeIndex.Double
            };
        }
        
        throw new Exception("Unsupported type: " + typeof(T) + ". Supported types are Half, float, double, and Apache.Arrow.Array.");
    }

    internal static IList<RecordBatch> ArrowTableToRecordBatch(Apache.Arrow.Table data)
    {
        // Extract the schema from the table
        Schema schema = data.Schema;

        // Create a RecordBatch from the table
        var arrays = new List<IArrowArray>();
        for (int i = 0; i < data.ColumnCount; i++)
        {
            var chunkedArray = data.Column(i).Data;
            var count = chunkedArray.ArrayCount; 
            for (int n = 0; n < count; n++)
            {
                var array = chunkedArray.ArrowArray(n);
                arrays.Add(array);
            }
        }
        var recordBatch = new RecordBatch(schema, arrays, (int)data.RowCount);
        var recordBatches = new List<RecordBatch> { recordBatch };
        return recordBatches;
    }
    
    internal static Apache.Arrow.Table ConcatTables(IList<Apache.Arrow.Table> tables)
    {
        if (tables == null || tables.Count == 0)
        {
            throw new ArgumentException("Concat requires input of at least one table.");
        }
        else if (tables.Count == 1)
        {
            return tables[0];
        }

        var schema = tables[0].Schema;
        /*if (!SchemaMatch(schema, tables[1].Schema))
        {
            throw new ArgumentException("Schema mismatch between tables.");
        }*/

        List<RecordBatch> combinedRecordBatches = new List<RecordBatch>();

        foreach (var table in tables)
        {
            combinedRecordBatches.AddRange(ArrowTableToRecordBatch(table));
        }

        var concatenatedTable = Apache.Arrow.Table.TableFromRecordBatches(schema, combinedRecordBatches);

        return concatenatedTable;
    }

    public static IEnumerable<IDictionary<string, object>> ArrowTableToListOfDicts(Apache.Arrow.Table table)
    {
        var result = new List<IDictionary<string, object>>();
        
        // Pre-fill the list with empty dictionaries
        for (var i = 0; i < table.RowCount; i++)
        {
            result.Add(new Dictionary<string, object>());
        }
        
        for (var j = 0; j < table.ColumnCount; j++)
        {
            var column = table.Column(j);
            for (var colIdx = 0 ; colIdx < column.Data.ArrayCount; colIdx++)
            {
                var raw = ArrayHelpers.ArrowArrayDataToConcrete(column.Data.Array(colIdx), rowCount:(int)column.Data.Array(colIdx).Length);
                if (raw is ArrayList chunked)
                {
                    if (chunked[0] is IEnumerable inner)
                    {
                        var rowIdx = 0;
                        foreach(var row in inner)
                        {
                            result[rowIdx][column.Name] = row;
                            rowIdx++;
                        }
                    }
                    else
                    {
                        throw new Exception("Unexpected chunked data");
                    }
                }
                else
                {
                    result[colIdx][column.Name] = raw;
                }
            }
        }
        return result;
    }
    
    internal static List<ulong>? ArrowTableUint64ColumnToList(Apache.Arrow.Table table, string name)
    {
        for (var i = 0; i < table.ColumnCount; i++)
        {
            var col = table.Column(i);
            if (col.Name == name)
            {
                // We found it
                var result = new List<ulong>();
                for (var j = 0; j < col.Data.ArrayCount; j++)
                {
                    var array = col.Data.Array(j);
                    if (array is UInt64Array idArray)
                    {
                        for (var k = 0; k < idArray.Length; k++)
                        {
                            var value = idArray.GetValue(k);
                            if (value == null) continue;
                            result.Add((ulong)value);
                        }
                    }
                    else
                    {
                        throw new Exception("Column " + name + " is not a string column. It is a " + array.GetType() + ".");
                    }
                }
                return result;
            }
        }

        return null;
    }
    
    internal static bool TableContainsColumn(Apache.Arrow.Table table, string name)
    {
        for (var i = 0; i < table.ColumnCount; i++)
        {
            if (table.Column(i).Name == name)
            {
                return true;
            }
        }

        return false;
    }
    
    internal static Apache.Arrow.Table DropColumns(Apache.Arrow.Table table, List<string> columns)
    {
        foreach (var column in columns)
        {
            for (var i = 0; i < table.ColumnCount; i++)
            {
                if (table.Column(i).Name == column)
                {
                    table = table.RemoveColumn(i);
                    break;
                }
            }
        }

        return table;
    }
    
    internal static Apache.Arrow.Table AppendFloatColumn(Apache.Arrow.Table table, string name, List<float> values, IArrowType type)
    {
        // Validate
        if (values == null || values.Count != table.RowCount)
        {
            throw new ArgumentException("Values list is null or does not match the row count of the table.");
        }
        if (type.TypeId != ArrowTypeId.Float)
        {
            throw new ArgumentException("Type provided is not a FloatType.");
        }

        // Build the array
        var builder = new FloatArray.Builder();
        foreach (var value in values)
        {
            builder.Append(value);
        }
        var floatArray = builder.Build();

        // Build the column
        var field = new Field(name, type, false, null);
        var column = new Column(field, [(IArrowArray)floatArray]);
        var newTable = table.InsertColumn(table.ColumnCount, column);
        
        // Modify the schema to add the column
        var fieldIndex = table.Schema.FieldsList.Count;
        newTable.Schema.InsertField(fieldIndex, field);
        
        return newTable;
    }
    
    internal static Apache.Arrow.Table SortBy(Apache.Arrow.Table table, string column, bool descending)
    {
        // Find the column
        if (!TableContainsColumn(table, column))
        {
            throw new ArgumentException("Column " + column + " not found in the table.");
        }
        
        // Extract the column. This gives us an array of arrays.
        var native = ArrowTableToListOfDicts(table).ToList();
        var targetColumn = native.Select(row => row[column]).ToList();
        // Transform IList<Object> to IList<(Object, int)>. The (int) should be the enumerator.
        var withIndex = new List<(object, int)>();
        for (var i = 0; i < targetColumn.Count; i++)
        {
            withIndex.Add((targetColumn[i], i));
        }
        // Sort withIndex by the object
        withIndex.Sort((a, b) =>
        {
            if (a.Item1 is IList listA)
            {
                if (b.Item1 is IList listB)
                {
                    // Compare the first element of the list
                    if (listA[0] is IComparable comparableA && listB[0] is IComparable comparableB)
                    {
                        return descending ? comparableB.CompareTo(comparableA) : comparableA.CompareTo(comparableB);
                    }
                }
            }

            throw new ArgumentException("Column " + column + " is not comparable.");
        });
        
        // Extract just the index from withIndex
        var sortedIndices = withIndex.Select(x => x.Item2).ToList();
        // If descending, reverse the list
        if (descending)
        {
            sortedIndices.Reverse();
        }
        
        // Reorder the table
        var reordered = new List<IDictionary<string, object>>();
        foreach (var index in sortedIndices)
        {
            reordered.Add(native[index]);
        }
        
        // Convert back to Arrow Table
        var schema = table.Schema;
        
        // Populate listOfDicts
        var convertedList = reordered
            .Select(dict => dict.ToDictionary(entry => entry.Key, entry => entry.Value))
            .ToList();
        // Remove any items where the dictionary only contains one item
        convertedList = convertedList.Where(dict => dict.Count > 1).ToList();

        var result = ConcreteToArrowTable(convertedList, schema);
        return Apache.Arrow.Table.TableFromRecordBatches(table.Schema, result);
    }

    internal static bool SchemaMatch(Schema a, Schema b)
    {
        if (a.FieldsList.Count != b.FieldsList.Count) return false;
        for (var i = 0; i < a.FieldsList.Count; i++)
        {
            if (a.GetFieldByIndex(i).Name != b.GetFieldByIndex(i).Name) return false;
            if (a.GetFieldByIndex(i).DataType.TypeId != b.GetFieldByIndex(i).DataType.TypeId) return false;
        }
        return true;
    }
}