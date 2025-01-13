using System.Collections;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using Apache.Arrow;
using Apache.Arrow.Types;
using LanceDbInterface;
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
        internal object Builder;
        internal MethodInfo AppendMethod;
        internal Type SubType;
        internal bool IsFixedSizeArray;
        internal IArrowType ParentType;
    }
    
    public static List<RecordBatch> ConcreteToArrowTable(IEnumerable<Dictionary<string, object>> data, Schema schema)
    {
        // Build a column-first dictionary. Every item contains a builder, so we end up with a big array
        // containing all rows of for that field.
        var columns = new Dictionary<string, FieldData>();
        foreach (var field in schema.FieldsList)
        {
            // Construct the builder
            bool isFixedSizedArray = false;
            var typeForBuilder = field.DataType;
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
            var val = data.FirstOrDefault()[field.Name];
            var dataType = val.GetType();
            Type? subType;
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
                        throw new Exception("Could not determine subType for " + field.Name + ".");
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
            
            columns[field.Name] = new FieldData()
            {
                Builder = builder,
                AppendMethod = appendMethod,
                SubType = subType,
                IsFixedSizeArray = isFixedSizedArray,
                ParentType = parentType
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
                        if (targetBuilder.SubType == typeof(string))
                        {
                            targetBuilder.AppendMethod.Invoke(targetBuilder.Builder, [value, null]);
                        }
                        else
                        {
                            targetBuilder.AppendMethod.Invoke(targetBuilder.Builder, [value]);
                        }
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
            var array = (IArrowArray)column.Value.Builder.GetType().GetMethod("Build")!.Invoke(column.Value.Builder,
                [null])!;
            if (column.Value.IsFixedSizeArray)
            {
                var total = data.Count();
                var fixedSizeListType = (FixedSizeListType)column.Value.ParentType;
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
    
    internal static Apache.Arrow.Table ConcatTables(IList<Apache.Arrow.Table> tables, bool deduplicate = false)
    {
        if (tables == null || tables.Count == 0)
        {
            throw new ArgumentException("Concat requires input of at least one table.");
        }
        if (tables.Count == 1)
        {
            return tables[0];
        }
        
        // Remove distance and score
        var workingSchema = tables[0].Schema;
        
        // Convert the field names into a set of strings
        var fieldNames = new HashSet<string>();
        for (var i = 0; i < workingSchema.FieldsList.Count; i++)
        {
            fieldNames.Add(workingSchema.GetFieldByIndex(i).Name);
        }
        
        // Iterate all the schemas, removing fields that are not common to all tables
        for (var i = 1; i < tables.Count; i++)
        {
            var schema = tables[i].Schema;
            for (var j = 0; j < schema.FieldsList.Count; j++)
            {
                var field = schema.GetFieldByIndex(j);
                if (!fieldNames.Contains(field.Name))
                {
                    workingSchema = workingSchema.RemoveField(j);
                }
            }
        }
        
        // Merge all entries into a single dictionary list, preserving only the columns that are common to all tables
        var mergedRows = new List<IDictionary<string, object>>();
        var seenRowIds = new HashSet<ulong>();
        foreach (var table in tables)
        {
            var rows = ArrowTableToListOfDictionaries(table);
            foreach (var row in rows)
            {
                var performAdd = true;
                var newRow = new Dictionary<string, object>();
                foreach (var key in row.Keys)
                {
                    if (deduplicate && key == "_rowid")
                    {
                        object rowId = row[key];
                        if (rowId is IList<ulong> id)
                        {
                            if (seenRowIds.Contains(id[0]))
                            {
                                performAdd = false;
                            }
                            seenRowIds.Add(id[0]);
                        }
                    }
                    if (workingSchema.GetFieldByName(key) != null)
                    {
                        newRow[key] = row[key];
                    }
                }
                if (performAdd) mergedRows.Add(newRow);
            }
        }
        var convertedItems = mergedRows
            .Select(dict => dict.ToDictionary(entry => entry.Key, entry => entry.Value))
            .ToList();
        
        var result = ConcreteToArrowTable(convertedItems, workingSchema);
        return Apache.Arrow.Table.TableFromRecordBatches(workingSchema, result);
    }

    public static IEnumerable<IDictionary<string, object>> ArrowTableToListOfDictionaries(Apache.Arrow.Table table)
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
                var raw = ArrayHelpers.ArrowArrayDataToConcrete(column.Data.Array(colIdx), rowCount: column.Data.Array(colIdx).Length);
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
    
    internal static Apache.Arrow.Table SortBy(Apache.Arrow.Table table, string column, bool descending, int limit = 0)
    {
        // Find the column
        if (!TableContainsColumn(table, column))
        {
            throw new ArgumentException("Column " + column + " not found in the table.");
        }
        
        // Extract the column. This gives us an array of arrays.
        var native = ArrowTableToListOfDictionaries(table).ToList();
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
                        return descending ? comparableA.CompareTo(comparableB) : comparableB.CompareTo(comparableA);
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
        var count = 0;
        foreach (var index in sortedIndices)
        {
            if (limit > 0 && count >= limit) break;
            reordered.Add(native[index]);
            count++;
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

    internal static IEnumerable<Dictionary<string, object>> SanitizeVectorAdd(Schema tableSchema, IEnumerable<Dictionary<string, object>> rowsToAdd, BadVectorHandling mode, float fillValue)
    {
        // "Error" is the default LanceDb behavior - it will throw an error when a bad vector is encountered
        if (mode == BadVectorHandling.Error) return rowsToAdd;

        var result = new List<Dictionary<string, object>>();
        
        foreach (var row in rowsToAdd)
        {
            var dropped = false;
            foreach (var keyValuePair in row)
            {
                var fieldName = keyValuePair.Key;
                Field? schemaField = tableSchema.GetFieldByName(fieldName);
                if (schemaField == null) continue;
                var fieldType = schemaField.DataType;
                if (fieldType is FixedSizeListType arr)
                {
                    var desiredLength = arr.ListSize;
                    if (keyValuePair.Value is IList list)
                    {
                        if (list.Count != desiredLength)
                        {
                            if (mode == BadVectorHandling.Fill)
                            {
                                var fillList = new List<float>();
                                for (var i = 0; i < desiredLength; i++)
                                {
                                    fillList.Add(fillValue);
                                }
                                row[fieldName] = fillList;
                            } else if (mode == BadVectorHandling.Drop)
                            {
                                dropped = true;
                            }
                        }
                    }
                }
            }

            if (!dropped)
            {
                result.Add(row);
            }
        }

        return result;
    }
}