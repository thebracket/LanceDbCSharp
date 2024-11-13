using System.Collections;
using System.Reflection;
using System.Text;
using Apache.Arrow;
using Apache.Arrow.Types;
using Array = Apache.Arrow.Array;

namespace LanceDbClient;

public static class ArrayHelpers
{
    private static object getValue<T>(Array array)
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
        return val;
    }
    
    public static object ArrowArrayDataToConcrete(object array, int depth=0)
    {
        object val = null;
        switch (array)
        {
            case Int32Array intArray: val = getValue<int>(intArray); break;
            case FloatArray floatArray: val = getValue<float>(floatArray); break;
            case DoubleArray doubleArray: val = getValue<double>(doubleArray); break;
            case StringArray stringArray:
                val = stringArray.GetString(0); // Returns as string
                break;
            case BooleanArray boolArray: val = getValue<bool>(boolArray); break;
            case Int8Array int8Array: val = getValue<sbyte>(int8Array); break;
            case Int16Array int16Array: val = getValue<short>(int16Array); break;
            case Int64Array int64Array: val = getValue<long>(int64Array); break;
            case UInt8Array uint8Array: val = getValue<byte>(uint8Array); break;
            case UInt16Array uint16Array: val = getValue<ushort>(uint16Array); break;
            case UInt32Array uint32Array: val = getValue<uint>(uint32Array); break;
            case UInt64Array uint64Array: val = getValue<ulong>(uint64Array); break;
            case Date32Array date32Array: val = getValue<int>(date32Array); break;
            case Date64Array date64Array: val = getValue<long>(date64Array); break;
            case TimestampArray timestampArray: val = getValue<long>(timestampArray); break;
            case Time32Array time32Array: val = getValue<int>(time32Array); break;
            case Time64Array time64Array: val = getValue<long>(time64Array); break;
            case Decimal128Array decimal128Array: val = getValue<decimal>(decimal128Array); break;
            case Decimal256Array decimal256Array: val = getValue<decimal>(decimal256Array); break;
            case ListArray listArray:
                if (depth > 1) throw new NotSupportedException("List depth > 1 is not supported.");
                val = ArrowArrayDataToConcrete(listArray.Values, depth + 1); // Returns as Apache.Arrow.Array
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
                val = ArrowArrayDataToConcrete(fixedSizeListArray.Values, depth+1); // Returns as Apache.Arrow.Array
                break;
            default:
                throw new NotSupportedException($"Array type {array.GetType()} is not supported.");
        }

        return val;
    }

    private static bool DoesTypeMatchSchema(object o, ArrowTypeId s)
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
            buffers: new[] { ArrowBuffer.Empty }, // No null bitmap buffer, assuming all are valid
            children: new[] { data });
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

    private static object ArrayBuilderFactory(ArrowTypeId t)
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
    
    public static List<RecordBatch> ConcreteToArrowTable(IEnumerable<Dictionary<string, object>> data, Schema schema)
    {
        var result = new List<RecordBatch>();

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
                // TODO: Not working with fixed sized lists
                if (type.TypeId == ArrowTypeId.FixedSizeList)
                {
                    // Get the inner type
                    var innerType = ((FixedSizeListType)type).ValueDataType;
                    if (!DoesTypeMatchSchema(item.Value, innerType.TypeId)) throw new Exception("Type mismatch for " + item.Key + ". Expected " + innerType.TypeId + ", got " + item.Value.GetType() + ".");
                } else if (!DoesTypeMatchSchema(item.Value, type.TypeId)) throw new Exception("Type mismatch for " + item.Key + ". Expected " + type.TypeId + ", got " + item.Value.GetType() + ".");

                // Get the array builder
                // TODO: Many more builders required
                var baseType = item.Value.GetType();
                var subType = baseType.GetGenericArguments()[0];
                object? builder = null;
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
                
                MethodInfo? appendMethod = null;
                appendMethod = builder.GetType().GetMethod("Append", subType == typeof(string) ? 
                    new[] { typeof(string), typeof(Encoding) } : new[] { subType });
                if (appendMethod == null) throw new NotImplementedException("Append method for " + subType + " not implemented.");
                
                foreach (var value in item.Value as IList)
                {
                    if (subType == typeof(string))
                    {
                        appendMethod.Invoke(builder, new object[] { value, null });
                    }
                    else
                    {
                        appendMethod.Invoke(builder, new object[] { value });
                    }
                }
                var array = (IArrowArray)builder.GetType().GetMethod("Build").Invoke(builder, new object[] { null });
                AddToArrayOrWrapInList(arrayRows, type, array);
            }
            var recordBatch = new RecordBatch(schema, arrayRows.ToArray(), length: 1);
            result.Add(recordBatch);
        }
        
        return result;
    }
}