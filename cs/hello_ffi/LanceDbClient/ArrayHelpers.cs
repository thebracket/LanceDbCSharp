using Apache.Arrow;
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
}