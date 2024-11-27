using Apache.Arrow;
using Apache.Arrow.Types;
using LanceDbClient;
using Array = Apache.Arrow.Array;

namespace LanceDbClientTests;

public partial class Tests
{
    [Test]
    public void ArrowArrayDataToConcrete_Int32Array_ReturnsExpectedList()
    {
        // Arrange
        var int32Array = new Int32Array.Builder().Append(1).Append(2).Append(3).Build();

        // Act
        var result = ArrayHelpers.ArrowArrayDataToConcrete(int32Array);

        // Assert
        Assert.IsInstanceOf<List<int>>(result);
        CollectionAssert.AreEqual(new List<int> { 1, 2, 3 }, (List<int>)result);
    }
    
    [Test]
    public void ArrowArrayDataToConcrete_Int8Array_ReturnsExpectedList()
    {
        // Arrange
        var int8Array = new Int8Array.Builder().Append(-128).Append(0).Append(127).Build();
            
        // Act
        var result = ArrayHelpers.ArrowArrayDataToConcrete(int8Array);
            
        // Assert
        Assert.IsInstanceOf<List<sbyte>>(result);
        CollectionAssert.AreEqual(new List<sbyte> { -128, 0, 127 }, (List<sbyte>)result);
    }

    [Test]
    public void ArrowArrayDataToConcrete_Int16Array_ReturnsExpectedList()
    {
        // Arrange
        var int16Array = new Int16Array.Builder().Append(-32768).Append(0).Append(32767).Build();
            
        // Act
        var result = ArrayHelpers.ArrowArrayDataToConcrete(int16Array);
            
        // Assert
        Assert.IsInstanceOf<List<short>>(result);
        CollectionAssert.AreEqual(new List<short> { -32768, 0, 32767 }, (List<short>)result);
    }

    [Test]
    public void ArrowArrayDataToConcrete_FloatArray_ReturnsExpectedList()
    {
        // Arrange
        var floatArray = new FloatArray.Builder().Append(1.1f).Append(2.2f).Append(3.3f).Build();

        // Act
        var result = ArrayHelpers.ArrowArrayDataToConcrete(floatArray);

        // Assert
        Assert.IsInstanceOf<List<float>>(result);
        CollectionAssert.AreEqual(new List<float> { 1.1f, 2.2f, 3.3f }, (List<float>)result);
    }
    
    [Test]
    public void ArrowArrayDataToConcrete_DoubleArray_ReturnsExpectedList()
    {
        // Arrange
        var doubleArray = new DoubleArray.Builder().Append(1.1).Append(2.2).Append(3.3).Build();
            
        // Act
        var result = ArrayHelpers.ArrowArrayDataToConcrete(doubleArray);
            
        // Assert
        Assert.IsInstanceOf<List<double>>(result);
        CollectionAssert.AreEqual(new List<double> { 1.1, 2.2, 3.3 }, (List<double>)result);
    }

    [Test]
    public void ArrowArrayDataToConcrete_StringArray_ReturnsExpectedString()
    {
        // Arrange
        var stringArray = new StringArray.Builder().Append("hello").Build();

        // Act
        var result = ArrayHelpers.ArrowArrayDataToConcrete(stringArray);

        // Assert
        Assert.IsInstanceOf<List<string>>(result);
        Assert.AreEqual("hello", ((List<string>)result)[0]);
    }
    
    [Test]
        public void ArrowArrayDataToConcrete_Int64Array_ReturnsExpectedList()
        {
            // Arrange
            var int64Array = new Int64Array.Builder().Append(-9223372036854775808).Append(0).Append(9223372036854775807).Build();
            
            // Act
            var result = ArrayHelpers.ArrowArrayDataToConcrete(int64Array);
            
            // Assert
            Assert.IsInstanceOf<List<long>>(result);
            CollectionAssert.AreEqual(new List<long> { -9223372036854775808, 0, 9223372036854775807 }, (List<long>)result);
        }

        [Test]
        public void ArrowArrayDataToConcrete_UInt8Array_ReturnsExpectedList()
        {
            // Arrange
            var uint8Array = new UInt8Array.Builder().Append(0).Append(128).Append(255).Build();
            
            // Act
            var result = ArrayHelpers.ArrowArrayDataToConcrete(uint8Array);
            
            // Assert
            Assert.IsInstanceOf<List<byte>>(result);
            CollectionAssert.AreEqual(new List<byte> { 0, 128, 255 }, (List<byte>)result);
        }

        [Test]
        public void ArrowArrayDataToConcrete_UInt16Array_ReturnsExpectedList()
        {
            // Arrange
            var uint16Array = new UInt16Array.Builder().Append(0).Append(32768).Append(65535).Build();
            
            // Act
            var result = ArrayHelpers.ArrowArrayDataToConcrete(uint16Array);
            
            // Assert
            Assert.IsInstanceOf<List<ushort>>(result);
            CollectionAssert.AreEqual(new List<ushort> { 0, 32768, 65535 }, (List<ushort>)result);
        }

        [Test]
        public void ArrowArrayDataToConcrete_UInt32Array_ReturnsExpectedList()
        {
            // Arrange
            var uint32Array = new UInt32Array.Builder().Append(0).Append(2147483648).Append(4294967295).Build();
            
            // Act
            var result = ArrayHelpers.ArrowArrayDataToConcrete(uint32Array);
            
            // Assert
            Assert.IsInstanceOf<List<uint>>(result);
            CollectionAssert.AreEqual(new List<uint> { 0, 2147483648, 4294967295 }, (List<uint>)result);
        }

        [Test]
        public void ArrowArrayDataToConcrete_UInt64Array_ReturnsExpectedList()
        {
            // Arrange
            var uint64Array = new UInt64Array.Builder().Append(0).Append(9223372036854775808).Append(18446744073709551615).Build();
            
            // Act
            var result = ArrayHelpers.ArrowArrayDataToConcrete(uint64Array);
            
            // Assert
            Assert.IsInstanceOf<List<ulong>>(result);
            CollectionAssert.AreEqual(new List<ulong> { 0, 9223372036854775808, 18446744073709551615 }, (List<ulong>)result);
        }
        
       
        [Test]
        public void ArrowArrayDataToConcrete_BinaryArray_ReturnsExpectedListOfByteArrays()
        {
            // Arrange
            var binaryArray = new BinaryArray.Builder().Append([ 1, 2, 3 ]).Append([ 4, 5, 6 ]).Build();
            
            // Act
            var result = ArrayHelpers.ArrowArrayDataToConcrete(binaryArray);
            
            // Assert
            Assert.IsInstanceOf<List<byte[]>>(result);
            var expected = new List<byte[]> { new byte[] { 1, 2, 3 }, new byte[] { 4, 5, 6 } };
            CollectionAssert.AreEqual(expected, (List<byte[]>)result);
        }
}