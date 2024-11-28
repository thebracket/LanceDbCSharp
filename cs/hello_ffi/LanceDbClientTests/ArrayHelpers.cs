using Apache.Arrow;
using Apache.Arrow.Types;
using LanceDbClient;
using Array = Apache.Arrow.Array;

namespace LanceDbClientTests;

public partial class Tests
{
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