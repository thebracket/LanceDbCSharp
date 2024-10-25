namespace LanceDbClient;

//TODO: Implement IDiposable and the matching FFI call
public class Table
{
    internal Table(string name, long handle)
    {
        _name = name;
        _handle = handle;
    }
    
    private string _name;
    private long _handle;
}