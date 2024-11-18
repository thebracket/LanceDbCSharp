namespace LanceDbInterface;

public class IndexConfig
{
    public required string Name { get; set; }   
    public required IndexType IndexType { get; set; }
    public required IEnumerable<string> Columns { get; set; }
}