namespace LanceDbInterface;

public class IndexStatistics
{
    public int NumIndexedRows { get; set; }
    public int NumUnIndexedRows { get; set; }
    public IndexType IndexType { get; set; }    // this is string in python, but since it is statistics, enum should be fine.
    public Metric? DistanceType  { get; set; }
    public int? NumIndices { get; set; }
}