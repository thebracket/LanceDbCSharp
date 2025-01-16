namespace LanceDbClient;

public class CompactionMetrics
{
    public int FragmentsRemoved { get; set; } = 0;
    public int FragmentsAdded { get; set; } = 0;
    public int FilesRemoved { get; set; } = 0;
    public int FilesAdded { get; set; } = 0;
}