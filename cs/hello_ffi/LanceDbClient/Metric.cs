namespace LanceDbClient
{
    public enum Metric
    {
        None = 0, // Included because it is possible for LanceDb to return None as an index metric
        L2 = 1,
        Cosine = 2,
        Dot = 3,
        Hamming = 4,
    }
}
