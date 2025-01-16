namespace LanceDbClient
{
    public enum IndexType
    {
        BTree = 0,
        Bitmap = 1,
        LabelList = 2,
        Fts = 3,
        HnswPq = 4,
        HnswSq = 5,
        IvfPq = 6
    }
}
