namespace LanceDbInterface
{
    public enum IndexType
    {
        BTree = (int)ScalarIndexType.BTree,
        Bitmap = (int)ScalarIndexType.Bitmap,
        LabelList = (int)ScalarIndexType.LabelList,
        Fts = 3,
        HnswPq = 4,
        HnswSq = 5,
        IvfPq = 6
    }
}
