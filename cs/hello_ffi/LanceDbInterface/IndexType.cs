﻿namespace LanceDbInterface
{
    public enum IndexType
    {
        BTree = 1,
        Bitmap = 2,
        LabelList = 3,
        Fts = 4,
        HnswPq = 5,
        HnswSq = 6,
        IvfPq = 7
    }
}
