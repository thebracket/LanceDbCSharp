﻿namespace LanceDbClient;

public class OptimizeStats
{
    public CompactionMetrics? Compaction { get; set; }
    public RemovalStats? Prune { get; set; }
}