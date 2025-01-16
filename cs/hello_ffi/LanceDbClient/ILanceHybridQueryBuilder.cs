namespace LanceDbClient
{
    public interface ILanceHybridQueryBuilder : ILanceQueryBuilder
    {
        ILanceHybridQueryBuilder Metric(Metric metric = LanceDbClient.Metric.L2);
        ILanceHybridQueryBuilder NProbes(int nProbes);
        ILanceHybridQueryBuilder RefineFactor(int refineFactor);
    }
}
