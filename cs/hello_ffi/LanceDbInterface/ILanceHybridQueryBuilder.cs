namespace LanceDbInterface
{
    public interface ILanceHybridQueryBuilder : ILanceQueryBuilder
    {
        ILanceHybridQueryBuilder Metric(Metric metric = LanceDbInterface.Metric.L2);
        ILanceHybridQueryBuilder NProbes(int nProbes);
        ILanceHybridQueryBuilder RefineFactor(int refineFactor);
    }
}
