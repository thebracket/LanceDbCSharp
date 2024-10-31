namespace LanceDbInterface
{
    public interface ILanceHybridQueryBuilder : ILanceQueryBuilder
    {
        ILanceVectorQueryBuilder Metric(Metric metric = LanceDbInterface.Metric.L2);
        ILanceVectorQueryBuilder NProbes(int nProbes);
        ILanceVectorQueryBuilder RefineFactor(int refineFactor);
    }
}
