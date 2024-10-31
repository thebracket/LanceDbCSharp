namespace LanceDbInterface
{
    public interface ILanceVectorQueryBuilder : ILanceQueryBuilder
    {
        ILanceVectorQueryBuilder Metric(Metric metric = LanceDbInterface.Metric.L2);
        ILanceVectorQueryBuilder NProbes(int nProbes);
        ILanceVectorQueryBuilder RefineFactor(int refineFactor);
    }
}
