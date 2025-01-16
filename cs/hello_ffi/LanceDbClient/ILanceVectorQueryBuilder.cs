namespace LanceDbClient
{
    public interface ILanceVectorQueryBuilder : ILanceQueryBuilder
    {
        ILanceVectorQueryBuilder Metric(Metric metric = LanceDbClient.Metric.L2);
        ILanceVectorQueryBuilder NProbes(int nProbes);
        ILanceVectorQueryBuilder RefineFactor(int refineFactor);
    }
}
