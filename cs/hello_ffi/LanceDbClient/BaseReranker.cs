using System.Net.Mail;
using Apache.Arrow;
using LanceDbInterface;

namespace LanceDbClient;

public abstract class BaseReranker : IReranker
{
    protected internal string Query = "";
    
    public Apache.Arrow.Table RerankVector(string query, Apache.Arrow.Table vectorResults)
    {
        throw new NotImplementedException();
    }

    public Apache.Arrow.Table RerankFts(string query, Apache.Arrow.Table ftsResults)
    {
        throw new NotImplementedException();
    }

    public Apache.Arrow.Table RerankHybrid(string query, Apache.Arrow.Table vectorResults, Apache.Arrow.Table ftsResults)
    {
        throw new NotImplementedException();
    }

    /// <summary>
    /// Merge the results from the vector and FTS search. This is a vanilla merging
    /// function that just concatenates the results and removes the duplicates.
    ///
    /// NOTE: This doesn't take score into account. It'll keep the instance that was
    /// encountered first. This is designed for rerankers that don't use the score.
    /// In case you want to use the score, or support `return_scores="all"` you'll
    /// have to implement your own merging function.
    /// </summary>
    /// <param name="vectorResults"></param>
    /// <param name="ftsResults"></param>
    /// <returns></returns>
    /// <exception cref="Exception"></exception>
    public Apache.Arrow.Table MergeResults(Apache.Arrow.Table vectorResults, Apache.Arrow.Table ftsResults)
    {
        // See base.py, in the rerankers module. Line 130.
        if (vectorResults.Schema != ftsResults.Schema)
        {
            throw new Exception("The schemas of the two tables must match");
        }
        
        // The original code uses a Concatenation type that isn't implemented in C#. It then calls a de-duplicator that isn't in C# either.
        var combined = ArrayHelpers.ConcatTables([vectorResults, ftsResults]);
        // TODO: De-duplicate by RowId

        return combined;
    }

    /// <summary>
    /// This is a rerank function that receives the results from multiple
    /// vector searches. For example, this can be used to combine the
    /// results of two vector searches with different embeddings.
    /// </summary>
    /// <param name="vectorResults"></param>
    /// <param name="query"></param>
    /// <param name="deduplicate"></param>
    /// <returns></returns>
    public Apache.Arrow.Table RerankMultiVector(IEnumerable<Apache.Arrow.Table> vectorResults, string? query = null, bool deduplicate = false)
    {
        var merged = ArrayHelpers.ConcatTables(vectorResults.ToList());
        var reranked = this.RerankVector(query, merged);
        // TODO: Deduplication
        return reranked;
    }

    /// <summary>
    /// This is a rerank function that receives the results from multiple
    /// vector searches. For example, this can be used to combine the
    /// results of two vector searches with different embeddings.
    /// </summary>
    /// <param name="vectorResults"></param>
    /// <param name="query"></param>
    /// <param name="deduplicate"></param>
    /// <returns></returns>
    public Apache.Arrow.Table RerankMultiVector(IEnumerable<ILanceVectorQueryBuilder> vectorResults, string? query = null, bool deduplicate = false)
    {
        var tables = vectorResults.Select(qb => qb.ToArrow()).ToList();
        return RerankMultiVector(tables, query, deduplicate);
    }
}