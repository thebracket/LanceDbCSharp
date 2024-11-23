using LanceDbInterface;

namespace LanceDbClient;

public class RrfReranker : BaseReranker
{
    private readonly int _k;
    private readonly string _score;
    
    public RrfReranker(int k = 60, string score = "relevance")
    {
        this._k = k;
        this._score = score;
    }
    
    public new Apache.Arrow.Table RerankHybrid(string query, Apache.Arrow.Table vectorResults, Apache.Arrow.Table ftsResults)
    {
        var vectorIds = ArrayHelpers.ArrowTableStringColumnToList(vectorResults, "_rowid") ?? [];
        var ftsIds = ArrayHelpers.ArrowTableStringColumnToList(ftsResults, "_rowid") ?? [];

        // Initialize RRF score map with a default float value
        var rrfScoreMap = new Dictionary<string, float>();
        
        // Calculate RRF score for each result
        foreach (var ids in new List<List<string>> { vectorIds, ftsIds })
        {
            for (var i = 0; i < ids.Count; i++)
            {
                var resultId = ids[i];
                rrfScoreMap.TryAdd(resultId, 0f);
                rrfScoreMap[resultId] += 1.0f / (i + 1 + _k); // K is assumed to be inherited from BaseReranker
            }
        }

        // Merge the vector and FTS results
        var combinedResults = MergeResults(vectorResults, ftsResults);

        // Extract combined row IDs
        var combinedRowIds = ArrayHelpers.ArrowTableStringColumnToList(combinedResults, "_rowid");
        if (combinedRowIds == null)
        {
            throw new Exception("Combined results do not contain a '_rowid' column.");
        }

        // Create relevance scores based on the combined row IDs
        var relevanceScores = combinedRowIds.Select(rowId => rrfScoreMap.GetValueOrDefault(rowId, 0.0f)).ToList();

        // Append the relevance scores as a new column in the combined results
        combinedResults = ArrayHelpers.AppendFloatColumn(combinedResults, "_relevance_score", relevanceScores, Apache.Arrow.Types.FloatType.Default);

        // Sort the combined results by relevance score in descending order
        combinedResults = ArrayHelpers.SortBy(combinedResults, "_relevance_score", descending: true);

        // Optionally, keep only the relevance score if specified by the score type
        if (_score == "relevance")
        {
            combinedResults = KeepRelevanceScore(combinedResults);
        }

        return combinedResults;
    }
    
    // Method that retains only the relevance score if needed
    private static Apache.Arrow.Table KeepRelevanceScore(Apache.Arrow.Table combinedResults)
    {
        var columnsToDrop = new List<string>();

        if (ArrayHelpers.TableContainsColumn(combinedResults, "_score"))
        {
            columnsToDrop.Add("_score");
        }

        if (ArrayHelpers.TableContainsColumn(combinedResults,"_distance"))
        {
            columnsToDrop.Add("_distance");
        }

        if (columnsToDrop.Count > 0)
        {
            combinedResults = ArrayHelpers.DropColumns(combinedResults, columnsToDrop);
        }

        return combinedResults;
    }
}