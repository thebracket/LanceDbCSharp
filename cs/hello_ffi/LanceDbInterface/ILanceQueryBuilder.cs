using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LanceDbInterface
{
    public interface ILanceQueryBuilder
    {
        ILanceQueryBuilder Limit(int limit);
        ILanceQueryBuilder SelectColumns(IEnumerable<string> selectColumns);
        ILanceQueryBuilder WhereClause(string whereClause, bool prefilter = false);
        ILanceQueryBuilder WithRowId(bool withRowId);
        string ExplainPlan(bool verbose = false);

        Apache.Arrow.Table ToArrow();
        Task<Apache.Arrow.Table> ToArrowAsync(CancellationToken token = default);
        IEnumerable<IDictionary<string, object>> ToList();
        Task<IEnumerable<IDictionary<string, object>>> ToListAsync(CancellationToken token = default);
    }
}
