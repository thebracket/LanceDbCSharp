using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LanceDbInterface
{
    public interface ILanceDb
    {
        IConnection Connect(Uri uri, TimeSpan? readConsistencyInterval = null);
        Task<IConnection> ConnectAsync(Uri uri, TimeSpan? readConsistencyInterval = null, CancellationToken token = default);
    }

}
