﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Apache.Arrow;

namespace LanceDbInterface
{
    public interface IConnection : IDisposable
    {
        IEnumerable<string> TableNames();
        Task<IEnumerable<string>> TableNamesAsync(CancellationToken cancellationToken = default);

        // The Schema is Apache.Arrow.Schema, if it doesn't work with the FFI layer, then we have to create these classes ourselve.
        ITable CreateTable(string name, Schema schema);
        Task<ITable> CreateTableAsync(string name, Schema schema, CancellationToken cancellationToken = default);

        ITable OpenTable(string name);
        Task<ITable> OpenTableAsync(string name, CancellationToken cancellationToken = default);

        void DropTable(string name, bool ignoreMissing = false);
        Task DropTableAsync(string name, bool ignoreMissing = false, CancellationToken cancellationToken = default);

        void DropDatabase();
        Task DropDatabaseAsync(CancellationToken cancellationToken = default);

        void Close();
        Task CloseAsync(CancellationToken cancellationToken = default);

        bool IsOpen { get;  }

        Uri Uri { get; }
    }
}
