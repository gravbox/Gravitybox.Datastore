using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Gravitybox.Datastore.Server.Core.QueryBuilders
{
    internal interface IQueryBuilder
    {
        Task GenerateSql();
        Task Execute();
        Task Load();
    }
}
