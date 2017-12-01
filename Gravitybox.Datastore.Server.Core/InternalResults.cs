using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Gravitybox.Datastore.Server.Core
{
    internal class SqlResults
    {
        public int AffectedCount { get; set; }
    }

    internal class UpdateDataSqlResults : SqlResults
    {
        public int FountCount { get; set; }
    }
}
