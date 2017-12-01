using Gravitybox.Datastore.Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;

namespace Gravitybox.Datastore.Server.Core.QueryBuilders
{
    public class DataQueryGroupedResults
    {
        /// <summary>
        /// The query parameters used to generate this result set
        /// </summary>
        [DataMember]
        public DataQuery Query;

        /// <summary />
        [DataMember]
        public DateTime QueryTime { get; set; }

        /// <summary />
        [DataMember]
        public long VersionHash { get; set; }

    }
}
