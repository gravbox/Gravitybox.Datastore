using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Gravitybox.Datastore.Common.Queryable
{
    /// <summary />
    public class FieldGrouping<TKey, TElement> : List<TElement>, IGrouping<TKey, TElement>
    {
        /// <summary />
        public TKey Key
        {
            get;
            set;
        }
    }
}
