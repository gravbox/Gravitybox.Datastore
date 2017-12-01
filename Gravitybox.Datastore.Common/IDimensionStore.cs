using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Gravitybox.Datastore.Common
{
    /// <summary />
    public interface IDimensionStore
    {
        /// <summary />
        IEnumerable<DimensionItem> MasterList { get; }
        
    }
}