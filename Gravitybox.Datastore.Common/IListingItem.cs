using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Gravitybox.Datastore.Common
{
    /// <summary />
    public interface IListingItem
    {
        /// <summary />
        IDimensionStore AllDimensions { get; }
        /// <summary />
        Dictionary<string, string> ExtraValues { get; set; }
        /// <summary />
        System.Type GetFieldEnum();
        /// <summary />
        Gravitybox.Datastore.Common.DataItem ToTransfer(RepositorySchema schema);
        /// <summary />
        IEnumerable<DimensionItem> DimensionList { get; }
        /// <summary />
        long __OrdinalPosition { get; }
    }
}