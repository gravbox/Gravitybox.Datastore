using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Gravitybox.Datastore.Common
{
    /// <summary />
    public interface IListingResults
    {
        /// <summary />
        List<Gravitybox.Datastore.Common.DimensionItem> DimensionList { get; }
        /// <summary />
        IListingQuery Query { get; }
        /// <summary />
        List<IListingItem> RecordList { get; }
        /// <summary />
        int TotalRecordCount { get; }
        /// <summary />
        int TotalPageCount { get; }
        /// <summary />
        List<IDerivedFieldValue> DerivedFieldList { get; }
        /// <summary />
        System.Type FieldType { get; }
    }
}