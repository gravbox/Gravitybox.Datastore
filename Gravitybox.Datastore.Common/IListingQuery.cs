using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Gravitybox.Datastore.Common
{
    /// <summary />
    public interface IListingQuery
    {
        /// <summary />
        List<long> DimensionValueList { get; set; }
        /// <summary />
        List<IFieldSort> FieldSorts { get; set; }
        /// <summary />
        List<IFieldFilter> FieldFilters { get; set; }
        /// <summary />
        string Keyword { get; set; }
        /// <summary />
        int PageOffset { get; set; }
        /// <summary />
        int RecordsPerPage { get; set; }
        /// <summary />
        NamedItemList NonParsedFieldList { get; set; }
        /// <summary />
        string PageName { get; set; }
        /// <summary />
        List<int> UserList { get; set; }
        /// <summary />
        bool IncludeRecords { get; set; }
    }
}