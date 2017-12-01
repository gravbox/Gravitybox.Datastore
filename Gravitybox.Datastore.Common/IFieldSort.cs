using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Gravitybox.Datastore.Common
{
    /// <summary />
    public enum SortDirectionConstants
    {
        /// <summary />
        Asc,
        /// <summary />
        Desc,
    }

    /// <summary />
    public interface IFieldSort
    {
        /// <summary />
        Gravitybox.Datastore.Common.SortDirectionConstants SortDirection { get; set; }
        /// <summary />
        string Name { get; set; }
        /// <summary />
        string TokenName { get;  }
    }
}