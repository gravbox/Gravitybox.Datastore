using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Gravitybox.Datastore.Common
{
    /// <summary />
    public interface IDimensionItem
    {
        /// <summary />
        string Name { get; set; }

        /// <summary />
        long DIdx { get; set; }

        /// <summary />
        long? NumericBreak { get; set; }

        /// <summary />
        List<IRefinementItem> RefinementList { get; set; }

        /// <summary />
        IDimensionItem Parent { get; set; }

    }
}