using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Gravitybox.Datastore.Common
{
    /// <summary />
    public interface IRefinementItem: System.ICloneable
    {
        /// <summary />
        string FieldValue { get; set; }
        /// <summary />
        long DIdx { get; set; }
        /// <summary />
        long DVIdx { get; set; }
        /// <summary />
        int Count { get; set; }
        /// <summary />
        long? MinValue { get; set; }
        /// <summary />
        long? MaxValue { get; set; }

    }
}
