using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Gravitybox.Datastore.Common
{
    /// <summary />
    public interface ICloneable<T>
    {
        /// <summary />
        T Clone();

        /// <summary />
        T Clone(T dest);
    }
}