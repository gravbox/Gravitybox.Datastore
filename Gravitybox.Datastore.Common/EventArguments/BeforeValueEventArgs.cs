using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Gravitybox.Datastore.Common.EventArguments
{
    /// <summary />
    public partial class BeforeValueEventArgs<T> : System.EventArgs
    {
        /// <summary />
        public T Value { get; set; }
    }
}
