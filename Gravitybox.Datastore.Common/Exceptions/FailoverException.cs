using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Gravitybox.Datastore.Common.Exceptions
{
    /// <summary />
    [Serializable]
    public class FailoverException : System.Exception
    {
        /// <summary />
        public FailoverException() : base("The system has triggered a fail over.") { }

    }

}
