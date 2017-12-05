using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Gravitybox.Datastore.Common.Exceptions
{
    /// <summary />
    [Serializable]
    public class NotMasterInstanceException : System.Exception
    {
        /// <summary />
        public NotMasterInstanceException() : base("The service instance is not the master.") { }

    }

}
