using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Gravitybox.Datastore.Common.Exceptions
{
    /// <summary />
    [Serializable]
    public class SchemaVersionException : System.Exception
    {
        /// <summary />
        public SchemaVersionException() : base() { }

    }

}
