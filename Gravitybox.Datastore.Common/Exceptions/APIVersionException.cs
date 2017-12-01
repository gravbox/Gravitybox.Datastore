using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Gravitybox.Datastore.Common.Exceptions
{
    /// <summary />
    [Serializable]
    public class APIVersionException : System.Exception
    {
        /// <summary />
        public APIVersionException() : base() { }

        /// <summary />
        public APIVersionException(long hash, long queryHash)
            : base()
        {
            this.Hash = hash;
            this.QueryHash = queryHash;
        }

        /// <summary />
        public long Hash { get; private set; }

        /// <summary />
        public long QueryHash { get; private set; }

        /// <summary />
        public override string Message
        {
            get { return "The generated API is out of date. The repository model has been changed. Please re-generate this code file. Computed: " + this.Hash + ", Returned: " + this.QueryHash; }
        }
    }

}
