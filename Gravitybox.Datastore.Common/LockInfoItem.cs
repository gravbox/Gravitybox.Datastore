using System;
using System.Linq;
using System.Runtime.Serialization;
using System.Xml.Serialization;

namespace Gravitybox.Datastore.Common
{
    /// <summary />
    public class LockInfoItem
    {
        /// <summary />
        public LockInfoItem()
        {
            this.DateStamp = DateTime.Now;
        }

        /// <summary />
        public int ThreadId { get; set; }
        /// <summary />
        public bool Failure { get; set; }
        /// <summary />
        public int Elapsed { get; set; }
        /// <summary />
        public int CurrentReadCount { get; set; }
        /// <summary />
        public int WaitingReadCount { get; set; }
        /// <summary />
        public int WaitingWriteCount { get; set; }
        /// <summary />
        public bool IsWriteLockHeld { get; set; }
        /// <summary />
        public DateTime DateStamp { get; private set;}
        /// <summary />
        public string TraceInfo { get; set; }
    }

}
