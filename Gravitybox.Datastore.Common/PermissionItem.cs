using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;

namespace Gravitybox.Datastore.Common
{
    /// <summary />
    [Serializable]
    [DataContract()]
    public class PermissionItem
    {
        /// <summary />
        [DataMember]
        public int UserId { get; set; }

        /// <summary />
        [DataMember]
        public string FieldValue { get; set; }

        /// <summary />
        [DataMember]
        public bool Reset { get; set; }
    }
}
