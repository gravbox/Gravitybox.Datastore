using System;
using System.Runtime.Serialization;

namespace Gravitybox.Datastore.Common
{
    /// <summary />
    [Serializable]
    [DataContract()]
    public class SystemCredentials : UserCredentials
    {
        /// <summary />
        public SystemCredentials()
        {
            this.UserId = Guid.NewGuid();
        }

        /// <summary />
        [DataMember]
        public Guid UserId { get; set; }
    }
}