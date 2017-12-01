using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Runtime.Remoting.Channels;
using System.ServiceModel;
using System.Runtime.Serialization;

namespace Gravitybox.Datastore.Common
{
    /// <summary />
    [ServiceContract]
    [ServiceKnownType(typeof (BaseRemotingObject))]
    public interface IRemotingObject
    {
        /// <summary />
        [DataMember]
        RepositorySchema Repository { get; set; }

        /// <summary />
        [DataMember]
        long ItemCount { get; set; }

        /// <summary />
        [DataMember]
        long VersionHash { get; set; }

        /// <summary />
        [DataMember]
        bool IsLoaded { get; set; }

    }
}