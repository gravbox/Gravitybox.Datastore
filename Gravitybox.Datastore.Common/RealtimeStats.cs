using System;
using System.Linq;
using System.Runtime.Serialization;
using System.Xml.Serialization;

namespace Gravitybox.Datastore.Common
{
    /// <summary />
    [Serializable]
    public class RealtimeStats
    {
        /// <summary />
        [XmlElement]
        [DataMember]
        public DateTime Timestamp { get; set; }

        /// <summary />
        [XmlElement]
        [DataMember]
        public long MemoryUsageTotal { get; set; }

        /// <summary />
        [XmlElement]
        [DataMember]
        public long MemoryUsageAvailable { get; set; }

        /// <summary />
        [XmlElement]
        [DataMember]
        public long MemoryUsageProcess { get; set; }

        /// <summary />
        [XmlElement]
        [DataMember]
        public int RepositoryLoadDelta { get; set; }

        /// <summary />
        [XmlElement]
        [DataMember]
        public int RepositoryUnloadDelta { get; set; }

        /// <summary />
        [XmlElement]
        [DataMember]
        public int RepositoryTotal { get; set; }

        /// <summary />
        [XmlElement]
        [DataMember]
        public int RepositoryCreateDelta { get; set; }

        /// <summary />
        [XmlElement]
        [DataMember]
        public int RepositoryDeleteDelta { get; set; }

        /// <summary />
        [XmlElement]
        [DataMember]
        public int ProcessorUsage { get; set; }
    }
}
