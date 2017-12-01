using System;
using System.Runtime.Serialization;
using System.Xml.Serialization;

namespace Gravitybox.Datastore.Common
{
    /// <summary />
    [Serializable]
    public class SystemStats
    {
        /// <summary />
        [XmlElement]
        [DataMember]
        public long TotalMemory { get; set; }

        /// <summary />
        [XmlElement]
        [DataMember]
        public string MachineName { get; set; }

        /// <summary />
        [XmlElement]
        [DataMember]
        public string OSVersion { get; set; }

        /// <summary />
        [XmlElement]
        [DataMember]
        public int ProcessorCount { get; set; }

        /// <summary />
        [XmlElement]
        [DataMember]
        public int RepositoryCount { get; set; }


        /// <summary />
        [XmlElement]
        [DataMember]
        public int TickCount { get; set; }

    }
}
