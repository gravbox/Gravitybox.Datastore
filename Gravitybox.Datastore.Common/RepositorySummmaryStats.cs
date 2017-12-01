using System;
using System.Linq;
using System.Runtime.Serialization;
using System.Xml.Serialization;

namespace Gravitybox.Datastore.Common
{
    /// <summary />
    public enum RepositoryActionConstants
    {
        /// <summary />
        Query = 1,
        //LoadData = 2,
        /// <summary />
        SaveData = 3,
        //UnloadData = 4,
        /// <summary />
        Reset = 5,
        /// <summary />
        ExportSchema = 6,
        //Backup = 7,
        //Restore = 8,
        //Compress = 9,
        /// <summary />
        Shutdown = 10,
        /// <summary />
        DeleteData = 11,
    }

    /// <summary />
    [Serializable]
    public class RepositorySummmaryStats
    {
        /// <summary />
        public override string ToString()
        {
            return this.RepositoryId + " | " + this.ActionType.ToString() + " | " + this.Elapsed + " | " + this.ItemCount;
        }

        /// <summary />
        [XmlElement]
        [DataMember]
        public Guid RepositoryId { get; set; }

        /// <summary />
        [XmlElement]
        [DataMember]
        public RepositoryActionConstants ActionType { get; set; }

        /// <summary />
        [XmlElement]
        [DataMember]
        public int Elapsed { get; set; }

        /// <summary />
        [XmlElement]
        [DataMember]
        public int ItemCount { get; set; }

        /// <summary />
        [XmlElement]
        [DataMember]
        public int LockTime { get; set; }

        /// <summary />
        [XmlElement]
        [DataMember]
        public int WaitingLocksOnEntry { get; set; }

        /// <summary />
        [XmlElement]
        [DataMember]
        public int ReadLockCount { get; set; }

    }

}
