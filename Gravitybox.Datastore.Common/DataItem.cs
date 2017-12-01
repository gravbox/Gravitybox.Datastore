using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Runtime.Serialization;

namespace Gravitybox.Datastore.Common
{
    /// <summary />
    [Serializable]
    [DataContract]
    [KnownType(typeof(GeoCode))]
    [KnownType(typeof(GeoCodeFieldFilter))]
    [KnownType(typeof(string[]))]
    [KnownType(typeof(FieldFilter))]
    [KnownType(typeof(FieldSort))]
    [KnownType(typeof(NamedItem))]
    public class DataItem
    {
        /// <summary />
        [DataMember]
        public long __RecordIndex;
        /// <summary />
        [DataMember]
        public long __OrdinalPosition;
        /// <summary />
        [DataMember]
        public int __Timestamp;
        /// <summary />
        [DataMember]
        public object[] ItemArray;
        /// <summary />
        public long __Hash;
    }
}
