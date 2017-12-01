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
    public class SummarySliceValue
    {
        /// <summary />
        [DataMember]
        public SummarySlice Definition;

        /// <summary />
        [DataMember]
        public List<SummarySliceRecord> RecordList;

        /// <summary />
        [DataMember]
        public int TotalRecordCount;

        /// <summary />
        [DataMember]
        public int DataVersion;

        /// <summary />
        [DataMember]
        public string[] ErrorList;
    }

    /// <summary />
    [Serializable]
    public class SummarySliceRecord
    {
        /// <summary />
        [DataMember]
        public List<string> FieldValues = new List<string>();

        /// <summary />
        [DataMember]
        public List<RefinementItem> SliceValues = new List<RefinementItem>();
    }

}