using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Runtime.Serialization;

namespace Gravitybox.Datastore.Common
{
    /// <summary />
    [Serializable()]
    [DataContract()]
    [KnownType(typeof(GeoCode))]
    [KnownType(typeof(GeoCodeFieldFilter))]
    public class RefinementItem : IRefinementItem, System.ICloneable, Gravitybox.Datastore.Common.ICloneable<RefinementItem>
    {
        /// <summary />
        [DataMember]
        public string FieldValue;

        /// <summary />
        [DataMember]
        public long DVIdx;

        /// <summary />
        [DataMember]
        public long DIdx;

        /// <summary />
        [DataMember]
        public int Count;

        /// <summary />
        [DataMember]
        public long? MinValue { get; set; }

        /// <summary />
        [DataMember]
        public long? MaxValue { get; set; }

        /// <summary />
        public override string ToString()
        {
            return this.FieldValue;
        }

        object ICloneable.Clone()
        {
            return ((ICloneable<RefinementItem>)this).Clone(new RefinementItem());
        }

        #region ICloneable<RefinementItem> Members

        RefinementItem ICloneable<RefinementItem>.Clone()
        {
            return ((ICloneable<RefinementItem>)this).Clone(new RefinementItem());
        }

        RefinementItem ICloneable<RefinementItem>.Clone(RefinementItem dest)
        {
            if (dest == null)
                throw new Exception("Object cannot be null.");

            dest.Count = this.Count;
            dest.DVIdx = this.DVIdx;
            dest.DIdx = this.DIdx;
            dest.FieldValue = this.FieldValue;
            return dest;
        }

        #endregion

        #region IRefinementItem Members

        string IRefinementItem.FieldValue
        {
            get { return this.FieldValue; }
            set { this.FieldValue = value; }
        }

        long IRefinementItem.DVIdx
        {
            get { return this.DVIdx; }
            set { this.DVIdx = value; }
        }

        long IRefinementItem.DIdx
        {
            get { return this.DIdx; }
            set { this.DIdx = value; }
        }

        int IRefinementItem.Count
        {
            get { return this.Count; }
            set { this.Count = value; }
        }

        #endregion

    }
}