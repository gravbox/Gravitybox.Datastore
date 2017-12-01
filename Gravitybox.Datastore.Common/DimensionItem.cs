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
    public class DimensionItem : IDimensionItem, System.ICloneable, Gravitybox.Datastore.Common.ICloneable<DimensionItem>
    {
        /// <summary />
        public const string DateTimeFormat = "yyyy-MM-dd HH:mm:ss";

        /// <summary />
        [DataMember]
        public string Name;

        /// <summary />
        [DataMember]
        public long DIdx;

        /// <summary />
        [DataMember]
        public long? NumericBreak;

        /// <summary />
        [DataMember]
        public List<IRefinementItem> RefinementList = new List<IRefinementItem>();

        /// <summary />
        [DataMember]
        public IDimensionItem Parent;

        /// <summary>
        /// Determines if this column can not be used to sort
        /// </summary>
        [DataMember]
        public bool Sortable;

        /// <summary />
        public override string ToString()
        {
            return this.Name + "/" + this.DIdx;
        }

        object ICloneable.Clone()
        {
            return ((ICloneable<DimensionItem>)this).Clone(new DimensionItem());
        }

        #region ICloneable<DimensionItem> Members

        DimensionItem ICloneable<DimensionItem>.Clone()
        {
            return ((ICloneable<DimensionItem>)this).Clone(new DimensionItem());
        }

        DimensionItem ICloneable<DimensionItem>.Clone(DimensionItem dest)
        {
            if (dest == null)
                throw new Exception("Object cannot be null.");

            dest.DIdx = this.DIdx;
            dest.Name = this.Name;
            dest.NumericBreak = this.NumericBreak;
            dest.Sortable = this.Sortable;
            this.RefinementList.ForEach(x => dest.RefinementList.Add(((ICloneable)x).Clone() as RefinementItem));
            return dest;
        }

        #endregion

        #region IDimensionItem Members

        string IDimensionItem.Name
        {
            get { return this.Name; }
            set { this.Name = value; }
        }

        long IDimensionItem.DIdx
        {
            get { return this.DIdx; }
            set { this.DIdx = value; }
        }

        long? IDimensionItem.NumericBreak
        {
            get { return this.NumericBreak; }
            set { this.NumericBreak = value; }
        }

        List<IRefinementItem> IDimensionItem.RefinementList
        {
            get { return this.RefinementList; }
            set { this.RefinementList = value; }
        }

        IDimensionItem IDimensionItem.Parent
        {
            get { return this.Parent; }
            set { this.Parent = value; }
        }

        #endregion

    }
}