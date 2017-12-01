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
    public class SummarySlice
    {
        /// <summary />
        [DataMember]
        public string[] GroupFields;

        /// <summary />
        [DataMember]
        public long SpliceDIdx;

        /// <summary />
        [DataMember]
        public string SpliceDName;

        /// <summary />
        [DataMember]
        public DataQuery Query = new DataQuery();

        /// <summary />
        public SummarySlice ConvertToTrasfer()
        {
            if (this.Query != null && this.Query.FieldFilters != null)
            {
                this.Query.FieldFilters = this.Query.FieldFilters.Select(x => ((IFieldFilter)((ICloneable)x).Clone())).ToList();
            }

            if (this.Query != null && this.Query.FieldSorts != null)
            {
                this.Query.FieldSorts = this.Query.FieldSorts.Select(x => ((IFieldSort)((ICloneable)x).Clone())).ToList();
            }
            return this;
        }

        /// <summary />
        public override int GetHashCode()
        {
            var hash = string.Empty;

            //IF there is a name then use it and ignore SpliceDIdx
            if (!string.IsNullOrEmpty(this.SpliceDName))
            {
                hash += this.SpliceDName + "|";
            }
            else
            {
                hash += this.SpliceDIdx + "|";
            }

            if (this.GroupFields == null || this.GroupFields.Length == 0)
                hash += "NULL|";
            else
                hash += string.Join("-", this.GroupFields.ToArray()) + "|";

            if (this.Query != null)
                hash += this.Query.GetHashCode() + "|";

            return EncryptionDomain.Hash(hash);
        }

    }
}
