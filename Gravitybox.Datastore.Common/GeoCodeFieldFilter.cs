using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Runtime.Serialization;

namespace Gravitybox.Datastore.Common
{
    /// <summary />
    [DataContract]
    [Serializable]
    public class GeoCodeFieldFilter : FieldFilter, System.ICloneable
    {
        /// <summary />
        public GeoCodeFieldFilter()
        {
        }

        /// <summary />
        public GeoCodeFieldFilter(string name)
            : this()
        {
            this.Name = name;
        }

        /// <summary />
        [DataMember]
        public double Latitude;

        /// <summary />
        [DataMember]
        public double Longitude;

        /// <summary />
        [DataMember]
        public double Radius;

        /// <summary />
        public override int GetHashCode()
        {
            return EncryptionDomain.Hash(this.Comparer.ToString() + "·" + this.Latitude + "·" + this.Longitude + "·" + this.Radius);
        }

        object ICloneable.Clone()
        {
            var retval = new GeoCodeFieldFilter();
            retval.Comparer = this.Comparer;
            retval.Name = this.Name;
            ((Gravitybox.Datastore.Common.IFieldFilter)retval).Value = ((Gravitybox.Datastore.Common.IFieldFilter)this).Value;
            retval.Latitude = this.Latitude;
            retval.Longitude = this.Longitude;
            retval.Radius = this.Radius;
            return retval;
        }

        /// <summary />
        public override string ToString()
        {
            return this.Name + "," + this.Comparer + "," + this.Latitude + "," + this.Longitude + "," + this.Radius;
        }

    }

}