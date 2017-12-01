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
    public class GeoCode
    {
        /// <summary />
		[DataMember]
        public double Latitude;

        /// <summary />
		[DataMember]
        public double Longitude;

        /// <summary />
		[DataMember]
        public double? Distance;
    }
}