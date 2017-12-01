using System;
using System.Runtime.Serialization;
namespace Gravitybox.Datastore.Common
{
    /// <summary />
    [DataContract]
    [Serializable]
    public class DerivedFieldValue : DerivedField, IDerivedFieldValue
    {
        /// <summary />
        [DataMember]
        public object Value { get; set; }
    }
}
