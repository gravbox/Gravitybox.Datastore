using System;
using System.Runtime.Serialization;

namespace Gravitybox.Datastore.Common
{
    /// <summary />
    public enum AggregateOperationConstants
    {
        /// <summary />
        Sum = 0,
        /// <summary />
        Min = 1,
        /// <summary />
        Max = 2,
        /// <summary />
        Count = 3,
        /// <summary />
        Distinct = 4,
    }

    /// <summary />
    [DataContract]
    [Serializable]
    public class DerivedField : IDerivedField
    {
        /// <summary />
        [DataMember]
        public string Field { get; set; }

        /// <summary />
        [DataMember]
        public string Alias { get; set; }

        /// <summary />
        [DataMember]
        public AggregateOperationConstants Action { get; set; }

        /// <summary />
        public override string ToString()
        {
            return this.Field + ", " + this.Action;
        }

        /// <summary />
        public string TokenName
        {
            get
            {
                if (!string.IsNullOrEmpty(this.Alias))
                    return Utilities.DbTokenize(this.Alias);
                else
                    return Utilities.DbTokenize(this.Field);
            }
        }

    }

}
