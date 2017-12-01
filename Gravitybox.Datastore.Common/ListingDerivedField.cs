using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Runtime.Serialization;

namespace Gravitybox.Datastore.Common
{
    /// <summary />
    [Serializable]
    public partial class ListingDerivedField : IDerivedField
    {
        /// <summary />
        public System.Enum Field { get; set; }

        /// <summary />
        public string Alias { get; set; }

        /// <summary />
        public AggregateOperationConstants Action { get; set; }

        /// <summary />
        string IDerivedField.Field
        {
            get { return this.Field.ToString(); }
            set { this.Field = (System.Enum)Enum.Parse(typeof(System.Enum), value); }
        }
    }

    /// <summary />
    [Serializable]
    public partial class ListingDerivedFieldValue : ListingDerivedField, IDerivedFieldValue
    {
        /// <summary />
        public object Value { get; set; }
    }
}
