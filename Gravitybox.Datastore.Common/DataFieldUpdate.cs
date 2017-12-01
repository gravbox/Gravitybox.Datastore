using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;

namespace Gravitybox.Datastore.Common
{
    /// <summary />
    [Serializable]
    [DataContract]
    public class DataFieldUpdate
    {
        /// <summary />
        [DataMember]
        public string FieldName { get; set; }
        /// <summary />
        [DataMember]
        public object FieldValue { get; set; }

        /// <summary />
        public override string ToString()
        {
            var retval = this.FieldName + " / ";
            if (this.FieldValue == null)
                retval += "NULL";
            else if (this.FieldValue is string[])
                retval += string.Join(", ", ((string[])this.FieldValue));
            else
                retval += this.FieldValue.ToString();
            return retval;
        }

    }
}
