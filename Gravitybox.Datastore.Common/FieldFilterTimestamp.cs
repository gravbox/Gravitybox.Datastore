using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Gravitybox.Datastore.Common
{
    /// <summary />
    [Serializable]
    public sealed class FieldFilterTimestamp : Gravitybox.Datastore.Common.FieldFilter, Gravitybox.Datastore.Common.IFieldFilter
    {
        /// <summary />
        public static string FilterName { get { return "__Timestamp"; } }
        /// <summary />
        public FieldFilterTimestamp() : base(FilterName) { }
        /// <summary />
        public new int Value { get; set; }
        object Gravitybox.Datastore.Common.IFieldFilter.Value
        {
            get { return this.Value; }
            set { this.Value = (int)value; }
        }
        /// <summary />
        public override RepositorySchema.DataTypeConstants DataType { get { return RepositorySchema.DataTypeConstants.Int; } set { ; } }
    }

}
