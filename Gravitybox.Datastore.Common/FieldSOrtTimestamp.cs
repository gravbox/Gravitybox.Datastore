using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Gravitybox.Datastore.Common
{
    /// <summary />
    [Serializable]
    public sealed class FieldSortTimestamp : Gravitybox.Datastore.Common.FieldSort, Gravitybox.Datastore.Common.IFieldSort
    {
        /// <summary />
        public override Gravitybox.Datastore.Common.SortDirectionConstants SortDirection { get; set; }

        /// <summary />
        public override string Name
        {
            get { return "__Timestamp"; }
            set { ; }
        }

        /// <summary />
        public override string TokenName
        {
            get { return Utilities.DbTokenize(this.Name); }
        }
    }

}