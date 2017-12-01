using System;
using System.Collections.Generic;
using System.ServiceModel;

namespace Gravitybox.Datastore.Common
{
    /// <summary />
    public interface IDerivedField
    {
        /// <summary />
        string Field { get; set; }

        /// <summary />
        string Alias { get; set; }

        /// <summary />
        AggregateOperationConstants Action { get; set; }
    }

    /// <summary />
    public interface IDerivedFieldValue : IDerivedField
    {
        /// <summary />
        object Value { get; set; }
    }

}