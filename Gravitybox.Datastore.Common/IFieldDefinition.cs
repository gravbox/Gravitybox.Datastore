using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Runtime.Serialization;

namespace Gravitybox.Datastore.Common
{
    /// <summary />
    public interface IFieldDefinition
    {
        /// <summary />
        [DataMember]
        string Name { get; set; }

        /// <summary />
        [DataMember]
        RepositorySchema.FieldTypeConstants FieldType { get; set; }

        /// <summary />
        [DataMember]
        int Length { get; set; }

        /// <summary />
        [DataMember]
        RepositorySchema.DataTypeConstants DataType { get; set; }

        /// <summary />
        [DataMember]
        bool IsPrimaryKey { get; set; }

        /// <summary />
        [DataMember]
        bool AllowTextSearch { get; set; }

        /// <summary />
        [DataMember]
        bool SearchAsc { get; set; }
    }
}