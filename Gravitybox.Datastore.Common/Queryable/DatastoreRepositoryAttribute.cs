using System;
using RepositorySchema = Gravitybox.Datastore.Common.RepositorySchema;

namespace Gravitybox.Datastore.Common.Queryable
{
    /// <summary />
    public class DatastoreRepositoryAttribute : Attribute
    {
        /// <summary />
        public string Name { get; set; }
        
        /// <summary />
        public string ObjectAlias { get; set; }
        
        /// <summary />
        public string Id { get; set; }

        /// <summary />
        public string ParentId { get; set; }

        /// <summary />
        public RepositorySchema.FieldIndexingConstants FieldIndexing { get; set; }
    }

    /// <summary />
    public class DatastoreFieldAttribute : Attribute
    {
        /// <summary />
        public string Name { get; set; }
        /// <summary />
        public string Description { get; set; }
        /// <summary />
        public RepositorySchema.DataTypeConstants DataType { get; set; } = RepositorySchema.DataTypeConstants.String;
        /// <summary />
        public RepositorySchema.FieldTypeConstants FieldType { get; set; } = RepositorySchema.FieldTypeConstants.Field;
        /// <summary />
        public bool IsPrimaryKey { get; set; } = false;
        /// <summary />
        public bool AllowTextSearch { get; set; } = false;
        /// <summary />
        public RepositorySchema.DimensionTypeConstants DimensionType { get; set; }
        /// <summary />
        public long Didx { get; set; }
        /// <summary />
        public string Parent { get; set; }
        /// <summary />
        public long NumericBreak { get; set; }
        /// <summary />
        public bool IsPivot { get; set; }
        /// <summary />
        public string PivotGroup { get; set; }
        /// <summary />
        public int PivotOrder { get; set; }
        /// <summary />
        public int Length { get; set; }
        /// <summary />
        public bool UserPermission { get; set; }
        /// <summary />
        public bool SearchAsc { get; set; }
        /// <summary />
        public bool IsDataGrouping { get; set; }
        /// <summary>
        /// Allows for the selective toggling of an index on the specified field
        /// </summary>
        public bool AllowIndex { get; set; } = true;
    }
}
