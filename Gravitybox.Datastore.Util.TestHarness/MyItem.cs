using System;
using System.Collections.Generic;
using Gravitybox.Datastore.Common.Queryable;

namespace Gravitybox.Datastore.Util.TestHarness
{
    [DatastoreRepository(Name = "MyItem", Id = "00000000-0000-0000-0000-17728d4a8361")]
    [Serializable]
    public class MyItem : IDatastoreItem
    {
        [DatastoreField(IsPrimaryKey = true)]
        public int ID { get; set; }

        [DatastoreField(AllowTextSearch = true)]
        public string Project { get; set; }

        [DatastoreField()]
        public string Field1 { get; set; }

        [DatastoreField()]
        public DateTime? CreatedDate { get; set; }

        [DatastoreField(FieldType = Gravitybox.Datastore.Common.RepositorySchema.FieldTypeConstants.Dimension, DimensionType = Gravitybox.Datastore.Common.RepositorySchema.DimensionTypeConstants.Normal)]
        public string[] MyList { get; set; }

        [DatastoreField(FieldType = Gravitybox.Datastore.Common.RepositorySchema.FieldTypeConstants.Dimension, DimensionType = Gravitybox.Datastore.Common.RepositorySchema.DimensionTypeConstants.Normal)]
        public string Dim2 { get; set; }

        public Dictionary<string, string> ExtraValues { get; set; }

        public long __RecordIndex { get; set; }

        public long __OrdinalPosition { get; set; }

        public int __Timestamp { get; set; }

    }
}