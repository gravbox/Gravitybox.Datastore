using System;
using System.Collections.Generic;
using Gravitybox.Datastore.Common.Queryable;
using Gravitybox.Datastore.Common;

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
        public int? SomeInt2 { get; set; }

        [DatastoreField()]
        public string Field1 { get; set; }

        [DatastoreField()]
        public DateTime? CreatedDate { get; set; }

        [DatastoreField(FieldType = Gravitybox.Datastore.Common.RepositorySchema.FieldTypeConstants.Dimension, DimensionType = Gravitybox.Datastore.Common.RepositorySchema.DimensionTypeConstants.Normal)]
        public string[] MyList { get; set; }

        [DatastoreField(FieldType = Gravitybox.Datastore.Common.RepositorySchema.FieldTypeConstants.Dimension, DimensionType = Gravitybox.Datastore.Common.RepositorySchema.DimensionTypeConstants.Normal)]
        public string Dim2 { get; set; }

        [DatastoreField()]
        public bool MyBool { get; set; }

        [DatastoreField()]
        public bool? MyBool2 { get; set; }

        [DatastoreField()]
        public float MyFloat { get; set; }

        [DatastoreField()]
        public Single MyFloat2 { get; set; }

        [DatastoreField()]
        public double MyFloat3 { get; set; }

        //[DatastoreField()]
        //public byte MyByte { get; set; }

        //[DatastoreField()]
        //public short MyShort { get; set; }

        [DatastoreField()]
        public GeoCode MyGeo { get; set; }

        public Dictionary<string, string> ExtraValues { get; set; }

        public long __RecordIndex { get; set; }

        public long __OrdinalPosition { get; set; }

        public int __Timestamp { get; set; }

        public override string ToString()
        {
            return $"{this.__RecordIndex}, {this.Field1}";
        }

    }
}