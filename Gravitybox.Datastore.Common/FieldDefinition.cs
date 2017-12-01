using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Runtime.Serialization;

namespace Gravitybox.Datastore.Common
{
    /// <summary />
    [DataContract]
    [Serializable]
    [KnownType(typeof(DimensionDefinition))]
    public class FieldDefinition : IFieldDefinition
    {
        /// <summary />
        public FieldDefinition()
        {
        }

        /// <summary />
        [DataMember]
        public virtual string Name { get; set; }

        /// <summary />
        [DataMember]
        public virtual RepositorySchema.FieldTypeConstants FieldType { get; set; }

        /// <summary />
        [DataMember]
        public virtual int Length { get; set; } = 100;

        /// <summary />
        [DataMember]
        public virtual RepositorySchema.DataTypeConstants DataType { get; set; } = RepositorySchema.DataTypeConstants.String;

        /// <summary />
        [DataMember]
        public virtual bool IsPrimaryKey { get; set; } = false;

        /// <summary />
        [DataMember]
        public virtual bool AllowTextSearch { get; set; } = false;

        /// <summary />
        [DataMember]
        public virtual bool SearchAsc { get; set; } = true;

        /// <summary />
        [DataMember]
        public bool IsPivot;

        /// <summary />
        [DataMember]
        public string PivotGroup;

        /// <summary />
        [DataMember]
        public int PivotOrder;

        /// <summary />
        [DataMember]
        public string Description;

        /// <summary />
        [DataMember]
        public bool IsDataGrouping;

        /// <summary />
        [DataMember]
        public bool AllowNull = true;

        /// <summary>
        /// Allows for the selective toggling of an index on the specified field
        /// </summary>
        [DataMember]
        public bool AllowIndex = true;

        /// <summary />
        public string TokenName
        {
            get { return Utilities.DbTokenize(this.Name); }
        }

        /// <summary />
        public override string ToString()
        {
            return this.Name + ", " + this.DataType.ToString();
        }

        /// <summary />
        public bool IsSortable
        {
            //List and Geo are not sortable
            get { return !(this.DataType == RepositorySchema.DataTypeConstants.GeoCode || this.DataType == RepositorySchema.DataTypeConstants.List); }
        }

        /// <summary />
        public virtual int Hash
        {
            get
            {
                var h = this.Name + "|" +
                    this.FieldType.ToString() + "|" +
                    this.IsPivot + "|" +
                    this.PivotGroup + "|" +
                    //this.PivotOrder + "|" +
                    (this.DataType != Gravitybox.Datastore.Common.RepositorySchema.DataTypeConstants.String ? string.Empty : this.Length.ToString()) + "|" +
                    this.DataType.ToString() + "|" +
                    this.IsPrimaryKey + "|" +
                    (this.AllowTextSearch && this.DataType == RepositorySchema.DataTypeConstants.String).ToString();

                return (int)EncryptionDomain.HashFast(h);
            }
        }

        /// <summary />
        public virtual int HashNoFts
        {
            get
            {
                var h = this.Name + "|" +
                    this.FieldType.ToString() + "|" +
                    this.IsPivot + "|" +
                    this.PivotGroup + "|" +
                    //this.PivotOrder + "|" +
                    (this.DataType != Gravitybox.Datastore.Common.RepositorySchema.DataTypeConstants.String ? string.Empty : this.Length.ToString()) + "|" +
                    this.DataType.ToString() + "|" +
                    this.IsPrimaryKey;

                return (int)EncryptionDomain.HashFast(h);
            }
        }

        /// <summary />
        public override bool Equals(object obj)
        {
            var target = obj as FieldDefinition;
            if (target == null) return false;
            return (this.Hash == target.Hash);
        }

        /// <summary />
        public override int GetHashCode()
        {
            return this.Hash;
        }

        [OnDeserializing]
        private void Setup(StreamingContext sc)
        {
            this.IsPivot = false;
        }

    }
}