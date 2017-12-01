using System;
using System.Runtime.Serialization;

namespace Gravitybox.Datastore.Common
{
    /// <summary />
    [DataContract]
    [Serializable]
    [KnownType(typeof(GeoCode))]
    [KnownType(typeof(GeoCodeFieldFilter))]
    public class DimensionDefinition : FieldDefinition
    {
        /// <summary />
        public DimensionDefinition()
            : base()
        {
            this.MultivalueComparison = RepositorySchema.MultivalueComparisonContants.Union;
        }

        /// <summary />
        [DataMember]
        public override bool IsPrimaryKey
        {
            get { return false; }
            set { ; }
        }

        /// <summary />
        [DataMember]
        public virtual long DIdx { get; set; }

        /// <summary />
        [DataMember]
        public virtual RepositorySchema.DimensionTypeConstants DimensionType { get; set; }

        /// <summary />
        [DataMember]
        public virtual long? NumericBreak { get; set; }

        /// <summary />
        [DataMember]
        public string Parent { get; set; }

        /// <summary />
        [DataMember]
        public override RepositorySchema.FieldTypeConstants FieldType
        {
            get { return RepositorySchema.FieldTypeConstants.Dimension; }
            set { ; }
        }

        /// <summary />
        public override RepositorySchema.DataTypeConstants DataType
        {
            get
            {
                if (this.DimensionType == RepositorySchema.DimensionTypeConstants.List)
                    return RepositorySchema.DataTypeConstants.List;
                else
                    return base.DataType;
            }
            set { base.DataType = value; }
        }

        /// <summary />
        //THIS NEED TO BE FIXED. THE DESERIALIZE DOES NOT WORK IF WE MAKE THIS A REAL PROPERTY!!!!!
        [DataMember(IsRequired = false)]
        public virtual RepositorySchema.MultivalueComparisonContants MultivalueComparison //{ get; set; }
        {
            get { return RepositorySchema.MultivalueComparisonContants.Union; }
            set { ; }
        }

        /// <summary />
        public override int Hash
        {
            //Make sure it is different from a field definition
            get { return base.Hash + 1; }
        }

        /// <summary />
        public override string ToString()
        {
            return this.Name + ", " + this.DataType.ToString() + "," + this.DIdx;
        }

    }
}