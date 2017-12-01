#pragma warning disable 0168
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Runtime.Serialization;
using Gravitybox.Datastore.Common.Queryable;

namespace Gravitybox.Datastore.Common
{
    /// <summary />
    [DataContract]
    [Serializable]
    public class FieldSort : Gravitybox.Datastore.Common.IFieldSort, System.ICloneable
    {
        /// <summary />
        public FieldSort()
        {
        }

        /// <summary />
        public FieldSort(string name)
            : this()
        {
            this.Name = name;
        }

        /// <summary />
        [DataMember]
        public virtual Gravitybox.Datastore.Common.SortDirectionConstants SortDirection { get; set; }

        /// <summary />
        [DataMember]
        public virtual string Name { get; set; }

        /// <summary />
        public virtual string TokenName
        {
            get { return Utilities.DbTokenize(this.Name); }
        }

        /// <summary />
        public override int GetHashCode()
        {
            return EncryptionDomain.Hash(this.SortDirection.ToString() + "·" + this.Name);
        }

        /// <summary />
        public override string ToString()
        {
            try
            {
                return this.Name + (this.SortDirection == Gravitybox.Datastore.Common.SortDirectionConstants.Asc ? string.Empty : ",0");
            }
            catch (Exception ex)
            {
                throw;
            }
        }

        /// <summary />
        public static FieldSort FromString(string str)
        {
            var retval = new FieldSort();
            if (!string.IsNullOrEmpty(str))
            {
                var arr = str.Split(new char[] { ',' });
                if (arr.Length > 0) retval.Name = arr[0];
                if (arr.Length > 1) retval.SortDirection = (arr[1] == "1" ? Gravitybox.Datastore.Common.SortDirectionConstants.Desc : Gravitybox.Datastore.Common.SortDirectionConstants.Asc);
            }
            return retval;
        }

        object ICloneable.Clone()
        {
            return new FieldSort { Name = this.Name, SortDirection = this.SortDirection };
        }

        /// <summary />
        public static FieldSort Create<TSourceType>(Expression<Func<TSourceType, object>> member, bool asc = true)
            where TSourceType : IDatastoreItem
        {
            var memberName = ExpressionHelper.GetMemberName(member);
            if (memberName == null)
                return null;
            return Create<TSourceType>(memberName, asc);
        }

        /// <summary />
        public static FieldSort Create<TSourceType>(string name, bool asc = true)
            where TSourceType : IDatastoreItem
        {
            var fields = DatastoreService.GetFields(typeof(TSourceType));
            var field = fields.FirstOrDefault(x => x.Name.Match(name));
            if (field != null)
            {
                if (field.DataType == RepositorySchema.DataTypeConstants.GeoCode ||
                    field.DataType == RepositorySchema.DataTypeConstants.List)
                {
                    return null;
                }
                return new FieldSort { Name = field.Name, SortDirection = (asc ? SortDirectionConstants.Asc : SortDirectionConstants.Desc) };
            }
            return null;
        }

    }
}