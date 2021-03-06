#pragma warning disable 0168
using System;
using System.Linq;
using System.Runtime.Serialization;
using Gravitybox.Datastore.Common.Queryable;
using System.Linq.Expressions;

namespace Gravitybox.Datastore.Common
{
    /// <summary />
    [DataContract]
    [Serializable]
    [KnownType(typeof(int[]))]
    [KnownType(typeof(string[]))]
    [KnownType(typeof(bool[]))]
    [KnownType(typeof(int[]))]
    [KnownType(typeof(long[]))]
    public class FieldFilter : Gravitybox.Datastore.Common.IFieldFilter, System.ICloneable
    {
        /// <summary />
        public FieldFilter()
        {
        }

        /// <summary />
        public FieldFilter(string name)
            : this()
        {
            this.Name = name;
        }

        /// <summary />
        [DataMember]
        public ComparisonConstants Comparer { get; set; } = ComparisonConstants.Equals;

        /// <summary />
        [DataMember]
        public string Name { get; set; }

        /// <summary />
        [DataMember]
        public object Value { get; set; }

        /// <summary />
        [DataMember]
        public virtual RepositorySchema.DataTypeConstants DataType { get; set; }

        /// <summary />
        public override bool Equals(object obj)
        {
            if (obj == null) return false;
            return this.ToString() == obj.ToString();
        }

        /// <summary />
        public override int GetHashCode()
        {
            var h = EncryptionDomain.Hash(this.Comparer.ToString()) + "." + this.Name + ".";
            if (((IFieldFilter)this).Value == null)
                h += "NULL";
            else if (((IFieldFilter)this).Value.GetType().IsArray)
            {
                var list = ((System.Collections.IEnumerable)((IFieldFilter)this).Value);
                foreach(var oo in list)
                {
                    h += oo?.ToString() + "|";
                }
            }
            else if (((IFieldFilter)this).Value.GetType().IsAssignableFrom(typeof(System.Collections.Generic.List<int>)))
            {
                var list = ((System.Collections.Generic.List<int>)((IFieldFilter)this).Value);
                foreach (var oo in list)
                {
                    h += oo.ToString() + "|";
                }
            }
            else if (((IFieldFilter)this).Value.GetType().IsAssignableFrom(typeof(System.Collections.Generic.List<int?>)))
            {
                var list = ((System.Collections.Generic.List<int?>)((IFieldFilter)this).Value);
                foreach (var oo in list)
                {
                    h += oo?.ToString() + "|";
                }
            }
            else if (((IFieldFilter)this).Value.GetType().IsAssignableFrom(typeof(System.Collections.Generic.List<long>)))
            {
                var list = ((System.Collections.Generic.List<long>)((IFieldFilter)this).Value);
                foreach (var oo in list)
                {
                    h += oo.ToString() + "|";
                }
            }
            else if (((IFieldFilter)this).Value.GetType().IsAssignableFrom(typeof(System.Collections.Generic.List<long?>)))
            {
                var list = ((System.Collections.Generic.List<long?>)((IFieldFilter)this).Value);
                foreach (var oo in list)
                {
                    h += oo?.ToString() + "|";
                }
            }
            else if (((IFieldFilter)this).Value.GetType().IsAssignableFrom(typeof(System.Collections.Generic.List<string>)))
            {
                var list = ((System.Collections.Generic.List<string>)((IFieldFilter)this).Value);
                foreach (var oo in list)
                {
                    h += oo?.ToString() + "|";
                }
            }
            else
                h += ((IFieldFilter)this).Value.ToString();

            return EncryptionDomain.Hash(h);
        }

        /// <summary />
        public override string ToString()
        {
            try
            {
                var retval = string.Empty;
                var f1 = (Gravitybox.Datastore.Common.IFieldFilter)this;

                if (this.DataType == RepositorySchema.DataTypeConstants.GeoCode)
                {
                    var gff = this as Gravitybox.Datastore.Common.GeoCodeFieldFilter;
                    if (gff != null)
                        retval = $"{this.Name},{this.Comparer},{gff.Latitude},{gff.Longitude},{gff.Radius}|";
                }
                else if (this.DataType == RepositorySchema.DataTypeConstants.List)
                {
                    if (f1.Value != null)
                    {
                        //TODO: handle different array types and null values
                        var arr1 = (string[])f1.Value;
                        retval = this.Name + "," + this.Comparer.ToString() + "," + string.Join("^^", arr1) + "|";
                    }
                }
                else
                {
                    if (f1.Value != null)
                    {
                        var v = f1.Value.ToString();
                        if (f1.Value.GetType() == typeof(string[]))
                            v = string.Join("^", ((string[])f1.Value));
                        else if (f1.Value.GetType() == typeof(int[]))
                            v = string.Join("^", ((int[])f1.Value));
                        else if (f1.Value.GetType() == typeof(bool[]))
                            v = string.Join("^", ((bool[])f1.Value));
                        else if (f1.Value.GetType() == typeof(long[]))
                            v = string.Join("^", ((long[])f1.Value));
                        else
                            v = f1.Value.ToString();

                        retval = $"{this.Name},{this.Comparer},{v}|";
                    }
                    else
                    {
                        retval = $"{this.Name},{this.Comparer},NULL|";
                    }
                }

                retval = retval.TrimEnd(new char[] { '|' });
                return retval;
            }
            catch (Exception ex)
            {
                throw;
            }
        }

        /// <summary />
        public virtual bool FromUrl(string url)
        {
            try
            {
                var svalues = url.Split(new char[] { ',' }, StringSplitOptions.RemoveEmptyEntries);

                if (svalues.Length < 3)
                    return false;

                Datastore.Common.ComparisonConstants enumValue;
                if (!Enum.TryParse<Datastore.Common.ComparisonConstants>(svalues[1], true, out enumValue))
                    return false;

                if (svalues.Length == 5 &&
                    enumValue != ComparisonConstants.ContainsAll &&
                    enumValue != ComparisonConstants.ContainsAny &&
                    enumValue != ComparisonConstants.ContainsNone)
                {
                    if (string.IsNullOrEmpty(svalues[0]))
                        return false;
                    if (!(this is GeoCodeFieldFilter))
                        return false;

                    this.Name = svalues[0];
                    this.Comparer = enumValue;
                    ((GeoCodeFieldFilter)this).Latitude = svalues[2].ToDouble();
                    ((GeoCodeFieldFilter)this).Longitude = svalues[3].ToDouble();
                    ((GeoCodeFieldFilter)this).Radius = svalues[4].ToInt();
                }
                else if (svalues.Length >= 3)
                {
                    if (string.IsNullOrEmpty(svalues[0]))
                        return false;

                    this.Name = svalues[0];
                    this.Comparer = enumValue;
                    ((IFieldFilter)this).Value = svalues[2];
                }
                return true;
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex);
                throw;
            }
        }


        object ICloneable.Clone()
        {
            var retval = new FieldFilter {Comparer = this.Comparer, Name = this.Name, DataType = this.DataType};
            ((IFieldFilter) retval).Value = ((IFieldFilter) this).Value;
            return retval;
        }

        /// <summary />
        public static FieldFilter Create<TSourceType>(Expression<Func<TSourceType, object>> member)
            where TSourceType : IDatastoreItem
        {
            var memberName = ExpressionHelper.GetMemberName(member);
            if (memberName == null)
                return null;
            return Create<TSourceType>(memberName);
        }

        /// <summary />
        public static FieldFilter Create<TSourceType>(string name)
            where TSourceType : IDatastoreItem
        {
            var fields = DatastoreService.GetFields(typeof(TSourceType));
            var field = fields.FirstOrDefault(x => x.Name.Match(name));
            if (field != null)
            {
                if (field.DataType == RepositorySchema.DataTypeConstants.GeoCode)
                {
                    return new GeoCodeFieldFilter()
                    {
                        Name = field.Name,
                        Comparer = ComparisonConstants.LessThanOrEq,
                        DataType = field.DataType,
                    };
                }
                else
                {
                    return new FieldFilter()
                    {
                        Name = field.Name,
                        Comparer = ComparisonConstants.Equals,
                        DataType = field.DataType,
                    };
                }
            }
            return null;
        }
    }
}