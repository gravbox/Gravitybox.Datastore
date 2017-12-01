using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Runtime.Serialization;

namespace Gravitybox.Datastore.Common
{
    /// <summary />
    [Serializable]
    [DataContract]
    [KnownType(typeof(GeoCode))]
    [KnownType(typeof(GeoCodeFieldFilter))]
    [KnownType(typeof(string[]))]
    [KnownType(typeof(FieldFilter))]
    [KnownType(typeof(FieldSort))]
    [KnownType(typeof(NamedItem))]
    [KnownType(typeof(NamedItemList))]
    [KnownType(typeof(UserCredentials))]
    [KnownType(typeof(DerivedField))]
    [KnownType(typeof(DerivedFieldValue))]
    public class DataQuery : BaseListingQuery, ICloneable
    {
        /// <summary />
        public DataQuery()
            : base()
        {
        }

        /// <summary />
        public DataQuery(string url)
            : base(url)
        {
        }

        object ICloneable.Clone()
        {
            var url = this.ToString();
            if (!url.Contains("?") && !url.StartsWith("/"))
                url = "?" + url;

            var reval = new DataQuery(url);
            reval.SkipDimensions = this.SkipDimensions;
            reval.DerivedFieldList = this.DerivedFieldList;
            reval.FieldSelects = this.FieldSelects;
            reval.IPMask = this.IPMask;
            reval.NonParsedFieldList = this.NonParsedFieldList;
            reval.QueryID = this.QueryID;
            reval.UserList = this.UserList;
            return reval;
        }
    }
}