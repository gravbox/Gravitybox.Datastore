using System;
using System.Collections.Generic;
using System.Runtime.Serialization;

namespace Gravitybox.Datastore.Common
{
    /// <summary />
    [Serializable]
    [DataContract]
    [KnownType(typeof(GeoCode))]
    [KnownType(typeof(GeoCodeFieldFilter))]
    [KnownType(typeof(DimensionItem))]
    [KnownType(typeof(RefinementItem))]
    [KnownType(typeof(BaseListingQuery))]
    [KnownType(typeof(DataItem))]
    [KnownType(typeof(DataQuery))]
    [KnownType(typeof(FieldSort))]
    [KnownType(typeof(FieldFilter))]
    [KnownType(typeof(GeoCodeFieldFilter))]
    [KnownType(typeof(string[]))]
    [KnownType(typeof(NamedItem))]
    [KnownType(typeof(NamedItemList))]
    [KnownType(typeof(DerivedField))]
    [KnownType(typeof(DerivedFieldValue))]
    public class DataQueryResults
    {
        /// <summary />
        public DataQueryResults()
        {
            this.QueryTime = DateTime.Now;
        }

        /// <summary>
        /// The dimensions that were applied to the query that generated this result set
        /// </summary>
        [DataMember]
        public List<DimensionItem> AppliedDimensionList;

        /// <summary>
        /// The remaining dimensions that can be used to further filter this result set
        /// </summary>
        [DataMember]
        public List<DimensionItem> DimensionList;

        /// <summary>
        /// The full set of dimensions over the total set of data with no filtering applied
        /// </summary>
        [DataMember]
        public List<DimensionItem> AllDimensionList;

        /// <summary>
        /// The query parameters used to generate this result set
        /// </summary>
        [DataMember]
        public DataQuery Query;

        /// <summary>
        /// A paginated set of records generated for this query
        /// </summary>
        [DataMember]
        public List<DataItem> RecordList;

        /// <summary>
        /// The total, non-paginated number of records generated by this query
        /// </summary>
        [DataMember]
        public int TotalRecordCount;

        /// <summary>
        /// The time is took to run this query in milliseconds
        /// </summary>
        [DataMember]
        public long ComputeTime;

        /// <summary>
        /// This is the total time in milliseconds to get a lock before processing
        /// </summary>
        [DataMember]
        public long LockTime;

        /// <summary />
        [DataMember]
        public int DataVersion;

        /// <summary>
        /// Detmermines if the cache was hit by running this query
        /// </summary>
        [DataMember]
        public bool CacheHit;

        /// <summary />
        [DataMember]
        public string[] ErrorList;

        /// <summary />
        [DataMember]
        public long VersionHash { get; set; }

        /// <summary />
        [DataMember]
        public DateTime QueryTime { get; set; }

        /// <summary />
        [DataMember]
        public FieldDefinition[] Fieldset;

        /// <summary />
        [DataMember]
        public DerivedFieldValue[] DerivedFieldList;

    }
}