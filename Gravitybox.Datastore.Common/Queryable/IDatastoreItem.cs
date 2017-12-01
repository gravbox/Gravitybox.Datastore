using System.Collections.Generic;

namespace Gravitybox.Datastore.Common.Queryable
{
    /// <summary />
    public interface IDatastoreItem
    {
        /// <summary>
        /// Additional field values that are not part of the object schema
        /// </summary>
        Dictionary<string, string> ExtraValues { get; set; }

        /// <summary>
        /// The internal unique key that identifies this record
        /// </summary>
        long __RecordIndex { get; set; }

        /// <summary>
        /// The ordinal position in the list of this record [0..N-1]
        /// </summary>
        long __OrdinalPosition { get; set; }

        /// <summary>
        /// A timestamp value reset when the record is modified
        /// </summary>
        int __Timestamp { get; set; }
    }
}
