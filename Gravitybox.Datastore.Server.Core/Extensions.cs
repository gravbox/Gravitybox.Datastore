using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Gravitybox.Datastore.Common;
using System.Collections;

namespace Gravitybox.Datastore.Server.Core
{
    public static class Extensions
    {
        public static bool ToBool(this string s)
        {
            if (string.IsNullOrEmpty(s)) return false;
            s = s.ToLower();
            return (s == "true" || s == "1");
        }

        public static bool IsDefault<T>(this T value) where T : struct
        {
            var isDefault = value.Equals(default(T));
            return isDefault;
        }

        public static List<T> ToSqlList<T>(this System.Data.DataSet ds, string field)
        {
            try
            {
                var retval = new List<T>();
                if (ds == null) return retval;
                if (ds.Tables.Count == 0) return retval;
                foreach (System.Data.DataRow dr in ds.Tables[0].Rows)
                {
                    retval.Add((T)dr[field]);
                }
                return retval;
            }
            catch (Exception)
            {
                throw;
            }
        }

        public static List<Tuple<T, K>> ToSqlList<T, K>(this System.Data.DataSet ds, string field1, string field2)
        {
            try
            {
                var retval = new List<Tuple<T, K>>();
                if (ds == null) return retval;
                if (ds.Tables.Count == 0) return retval;
                foreach (System.Data.DataRow dr in ds.Tables[0].Rows)
                {
                    retval.Add(new Tuple<T, K>((T)dr[field1], (K)dr[field2]));
                }
                return retval;
            }
            catch (Exception)
            {
                throw;
            }
        }

        public static string GetSqlDefinition(this FieldDefinition field)
        {
            var sqlLength = string.Empty;
            if (field.DataType == RepositorySchema.DataTypeConstants.String)
            {
                if (field.Length > 0) sqlLength = $"({field.Length})";
                else sqlLength = "(MAX)";
            }
            return $"[{field.TokenName}] [{field.ToSqlType()}] {sqlLength}";
        }

        public static HashSet<T> ToHash<T>(this IEnumerable<T> list)
        {
            var retval = new HashSet<T>();
            if (list == null) return retval;
            foreach (var item in list)
                retval.Add(item);
            return retval;
        }

        public static string ToSqlDirection(this IFieldSort sort)
        {
            if (sort == null) return string.Empty;
            return sort.SortDirection.ToSqlDirection();
        }

        public static string ToSqlDirection(this SortDirectionConstants sort)
        {
            return sort == SortDirectionConstants.Desc ? "DESC" : "ASC";
        }

        public static string ToSqlType(this IFieldDefinition field)
        {
            if (field == null) return string.Empty;
            return ServerUtilities.GetSqlType(field.DataType);
        }

        public static string ReplaceSqlTicks(this string str)
        {
            return str?.Replace("'", "''");
        }

        /// <summary>
        /// Concatenates the members of a list into a string with a comma as a separator
        /// </summary>
        internal static string ToCommaList<T>(this IEnumerable<T> list)
        {
            return list.ToStringList(",");
        }

        /// <summary>
        /// Concatenates the members of a list into a string with a separator
        /// </summary>
        internal static string ToStringList<T>(this IEnumerable<T> list, string separator)
        {
            if (list == null || !list.Any()) return string.Empty;
            return string.Join(separator, list);
        }

        /// <summary />
        internal static long? ToInt64(this string v)
        {
            if (string.IsNullOrEmpty(v)) return null;
            if (long.TryParse(v, out long parsed))
                return parsed;
            return null;
        }

        /// <summary>
        /// Given a DataItem object this will generate a repeatable has for its state
        /// </summary>
        public static long Hash(this DataItem item)
        {
            if (item == null || item.ItemArray == null) return 0;
            var sb = new StringBuilder();
            foreach (var o in item.ItemArray)
            {
                if (o == null)
                    sb.Append("~~NULL|");
                else if (o is string)
                    sb.Append((string)o);
                else if (o is DateTime)
                    sb.Append(((DateTime)o).Ticks);
                else if (o is GeoCode)
                {
                    var g = o as GeoCode;
                    sb.Append(g.Latitude + "*" + g.Longitude);
                }
                else
                    sb.Append(o.ToString());
            }
            string data = sb.ToString();

            UInt64 hashedValue = 3074457345618258791ul;
            for (int i = 0; i < data.Length; i++)
            {
                hashedValue += data[i];
                hashedValue *= 3074457345618258799ul;
            }

            //Convert to long as it is just a hash.
            //We do not care what the actual value is as long as it is unique
            return (long)hashedValue;
        }

        /// <summary />
        internal static bool Match(this string s, string str)
        {
            if (s == null && str == null) return true;
            if (s != null && str == null) return false;
            if (s == null && str != null) return false;
            return string.Equals(s, str, StringComparison.InvariantCultureIgnoreCase);
        }
    }
}