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
            return sort.SortDirection == SortDirectionConstants.Desc ? "DESC" : "ASC";
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

        internal static string ToCommaList(this IEnumerable<string> list)
        {
            if (list == null || !list.Any()) return string.Empty;
            return string.Join(",", list);
        }
    }
}