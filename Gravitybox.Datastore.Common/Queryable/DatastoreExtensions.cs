using System;
using System.Linq;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;

namespace Gravitybox.Datastore.Common.Queryable
{

    #region IDatastoreQueryable

    /// <summary />
    public interface IDatastoreQueryable
    {
        /// <summary />
        Expression Expression { get; }

        /// <summary />
        Type ElementType { get; }

        /// <summary />
        IDatastoreProvider Provider { get; }
    }

    // IDatastoreQueryable uses two types TSourceType and TResultType because of the DatastoreResult and DimensionStore objects.
    // Most of the time the types are the same, but when you perform a Select then TSourceType chantes to TResultType.
    // I needed the TSourceType to remain constant so that I could pass it to the DimensionStore object which provide dimension 
    // lookups based on TSourceType members.

    /// <summary />
    public interface IDatastoreQueryable<out T> : IDatastoreQueryable
    {
        /// <summary />
        IDatastoreQueryable<T> Clone();
    }

    /// <summary />
    public interface IDatastoreOrderedQueryable : IDatastoreQueryable
    {
    }

    /// <summary />
    public interface IDatastoreOrderedQueryable<out T> : IDatastoreQueryable<T>, IDatastoreOrderedQueryable
    {
    }

    /// <summary />
    public interface IDatastoreGroupable
    {
        /// <summary />
        Expression Expression { get; }

        /// <summary />
        IDatastoreProvider Provider { get; }
    }

    /// <summary />
    public interface IDatastoreGroupable<out T> : IDatastoreGroupable
    {
    }

    /// <summary />
    public interface IDatastoreGrouping<out TKey, out TElement>
    {
        /// <summary />
        TKey Key { get; }
    }

    #endregion

    #region IDatastoreUpdatable

    /// <summary />
    public interface IDatastoreUpdatable
    {
        /// <summary />
        Expression Expression { get; }

        /// <summary />
        IDatastoreProvider Provider { get; }
    }

    /// <summary />
    public interface IDatastoreUpdatable<out T> : IDatastoreUpdatable
    {
    }

    #endregion

    #region IDatastoreDeletable

    /// <summary />
    public interface IDatastoreDeletable
    {
        /// <summary />
        Expression Expression { get; }

        /// <summary />
        IDatastoreProvider Provider { get; }
    }

    /// <summary />
    public interface IDatastoreDeletable<out T> : IDatastoreDeletable
    {
    }

    #endregion

    #region IDatastoreSliceable

    /// <summary />
    public interface IDatastoreSliceable
    {
        /// <summary />
        Expression Expression { get; }

        /// <summary />
        Type ElementType { get; }

        /// <summary />
        IDatastoreProvider Provider { get; }
    }

    /// <summary />
    public interface IDatastoreSliceable<out T> : IDatastoreSliceable
    {
    }

    /// <summary />
    public interface IDatastoreOrderedSliceable : IDatastoreSliceable
    {
    }

    /// <summary />
    public interface IDatastoreOrderedSliceable<out T> : IDatastoreSliceable<T>, IDatastoreOrderedSliceable
    {
    }

    #endregion

    #region IDatastoreProvider

    /// <summary />
    public interface IDatastoreProvider
    {
        /// <summary />
        IDatastoreQueryable<TResult> CreateQuery<TResult>(Expression expression);

        /// <summary />
        IDatastoreUpdatable<TResult> CreateUpdateQuery<TResult>(Expression expression);

        /// <summary />
        IDatastoreDeletable<TResult> CreateDeleteQuery<TResult>(Expression expression);

        /// <summary />
        IDatastoreSliceable<TResult> CreateSliceQuery<TResult>(Expression expression);

        /// <summary />
        IDatastoreGroupable<IDatastoreGrouping<TKey, TResult>> CreateGroupingQuery<TKey, TResult>(Expression expression);

        /// <summary />
        IDatastoreGroupable<TResult> CreateGroupingSelectQuery<TKey, TResult>(Expression expression);

        /// <summary />
        TResult Execute<TResult>(Expression expression);
    }

    #endregion

    #region DatastoreExtensions

    /// <summary />
    public static class DatastoreExtensions
    {
        private const string ERROR_SOURCE = "The source argument was not found.";

        #region Helper methods to obtain MethodInfo in a safe way

        private static MethodInfo GetMethodInfo<T1, T2>(Func<T1, T2> f, T1 unused1)
        {
            return f.Method;
        }

        private static MethodInfo GetMethodInfo<T1, T2, T3>(Func<T1, T2, T3> f, T1 unused1, T2 unused2)
        {
            return f.Method;
        }

        private static MethodInfo GetMethodInfo<T1, T2, T3, T4>(Func<T1, T2, T3, T4> f, T1 unused1, T2 unused2, T3 unused3)
        {
            return f.Method;
        }

        private static MethodInfo GetMethodInfo<T1, T2, T3, T4, T5>(Func<T1, T2, T3, T4, T5> f, T1 unused1, T2 unused2, T3 unused3, T4 unused4)
        {
            return f.Method;
        }

        private static MethodInfo GetMethodInfo<T1, T2, T3, T4, T5, T6>(Func<T1, T2, T3, T4, T5, T6> f, T1 unused1, T2 unused2, T3 unused3, T4 unused4, T5 unused5)
        {
            return f.Method;
        }

        private static MethodInfo GetMethodInfo<T1, T2, T3, T4, T5, T6, T7>(Func<T1, T2, T3, T4, T5, T6, T7> f, T1 unused1, T2 unused2, T3 unused3, T4 unused4, T5 unused5, T6 unused6)
        {
            return f.Method;
        }

        #endregion

        #region Paging

        /// <summary />
        public static IDatastoreQueryable<TSourceType> RecordsPerPage<TSourceType>(this IDatastoreQueryable<TSourceType> source, int recordsPerPage)
            where TSourceType : IDatastoreItem
        {
            if (source == null)
                throw new ArgumentNullException(ERROR_SOURCE);

            return source.Provider.CreateQuery<TSourceType>(
                Expression.Call(
                    null,
                    GetMethodInfo(DatastoreExtensions.RecordsPerPage<TSourceType>, source, recordsPerPage),
                    new Expression[] { source.Expression, Expression.Constant(recordsPerPage) }
                ));
        }

        /// <summary />
        public static IDatastoreQueryable<TSourceType> PageOffset<TSourceType>(this IDatastoreQueryable<TSourceType> source, int pageOffset)
            where TSourceType : IDatastoreItem
        {
            if (source == null)
                throw new ArgumentNullException(ERROR_SOURCE);

            return source.Provider.CreateQuery<TSourceType>(
                Expression.Call(
                    null,
                    GetMethodInfo(DatastoreExtensions.PageOffset<TSourceType>, source, pageOffset),
                    new Expression[] { source.Expression, Expression.Constant(pageOffset) }
                ));
        }

        /// <summary />
        public static IDatastoreSliceable<TSourceType> RecordsPerPage<TSourceType>(this IDatastoreSliceable<TSourceType> source, int recordsPerPage)
            where TSourceType : IDatastoreItem
        {
            if (source == null)
                throw new ArgumentNullException(ERROR_SOURCE);

            return source.Provider.CreateSliceQuery<TSourceType>(
                Expression.Call(
                    null,
                    GetMethodInfo(DatastoreExtensions.RecordsPerPage<TSourceType>, source, recordsPerPage),
                    new Expression[] { source.Expression, Expression.Constant(recordsPerPage) }
                ));
        }

        /// <summary />
        public static IDatastoreSliceable<TSourceType> PageOffset<TSourceType>(this IDatastoreSliceable<TSourceType> source, int pageOffset)
            where TSourceType : IDatastoreItem
        {
            if (source == null)
                throw new ArgumentNullException(ERROR_SOURCE);

            return source.Provider.CreateSliceQuery<TSourceType>(
                Expression.Call(
                    null,
                    GetMethodInfo(DatastoreExtensions.PageOffset<TSourceType>, source, pageOffset),
                    new Expression[] { source.Expression, Expression.Constant(pageOffset) }
                ));
        }

        #endregion

        #region Include/Exclude

        /// <summary />
        public static IDatastoreQueryable<TSourceType> IncludeDimensions<TSourceType>(this IDatastoreQueryable<TSourceType> source, bool value)
            where TSourceType : IDatastoreItem
        {
            if (source == null)
                throw new ArgumentNullException(ERROR_SOURCE);

            return source.Provider.CreateQuery<TSourceType>(
                Expression.Call(
                    null,
                    GetMethodInfo(DatastoreExtensions.IncludeDimensions<TSourceType>, source, value),
                    new Expression[] { source.Expression, Expression.Constant(value) }
                ));
        }

        /// <summary />
        public static IDatastoreQueryable<TSourceType> IncludeRecords<TSourceType>(this IDatastoreQueryable<TSourceType> source, bool value)
            where TSourceType : IDatastoreItem
        {
            if (source == null)
                throw new ArgumentNullException(ERROR_SOURCE);

            return source.Provider.CreateQuery<TSourceType>(
                Expression.Call(
                    null,
                    GetMethodInfo(DatastoreExtensions.IncludeRecords<TSourceType>, source, value),
                    new Expression[] { source.Expression, Expression.Constant(value) }
                ));
        }

        /// <summary />
        public static IDatastoreQueryable<TSourceType> IncludeIpAddress<TSourceType>(this IDatastoreQueryable<TSourceType> source, string value)
            where TSourceType : IDatastoreItem
        {
            if (source == null)
                throw new ArgumentNullException(ERROR_SOURCE);
            if (value == null) return source;

            return source.Provider.CreateQuery<TSourceType>(
                Expression.Call(
                    null,
                    GetMethodInfo(DatastoreExtensions.IncludeIpAddress<TSourceType>, source, value),
                    new Expression[] { source.Expression, Expression.Constant(value) }
                ));
        }

        /// <summary />
        public static IDatastoreQueryable<TSourceType> ExcludeCount<TSourceType>(this IDatastoreQueryable<TSourceType> source, bool value)
            where TSourceType : IDatastoreItem
        {
            if (source == null)
                throw new ArgumentNullException(ERROR_SOURCE);

            return source.Provider.CreateQuery<TSourceType>(
                Expression.Call(
                    null,
                    GetMethodInfo(DatastoreExtensions.ExcludeCount<TSourceType>, source, value),
                    new Expression[] { source.Expression, Expression.Constant(value) }
                ));
        }

        /// <summary />
        public static IDatastoreQueryable<TSourceType> IncludeEmptyDimensions<TSourceType>(this IDatastoreQueryable<TSourceType> source)
            where TSourceType : IDatastoreItem
        {
            if (source == null)
                throw new ArgumentNullException(ERROR_SOURCE);

            return source.Provider.CreateQuery<TSourceType>(
                Expression.Call(
                    null,
                    GetMethodInfo(DatastoreExtensions.IncludeEmptyDimensions<TSourceType>, source),
                    new Expression[] { source.Expression, }
                ));
        }

        /// <summary />
        public static IDatastoreDeletable<TSourceType> IncludeIpAddress<TSourceType>(this IDatastoreDeletable<TSourceType> source, string value)
            where TSourceType : IDatastoreItem
        {
            if (source == null)
                throw new ArgumentNullException(ERROR_SOURCE);
            if (value == null) return source;

            return source.Provider.CreateDeleteQuery<TSourceType>(
                Expression.Call(
                    null,
                    GetMethodInfo(DatastoreExtensions.IncludeIpAddress<TSourceType>, source, value),
                    new Expression[] { source.Expression, Expression.Constant(value) }
                ));
        }

        /// <summary />
        public static IDatastoreUpdatable<TSourceType> IncludeIpAddress<TSourceType>(this IDatastoreUpdatable<TSourceType> source, string value)
            where TSourceType : IDatastoreItem
        {
            if (source == null)
                throw new ArgumentNullException(ERROR_SOURCE);
            if (value == null) return source;

            return source.Provider.CreateUpdateQuery<TSourceType>(
                Expression.Call(
                    null,
                    GetMethodInfo(DatastoreExtensions.IncludeIpAddress<TSourceType>, source, value),
                    new Expression[] { source.Expression, Expression.Constant(value) }
                ));
        }

        #endregion

        #region WhereKeyword

        /// <summary>
        /// Apply a keyword filter on to the query
        /// </summary>
        public static IDatastoreQueryable<TSourceType> WhereKeyword<TSourceType>(this IDatastoreQueryable<TSourceType> source, string keyword)
            where TSourceType : IDatastoreItem
        {
            if (source == null)
                throw new ArgumentNullException(ERROR_SOURCE);

            return source.Provider.CreateQuery<TSourceType>(
                Expression.Call(
                    null,
                    GetMethodInfo(DatastoreExtensions.WhereKeyword<TSourceType>, source, keyword),
                    new Expression[] { source.Expression, Expression.Constant(keyword + string.Empty) } //avoid null problem, make empty
                ));
        }

        #endregion

        #region WhereUrl

        /// <summary>
        /// Uses a URL to initialize the query object
        /// </summary>
        public static IDatastoreQueryable<TSourceType> WhereUrl<TSourceType>(this IDatastoreQueryable<TSourceType> source, string url)
            where TSourceType : IDatastoreItem
        {
            if (source == null)
                throw new ArgumentNullException(ERROR_SOURCE);

            return source.Provider.CreateQuery<TSourceType>(
                Expression.Call(
                    null,
                    GetMethodInfo(DatastoreExtensions.WhereUrl<TSourceType>, source, url),
                    new Expression[] { source.Expression, Expression.Constant(url) }
                ));
        }

        /// <summary>
        /// Uses a URL to initialize the query object
        /// </summary>
        public static IDatastoreSliceable<TSourceType> WhereUrl<TSourceType>(this IDatastoreSliceable<TSourceType> source, string url)
            where TSourceType : IDatastoreItem
        {
            if (source == null)
                throw new ArgumentNullException(ERROR_SOURCE);

            if (!string.IsNullOrEmpty(url))
                url = System.Web.HttpUtility.UrlDecode(url);
            return source.Provider.CreateSliceQuery<TSourceType>(
                Expression.Call(
                    null,
                    GetMethodInfo(DatastoreExtensions.WhereUrl<TSourceType>, source, url),
                    new Expression[] { source.Expression, Expression.Constant(url) }
                ));
        }

        #endregion

        #region FromQuery

        /// <summary>
        /// Uses a URL to initialize the query object
        /// </summary>
        public static IDatastoreQueryable<TSourceType> FromQuery<TSourceType>(this IDatastoreQueryable<TSourceType> source, DataQuery query)
            where TSourceType : IDatastoreItem
        {
            if (source == null)
                throw new ArgumentNullException(ERROR_SOURCE);

            return source.Provider.CreateQuery<TSourceType>(
                Expression.Call(
                    null,
                    GetMethodInfo(DatastoreExtensions.FromQuery<TSourceType>, source, query),
                    new Expression[] { source.Expression, Expression.Constant(query) }
                ));
        }

        /// <summary>
        /// Uses a URL to initialize the query object
        /// </summary>
        public static IDatastoreSliceable<TSourceType> FromQuery<TSourceType>(this IDatastoreSliceable<TSourceType> source, DataQuery query)
            where TSourceType : IDatastoreItem
        {
            if (source == null)
                throw new ArgumentNullException(ERROR_SOURCE);

            return source.Provider.CreateSliceQuery<TSourceType>(
                Expression.Call(
                    null,
                    GetMethodInfo(DatastoreExtensions.FromQuery<TSourceType>, source, query),
                    new Expression[] { source.Expression, Expression.Constant(query) }
                ));
        }

        #endregion

        #region RemoveFieldFilter

        /// <summary>
        /// Removes a specific field filter
        /// </summary>
        public static IDatastoreQueryable<TSourceType> RemoveFieldFilter<TSourceType, TKey>(this IDatastoreQueryable<TSourceType> source, Expression<Func<TSourceType, TKey>> member)
            where TSourceType : IDatastoreItem
        {
            if (source == null)
                throw new ArgumentNullException(ERROR_SOURCE);

            return source.Provider.CreateQuery<TSourceType>(
                Expression.Call(
                    null,
                    GetMethodInfo(DatastoreExtensions.RemoveFieldFilter, source, member),
                    new Expression[] { source.Expression, member }
                ));
        }

        /// <summary>
        /// Removes a specific field filter
        /// </summary>
        public static IDatastoreQueryable<TSourceType> RemoveFieldFilter<TSourceType>(this IDatastoreQueryable<TSourceType> source, string member)
            where TSourceType : IDatastoreItem
        {
            if (source == null)
                throw new ArgumentNullException(ERROR_SOURCE);

            return source.Provider.CreateQuery<TSourceType>(
                Expression.Call(
                    null,
                    GetMethodInfo(DatastoreExtensions.RemoveFieldFilter, source, member),
                    new Expression[] { source.Expression, Expression.Constant(member) }
                ));
        }

        #endregion

        #region RemoveSort

        /// <summary>
        /// Removes a specific field filter
        /// </summary>
        public static IDatastoreQueryable<TSourceType> RemoveSort<TSourceType, TKey>(this IDatastoreQueryable<TSourceType> source, Expression<Func<TSourceType, TKey>> member)
            where TSourceType : IDatastoreItem
        {
            if (source == null)
                throw new ArgumentNullException(ERROR_SOURCE);

            return source.Provider.CreateQuery<TSourceType>(
                Expression.Call(
                    null,
                    GetMethodInfo(DatastoreExtensions.RemoveSort, source, member),
                    new Expression[] { source.Expression, member }
                ));
        }

        /// <summary>
        /// Removes a specific field filter
        /// </summary>
        public static IDatastoreQueryable<TSourceType> RemoveSort<TSourceType>(this IDatastoreQueryable<TSourceType> source, string member)
            where TSourceType : IDatastoreItem
        {
            if (source == null)
                throw new ArgumentNullException(ERROR_SOURCE);

            return source.Provider.CreateQuery<TSourceType>(
                Expression.Call(
                    null,
                    GetMethodInfo(DatastoreExtensions.RemoveSort, source, member),
                    new Expression[] { source.Expression, Expression.Constant(member) }
                ));
        }

        #endregion

        #region WhereUser

        /// <summary />
        public static IDatastoreQueryable<TSourceType> WhereUser<TSourceType>(this IDatastoreQueryable<TSourceType> source, int userId)
            where TSourceType : IDatastoreItem
        {
            var users = new List<int> { userId };
            return WhereUser(source, users);
        }

        /// <summary />
        public static IDatastoreQueryable<TSourceType> WhereUser<TSourceType>(this IDatastoreQueryable<TSourceType> source, IEnumerable<int> users)
            where TSourceType : IDatastoreItem
        {
            if (source == null)
                throw new ArgumentNullException(ERROR_SOURCE);

            return source.Provider.CreateQuery<TSourceType>(
                Expression.Call(
                    null,
                    GetMethodInfo(DatastoreExtensions.WhereUser<TSourceType>, source, users),
                    new Expression[] { source.Expression, Expression.Constant(users) }
                ));
        }

        /// <summary />
        public static IDatastoreSliceable<TSourceType> WhereUser<TSourceType>(this IDatastoreSliceable<TSourceType> source, int userId)
            where TSourceType : IDatastoreItem
        {
            var users = new List<int> { userId };
            return WhereUser(source, users);
        }

        /// <summary />
        public static IDatastoreSliceable<TSourceType> WhereUser<TSourceType>(this IDatastoreSliceable<TSourceType> source, IEnumerable<int> users)
            where TSourceType : IDatastoreItem
        {
            if (source == null)
                throw new ArgumentNullException(ERROR_SOURCE);

            return source.Provider.CreateSliceQuery<TSourceType>(
                Expression.Call(
                    null,
                    GetMethodInfo(DatastoreExtensions.WhereUser<TSourceType>, source, users),
                    new Expression[] { source.Expression, Expression.Constant(users) }
                ));
        }

        #endregion

        #region WhereDimensionValue

        /// <summary />
        public static IDatastoreQueryable<TSourceType> WhereDimensionValue<TSourceType>(this IDatastoreQueryable<TSourceType> source, long value)
            where TSourceType : IDatastoreItem
        {
            var values = new List<long> { value };
            return WhereDimensionValue(source, values);
        }

        /// <summary />
        public static IDatastoreQueryable<TSourceType> WhereDimensionValue<TSourceType>(this IDatastoreQueryable<TSourceType> source, IEnumerable<long> values)
            where TSourceType : IDatastoreItem
        {
            if (source == null)
                throw new ArgumentNullException(ERROR_SOURCE);

            return source.Provider.CreateQuery<TSourceType>(
                Expression.Call(
                    null,
                    GetMethodInfo(DatastoreExtensions.WhereDimensionValue<TSourceType>, source, values),
                    new Expression[] { source.Expression, Expression.Constant(values) }
                ));
        }

        /// <summary />
        public static IDatastoreSliceable<TSourceType> WhereDimensionValue<TSourceType>(this IDatastoreSliceable<TSourceType> source, long value)
            where TSourceType : IDatastoreItem
        {
            var values = new List<long> { value };
            return WhereDimensionValue(source, values);
        }

        /// <summary />
        public static IDatastoreSliceable<TSourceType> WhereDimensionValue<TSourceType>(this IDatastoreSliceable<TSourceType> source, IEnumerable<long> values)
            where TSourceType : IDatastoreItem
        {
            if (source == null)
                throw new ArgumentNullException(ERROR_SOURCE);

            return source.Provider.CreateSliceQuery<TSourceType>(
                Expression.Call(
                    null,
                    GetMethodInfo(DatastoreExtensions.WhereDimensionValue<TSourceType>, source, values),
                    new Expression[] { source.Expression, Expression.Constant(values) }
                ));
        }

        #endregion

        #region RemoveDimensionValue

        /// <summary />
        public static IDatastoreQueryable<TSourceType> RemoveDimensionValue<TSourceType>(this IDatastoreQueryable<TSourceType> source, long value)
            where TSourceType : IDatastoreItem
        {
            var values = new List<long> { value };
            return RemoveDimensionValue(source, values);
        }

        /// <summary />
        public static IDatastoreQueryable<TSourceType> RemoveDimensionValue<TSourceType>(this IDatastoreQueryable<TSourceType> source, IEnumerable<long> values)
            where TSourceType : IDatastoreItem
        {
            if (source == null)
                throw new ArgumentNullException(ERROR_SOURCE);

            return source.Provider.CreateQuery<TSourceType>(
                Expression.Call(
                    null,
                    GetMethodInfo(DatastoreExtensions.RemoveDimensionValue<TSourceType>, source, values),
                    new Expression[] { source.Expression, Expression.Constant(values) }
                ));
        }

        #endregion

        #region SkipDimensions

        /// <summary>
        /// Specifies a dimension to exclude from calculation
        /// </summary>
        /// <returns>This is used when the refinement data for a particular dimension is not needed.</returns>
        public static IDatastoreQueryable<TSourceType> SkipDimension<TSourceType>(this IDatastoreQueryable<TSourceType> source, long value)
            where TSourceType : IDatastoreItem
        {
            var values = new List<long> { value };
            return SkipDimension(source, values);
        }

        /// <summary>
        /// Specifies an explcit list of dimensions to exclude from calculation
        /// </summary>
        /// <returns>This is used when the refinement data for particular dimensions are not needed.</returns>
        public static IDatastoreQueryable<TSourceType> SkipDimension<TSourceType>(this IDatastoreQueryable<TSourceType> source, IEnumerable<long> values)
            where TSourceType : IDatastoreItem
        {
            if (source == null)
                throw new ArgumentNullException(ERROR_SOURCE);

            return source.Provider.CreateQuery<TSourceType>(
                Expression.Call(
                    null,
                    GetMethodInfo(DatastoreExtensions.SkipDimension<TSourceType>, source, values),
                    new Expression[] { source.Expression, Expression.Constant(values) }
                ));
        }

        #endregion

        #region Where

        /// <summary>
        /// Applies a filter to narrow the results returned
        /// </summary>
        public static IDatastoreQueryable<TSourceType> Where<TSourceType>(this IDatastoreQueryable<TSourceType> source, Expression<Func<TSourceType, bool>> predicate)
            where TSourceType : IDatastoreItem
        {
            if (source == null)
                throw new ArgumentNullException(ERROR_SOURCE);
            if (predicate == null)
                throw new ArgumentNullException("predicate");
            return source.Provider.CreateQuery<TSourceType>(
                Expression.Call(
                    null,
                    GetMethodInfo(DatastoreExtensions.Where, source, predicate),
                    new Expression[] { source.Expression, Expression.Quote(predicate) }
                ));
        }

        /// <summary>
        /// Applies a filter to narrow the results returned
        /// </summary>
        public static IDatastoreUpdatable<TSourceType> Where<TSourceType>(this IDatastoreUpdatable<TSourceType> source, Expression<Func<TSourceType, bool>> predicate)
            where TSourceType : IDatastoreItem
        {
            if (source == null)
                throw new ArgumentNullException(ERROR_SOURCE);
            if (predicate == null)
                throw new ArgumentNullException("predicate");
            return source.Provider.CreateUpdateQuery<TSourceType>(
                Expression.Call(
                    null,
                    GetMethodInfo(DatastoreExtensions.Where, source, predicate),
                    new Expression[] { source.Expression, Expression.Quote(predicate) }
                ));
        }

        /// <summary>
        /// Applies a filter to narrow the results returned
        /// </summary>
        public static IDatastoreDeletable<TSourceType> Where<TSourceType>(this IDatastoreDeletable<TSourceType> source, Expression<Func<TSourceType, bool>> predicate)
            where TSourceType : IDatastoreItem
        {
            if (source == null)
                throw new ArgumentNullException(ERROR_SOURCE);
            if (predicate == null)
                throw new ArgumentNullException("predicate");
            return source.Provider.CreateDeleteQuery<TSourceType>(
                Expression.Call(
                    null,
                    GetMethodInfo(DatastoreExtensions.Where, source, predicate),
                    new Expression[] { source.Expression, Expression.Quote(predicate) }
                ));
        }

        /// <summary>
        /// Applies a filter to narrow the results returned
        /// </summary>
        public static IDatastoreSliceable<TSourceType> Where<TSourceType>(this IDatastoreSliceable<TSourceType> source, Expression<Func<TSourceType, bool>> predicate)
            where TSourceType : IDatastoreItem
        {
            if (source == null)
                throw new ArgumentNullException(ERROR_SOURCE);
            if (predicate == null)
                throw new ArgumentNullException("predicate");
            return source.Provider.CreateSliceQuery<TSourceType>(
                Expression.Call(
                    null,
                    GetMethodInfo(DatastoreExtensions.Where, source, predicate),
                    new Expression[] { source.Expression, Expression.Quote(predicate) }
                ));
        }

        #endregion

        #region Select

        /// <summary />
        public static IDatastoreQueryable<TNewResultType> Select<TSourceType, TNewResultType>(this IDatastoreQueryable<TSourceType> source, Expression<Func<TSourceType, TNewResultType>> selector)
            where TSourceType : IDatastoreItem
        {
            if (source == null)
                throw new ArgumentNullException(ERROR_SOURCE);
            if (selector == null)
                throw new ArgumentNullException(nameof(selector));

            return source.Provider.CreateQuery<TNewResultType>(
                Expression.Call(
                    null,
                    GetMethodInfo(DatastoreExtensions.Select, source, selector),
                    new Expression[] { source.Expression, Expression.Quote(selector) }
                ));
        }

        #endregion

        #region NonParsed

        /// <summary />
        public static IDatastoreQueryable<TSourceType> AddNonParsedField<TSourceType>(this IDatastoreQueryable<TSourceType> source, string field, string value)
            where TSourceType : IDatastoreItem
        {
            if (source == null)
                throw new ArgumentNullException(ERROR_SOURCE);

            return source.Provider.CreateQuery<TSourceType>(
                Expression.Call(
                    null,
                    GetMethodInfo(DatastoreExtensions.AddNonParsedField<TSourceType>, source, field, value),
                    new Expression[] { source.Expression, Expression.Constant(field), Expression.Constant(value) }
                ));
        }

        /// <summary />
        public static IDatastoreQueryable<TSourceType> RemoveNonParsedField<TSourceType>(this IDatastoreQueryable<TSourceType> source, string field)
            where TSourceType : IDatastoreItem
        {
            if (source == null)
                throw new ArgumentNullException(ERROR_SOURCE);

            return source.Provider.CreateQuery<TSourceType>(
                Expression.Call(
                    null,
                    GetMethodInfo(DatastoreExtensions.RemoveNonParsedField<TSourceType>, source, field),
                    new Expression[] { source.Expression, Expression.Constant(field) }
                ));
        }

        #endregion

        #region AddFieldFilter

        /// <summary />
        public static IDatastoreQueryable<TSourceType> AddFieldFilter<TSourceType>(this IDatastoreQueryable<TSourceType> source, IFieldFilter filter)
            where TSourceType : IDatastoreItem
        {
            if (source == null)
                throw new ArgumentNullException(ERROR_SOURCE);

            return source.Provider.CreateQuery<TSourceType>(
                Expression.Call(
                    null,
                    GetMethodInfo(DatastoreExtensions.AddFieldFilter<TSourceType>, source, filter),
                    new Expression[] { source.Expression, Expression.Constant(filter) }
                ));
        }

        /// <summary />
        public static IDatastoreSliceable<TSourceType> AddFieldFilter<TSourceType>(this IDatastoreSliceable<TSourceType> source, IFieldFilter filter)
            where TSourceType : IDatastoreItem
        {
            if (source == null)
                throw new ArgumentNullException(ERROR_SOURCE);

            return source.Provider.CreateSliceQuery<TSourceType>(
                Expression.Call(
                    null,
                    GetMethodInfo(DatastoreExtensions.AddFieldFilter<TSourceType>, source, filter),
                    new Expression[] { source.Expression, Expression.Constant(filter) }
                ));
        }

        #endregion

        #region ClearSorts

        /// <summary>
        /// Uses a URL to initialize the query object
        /// </summary>
        public static IDatastoreQueryable<TSourceType> ClearSorts<TSourceType>(this IDatastoreQueryable<TSourceType> source)
            where TSourceType : IDatastoreItem
        {
            if (source == null)
                throw new ArgumentNullException(ERROR_SOURCE);

            return source.Provider.CreateQuery<TSourceType>(
                Expression.Call(
                    null,
                    GetMethodInfo(DatastoreExtensions.ClearSorts<TSourceType>, source),
                    new Expression[] { source.Expression }
                ));
        }

        /// <summary>
        /// Uses a URL to initialize the query object
        /// </summary>
        public static IDatastoreSliceable<TSourceType> ClearSorts<TSourceType>(this IDatastoreSliceable<TSourceType> source)
            where TSourceType : IDatastoreItem
        {
            if (source == null)
                throw new ArgumentNullException(ERROR_SOURCE);

            return source.Provider.CreateSliceQuery<TSourceType>(
                Expression.Call(
                    null,
                    GetMethodInfo(DatastoreExtensions.ClearSorts<TSourceType>, source),
                    new Expression[] { source.Expression }
                ));
        }

        #endregion

        #region ClearFilters

        /// <summary>
        /// Uses a URL to initialize the query object
        /// </summary>
        public static IDatastoreQueryable<TSourceType> ClearFilters<TSourceType>(this IDatastoreQueryable<TSourceType> source)
            where TSourceType : IDatastoreItem
        {
            if (source == null)
                throw new ArgumentNullException(ERROR_SOURCE);

            return source.Provider.CreateQuery<TSourceType>(
                Expression.Call(
                    null,
                    GetMethodInfo(DatastoreExtensions.ClearFilters<TSourceType>, source),
                    new Expression[] { source.Expression }
                ));
        }

        /// <summary>
        /// Uses a URL to initialize the query object
        /// </summary>
        public static IDatastoreSliceable<TSourceType> ClearFilters<TSourceType>(this IDatastoreSliceable<TSourceType> source)
            where TSourceType : IDatastoreItem
        {
            if (source == null)
                throw new ArgumentNullException(ERROR_SOURCE);

            return source.Provider.CreateSliceQuery<TSourceType>(
                Expression.Call(
                    null,
                    GetMethodInfo(DatastoreExtensions.ClearFilters<TSourceType>, source),
                    new Expression[] { source.Expression }
                ));
        }

        #endregion

        #region ClearDimensions

        /// <summary>
        /// Uses a URL to initialize the query object
        /// </summary>
        public static IDatastoreQueryable<TSourceType> ClearDimensions<TSourceType>(this IDatastoreQueryable<TSourceType> source)
            where TSourceType : IDatastoreItem
        {
            if (source == null)
                throw new ArgumentNullException(ERROR_SOURCE);

            return source.Provider.CreateQuery<TSourceType>(
                Expression.Call(
                    null,
                    GetMethodInfo(DatastoreExtensions.ClearDimensions<TSourceType>, source),
                    new Expression[] { source.Expression }
                ));
        }

        #endregion

        #region OrderBy

        /// <summary>
        /// Orders the results by the specified field
        /// </summary>
        public static IDatastoreOrderedQueryable<TSourceType> OrderBy<TSourceType, TKey>(this IDatastoreQueryable<TSourceType> source, Expression<Func<TSourceType, TKey>> keySelector)
        {
            if (source == null)
                throw new ArgumentNullException(ERROR_SOURCE);
            if (keySelector == null)
                throw new ArgumentNullException(nameof(keySelector));
            return (IDatastoreOrderedQueryable<TSourceType>)source.Provider.CreateQuery<TSourceType>(
                Expression.Call(
                    null,
                    GetMethodInfo(DatastoreExtensions.OrderBy, source, keySelector),
                    new Expression[] { source.Expression, Expression.Quote(keySelector) }
                ));
        }

        /// <summary>
        /// Orders the results by the specified field
        /// </summary>
        public static IDatastoreOrderedQueryable<TSourceType> OrderBy<TSourceType, TKey>(this IDatastoreQueryable<TSourceType> source, Expression<Func<TSourceType, TKey>> keySelector, bool ascending)
        {
            if (ascending)
                return OrderBy(source, keySelector);

            return OrderByDescending(source, keySelector);
        }

        /// <summary>
        /// Orders the results by the specified field
        /// </summary>
        public static IDatastoreOrderedQueryable<TSourceType> OrderBy<TSourceType>(this IDatastoreQueryable<TSourceType> source, string keySelector)
        {
            if (source == null)
                throw new ArgumentNullException(ERROR_SOURCE);
            if (keySelector == null)
                throw new ArgumentNullException(nameof(keySelector));
            return (IDatastoreOrderedQueryable<TSourceType>)source.Provider.CreateQuery<TSourceType>(
                Expression.Call(
                    null,
                    GetMethodInfo(DatastoreExtensions.OrderBy<TSourceType>, source, keySelector),
                    new Expression[] { source.Expression, Expression.Constant(keySelector) }
                ));
        }

        /// <summary>
        /// Orders the results by the specified field
        /// </summary>
        public static IDatastoreOrderedQueryable<TSourceType> OrderBy<TSourceType>(this IDatastoreQueryable<TSourceType> source, string keySelector, bool ascending)
        {
            if (ascending)
                return OrderBy(source, keySelector);

            return OrderByDescending(source, keySelector);
        }

        /// <summary>
        /// Orders the results ascending by the specified field
        /// </summary>
        public static IDatastoreOrderedQueryable<TSourceType> ThenBy<TSourceType, TKey>(this IDatastoreQueryable<TSourceType> source, Expression<Func<TSourceType, TKey>> keySelector, bool ascending)
        {
            if (ascending)
                return ThenBy(source, keySelector);

            return ThenByDescending(source, keySelector);
        }

        /// <summary>
        /// Orders the results ascending by the specified field
        /// </summary>
        public static IDatastoreOrderedQueryable<TSourceType> ThenBy<TSourceType, TKey>(this IDatastoreQueryable<TSourceType> source, Expression<Func<TSourceType, TKey>> keySelector)
        {
            if (source == null)
                throw new ArgumentNullException(ERROR_SOURCE);
            if (keySelector == null)
                throw new ArgumentNullException(nameof(keySelector));
            return (IDatastoreOrderedQueryable<TSourceType>)source.Provider.CreateQuery<TSourceType>(
                Expression.Call(
                    null,
                    GetMethodInfo(DatastoreExtensions.ThenBy, source, keySelector),
                    new Expression[] { source.Expression, Expression.Quote(keySelector) }
                ));
        }

        /// <summary>
        /// Orders the results by the specified field
        /// </summary>
        public static IDatastoreOrderedSliceable<TSourceType> OrderBy<TSourceType, TKey>(this IDatastoreSliceable<TSourceType> source, Expression<Func<TSourceType, TKey>> keySelector)
        {
            if (source == null)
                throw new ArgumentNullException(ERROR_SOURCE);
            if (keySelector == null)
                throw new ArgumentNullException(nameof(keySelector));
            return (IDatastoreOrderedSliceable<TSourceType>)source.Provider.CreateSliceQuery<TSourceType>(
                Expression.Call(
                    null,
                    GetMethodInfo(DatastoreExtensions.OrderBy, source, keySelector),
                    new Expression[] { source.Expression, Expression.Quote(keySelector) }
                ));
        }

        /// <summary>
        /// Orders the results by the specified field
        /// </summary>
        public static IDatastoreOrderedSliceable<TSourceType> OrderBy<TSourceType, TKey>(this IDatastoreSliceable<TSourceType> source, Expression<Func<TSourceType, TKey>> keySelector, bool ascending)
        {
            if (ascending)
                return OrderBy(source, keySelector);

            return OrderByDescending(source, keySelector);
        }

        /// <summary>
        /// Orders the results by the specified field
        /// </summary>
        public static IDatastoreOrderedSliceable<TSourceType> OrderBy<TSourceType>(this IDatastoreSliceable<TSourceType> source, string keySelector)
        {
            if (source == null)
                throw new ArgumentNullException(ERROR_SOURCE);
            if (keySelector == null)
                throw new ArgumentNullException(nameof(keySelector));
            return (IDatastoreOrderedSliceable<TSourceType>)source.Provider.CreateSliceQuery<TSourceType>(
                Expression.Call(
                    null,
                    GetMethodInfo(DatastoreExtensions.OrderBy<TSourceType>, source, keySelector),
                    new Expression[] { source.Expression, Expression.Constant(keySelector) }
                ));
        }

        /// <summary>
        /// Orders the results by the specified field
        /// </summary>
        public static IDatastoreOrderedSliceable<TSourceType> OrderBy<TSourceType>(this IDatastoreSliceable<TSourceType> source, string keySelector, bool ascending)
        {
            if (ascending)
                return OrderBy(source, keySelector);

            return OrderByDescending(source, keySelector);
        }

        /// <summary>
        /// Orders the results ascending by the specified field
        /// </summary>
        public static IDatastoreOrderedSliceable<TSourceType> ThenBy<TSourceType, TKey>(this IDatastoreSliceable<TSourceType> source, Expression<Func<TSourceType, TKey>> keySelector, bool ascending)
        {
            if (ascending)
                return ThenBy(source, keySelector);

            return ThenByDescending(source, keySelector);
        }

        /// <summary>
        /// Orders the results ascending by the specified field
        /// </summary>
        public static IDatastoreOrderedSliceable<TSourceType> ThenBy<TSourceType, TKey>(this IDatastoreSliceable<TSourceType> source, Expression<Func<TSourceType, TKey>> keySelector)
        {
            if (source == null)
                throw new ArgumentNullException(ERROR_SOURCE);
            if (keySelector == null)
                throw new ArgumentNullException(nameof(keySelector));
            return (IDatastoreOrderedSliceable<TSourceType>)source.Provider.CreateSliceQuery<TSourceType>(
                Expression.Call(
                    null,
                    GetMethodInfo(DatastoreExtensions.ThenBy, source, keySelector),
                    new Expression[] { source.Expression, Expression.Quote(keySelector) }
                ));
        }

        #endregion

        #region OrderByDescending

        /// <summary>
        /// Orders the results descending by the specified field
        /// </summary>
        public static IDatastoreOrderedQueryable<TSourceType> OrderByDescending<TSourceType, TKey>(this IDatastoreQueryable<TSourceType> source, Expression<Func<TSourceType, TKey>> keySelector)
        {
            if (source == null)
                throw new ArgumentNullException(ERROR_SOURCE);
            if (keySelector == null)
                throw new ArgumentNullException(nameof(keySelector));
            return (IDatastoreOrderedQueryable<TSourceType>)source.Provider.CreateQuery<TSourceType>(
                Expression.Call(
                    null,
                    GetMethodInfo(DatastoreExtensions.OrderByDescending, source, keySelector),
                    new Expression[] { source.Expression, Expression.Quote(keySelector) }
                ));
        }

        /// <summary />
        public static IDatastoreOrderedQueryable<TSourceType> OrderByDescending<TSourceType>(this IDatastoreQueryable<TSourceType> source, string keySelector)
        {
            if (source == null)
                throw new ArgumentNullException(ERROR_SOURCE);
            if (keySelector == null)
                throw new ArgumentNullException(nameof(keySelector));
            return (IDatastoreOrderedQueryable<TSourceType>)source.Provider.CreateQuery<TSourceType>(
                Expression.Call(
                    null,
                    GetMethodInfo(DatastoreExtensions.OrderByDescending<TSourceType>, source, keySelector),
                    new Expression[] { source.Expression, Expression.Constant(keySelector) }
                ));
        }

        /// <summary>
        /// Orders the results descending by the specified field
        /// </summary>
        public static IDatastoreOrderedQueryable<TSourceType> ThenByDescending<TSourceType, TKey>(this IDatastoreQueryable<TSourceType> source, Expression<Func<TSourceType, TKey>> keySelector)
        {
            if (source == null)
                throw new ArgumentNullException(ERROR_SOURCE);
            if (keySelector == null)
                throw new ArgumentNullException(nameof(keySelector));
            return (IDatastoreOrderedQueryable<TSourceType>)source.Provider.CreateQuery<TSourceType>(
                Expression.Call(
                    null,
                    GetMethodInfo(DatastoreExtensions.ThenByDescending, source, keySelector),
                    new Expression[] { source.Expression, Expression.Quote(keySelector) }
                ));
        }

        /// <summary>
        /// Orders the results descending by the specified field
        /// </summary>
        public static IDatastoreOrderedSliceable<TSourceType> OrderByDescending<TSourceType, TKey>(this IDatastoreSliceable<TSourceType> source, Expression<Func<TSourceType, TKey>> keySelector)
        {
            if (source == null)
                throw new ArgumentNullException(ERROR_SOURCE);
            if (keySelector == null)
                throw new ArgumentNullException(nameof(keySelector));
            return (IDatastoreOrderedSliceable<TSourceType>)source.Provider.CreateSliceQuery<TSourceType>(
                Expression.Call(
                    null,
                    GetMethodInfo(DatastoreExtensions.OrderByDescending, source, keySelector),
                    new Expression[] { source.Expression, Expression.Quote(keySelector) }
                ));
        }

        /// <summary />
        public static IDatastoreOrderedSliceable<TSourceType> OrderByDescending<TSourceType>(this IDatastoreSliceable<TSourceType> source, string keySelector)
        {
            if (source == null)
                throw new ArgumentNullException(ERROR_SOURCE);
            if (keySelector == null)
                throw new ArgumentNullException(nameof(keySelector));
            return (IDatastoreOrderedSliceable<TSourceType>)source.Provider.CreateSliceQuery<TSourceType>(
                Expression.Call(
                    null,
                    GetMethodInfo(DatastoreExtensions.OrderByDescending<TSourceType>, source, keySelector),
                    new Expression[] { source.Expression, Expression.Constant(keySelector) }
                ));
        }

        /// <summary>
        /// Orders the results descending by the specified field
        /// </summary>
        public static IDatastoreOrderedSliceable<TSourceType> ThenByDescending<TSourceType, TKey>(this IDatastoreSliceable<TSourceType> source, Expression<Func<TSourceType, TKey>> keySelector)
        {
            if (source == null)
                throw new ArgumentNullException(ERROR_SOURCE);
            if (keySelector == null)
                throw new ArgumentNullException(nameof(keySelector));
            return (IDatastoreOrderedSliceable<TSourceType>)source.Provider.CreateSliceQuery<TSourceType>(
                Expression.Call(
                    null,
                    GetMethodInfo(DatastoreExtensions.ThenByDescending, source, keySelector),
                    new Expression[] { source.Expression, Expression.Quote(keySelector) }
                ));
        }

        #endregion

        #region Count

        /// <summary />
        public static int Count<TSourceType>(this IDatastoreQueryable<TSourceType> source)
        {
            if (source == null)
                throw new ArgumentNullException(ERROR_SOURCE);

            try
            {
                int retval = 0;
                RetryHelper.DefaultRetryPolicy(FailoverConfiguration.RetryOnFailCount)
                    .Execute(() =>
                    {
                        retval = source.Provider.Execute<int>(
                            Expression.Call(
                                null,
                                GetMethodInfo(DatastoreExtensions.Count, source),
                                new Expression[] { source.Expression }
                            ));
                    });
                return retval;
            }
            catch (Exception ex)
            {
                if (ex is System.ServiceModel.CommunicationObjectFaultedException ||
                    ex is System.ServiceModel.EndpointNotFoundException ||
                    ex is System.ServiceModel.CommunicationException)
                {
                    if (FailoverConfiguration.IsConfigured)
                    {
                        if (FailoverConfiguration.TryFailOver())
                            throw new Exceptions.FailoverException();
                    }
                }
                throw;
            }
        }

        #endregion

        #region Results

        /// <summary />
        public static DatastoreResults<TSourceType> Results<TSourceType>(this IDatastoreQueryable<TSourceType> source)
            where TSourceType : IDatastoreItem
        {
            if (source == null)
                throw new ArgumentNullException(ERROR_SOURCE);

            try
            {
                DatastoreResults<TSourceType> retval = null;
                RetryHelper.DefaultRetryPolicy(FailoverConfiguration.RetryOnFailCount)
                    .Execute(() =>
                    {
                        retval = (DatastoreResults<TSourceType>)source.Provider.Execute<DatastoreResults<TSourceType>>(
                            Expression.Call(
                                null,
                                GetMethodInfo(DatastoreExtensions.Results<TSourceType>, source),
                                new Expression[] { source.Expression }
                            ));
                    });
                return retval;
            }
            catch (Exception ex)
            {
                if (ex is System.ServiceModel.CommunicationObjectFaultedException ||
                    ex is System.ServiceModel.EndpointNotFoundException ||
                    ex is System.ServiceModel.CommunicationException)
                {
                    if (FailoverConfiguration.IsConfigured)
                    {
                        if (FailoverConfiguration.TryFailOver())
                            throw new Exceptions.FailoverException();
                    }
                }
                throw;
            }
        }

        /// <summary />
        public static DatastoreResultsAsync<TSourceType> ResultsAsync<TSourceType>(this IDatastoreQueryable<TSourceType> source)
            where TSourceType : IDatastoreItem, new()
        {
            if (source == null)
                throw new ArgumentNullException(ERROR_SOURCE);

            try
            {
                DatastoreResultsAsync<TSourceType> retval = null;
                RetryHelper.DefaultRetryPolicy(FailoverConfiguration.RetryOnFailCount)
                    .Execute(() =>
                    {
                        retval = (DatastoreResultsAsync<TSourceType>)source.Provider.Execute<DatastoreResultsAsync<TSourceType>>(
                            Expression.Call(
                                null,
                                GetMethodInfo(DatastoreExtensions.ResultsAsync<TSourceType>, source),
                                new Expression[] { source.Expression }
                            ));
                    });
                return retval;
            }
            catch (Exception ex)
            {
                if (ex is System.ServiceModel.CommunicationObjectFaultedException ||
                    ex is System.ServiceModel.EndpointNotFoundException ||
                    ex is System.ServiceModel.CommunicationException)
                {
                    if (FailoverConfiguration.IsConfigured)
                    {
                        if (FailoverConfiguration.TryFailOver())
                            throw new Exceptions.FailoverException();
                    }
                }
                throw;
            }
        }

        #endregion

        #region ToUrl

        /// <summary />
        public static string ToUrl<TSourceType>(this IDatastoreQueryable<TSourceType> source)
            where TSourceType : IDatastoreItem
        {
            if (source == null)
                throw new ArgumentNullException(ERROR_SOURCE);

            string retval = null;
            RetryHelper.DefaultRetryPolicy(FailoverConfiguration.RetryOnFailCount)
                .Execute(() =>
                {
                    retval = source.Provider.Execute<string>(
                        Expression.Call(
                            null,
                            GetMethodInfo(DatastoreExtensions.ToUrl<TSourceType>, source),
                            new Expression[] { source.Expression }
                        ));
                });
            return retval;
        }

        #endregion

        #region ToQuery

        /// <summary />
        public static DataQuery ToQuery<TSourceType>(this IDatastoreQueryable<TSourceType> source)
            where TSourceType : IDatastoreItem
        {
            if (source == null)
                throw new ArgumentNullException(ERROR_SOURCE);

            DataQuery retval = null;
            RetryHelper.DefaultRetryPolicy(FailoverConfiguration.RetryOnFailCount)
                .Execute(() =>
                {
                    retval = source.Provider.Execute<DataQuery>(
                        Expression.Call(
                            null,
                            GetMethodInfo(DatastoreExtensions.ToQuery<TSourceType>, source),
                            new Expression[] { source.Expression }
                        ));
                });
            return retval;
        }

        #endregion

        #region Aggregates

        /// <summary />
        public static TResult Min<TResult, TSourceType>(this IDatastoreQueryable<TSourceType> source, Expression<Func<TSourceType, TResult>> selector)
            where TSourceType : IDatastoreItem
        {
            var q = AggregateMin(source, selector);
            return q.AggregateExecuteMin();
        }

        /// <summary />
        public static TResult Max<TSourceType, TResult>(this IDatastoreQueryable<TSourceType> source, Expression<Func<TSourceType, TResult>> selector)
            where TSourceType : IDatastoreItem
        {
            var q = AggregateMax(source, selector);
            return q.AggregateExecuteMax();
        }

        /// <summary />
        public static TResult Count<TSourceType, TResult>(this IDatastoreQueryable<TSourceType> source, Expression<Func<TSourceType, TResult>> selector)
            where TSourceType : IDatastoreItem
        {
            var q = AggregateCount(source, selector);
            return q.AggregateExecuteCount();
        }

        /// <summary />
        public static TResult CountDistinct<TSourceType, TResult>(this IDatastoreQueryable<TSourceType> source, Expression<Func<TSourceType, TResult>> selector)
            where TSourceType : IDatastoreItem
        {
            var q = AggregateCountDistinct(source, selector);
            return q.AggregateExecuteCountDistinct();
        }

        /// <summary />
        public static TResult Sum<TSourceType, TResult>(this IDatastoreQueryable<TSourceType> source, Expression<Func<TSourceType, TResult>> selector)
            where TSourceType : IDatastoreItem
        {
            var q = AggregateSum(source, selector);
            return q.AggregateExecuteSum();
        }

        /// <summary />
        public static TNewResultType Aggregate<TSourceType, TNewResultType>(this IDatastoreQueryable<TSourceType> source, Expression<Func<TSourceType, TNewResultType>> selector)
            where TSourceType : IDatastoreItem
        {
            var q = AggregateSelect(source, selector);
            return q.AggregateExecute();
        }

        private static IDatastoreQueryable<TNewResultType> AggregateSelect<TSourceType, TNewResultType>(this IDatastoreQueryable<TSourceType> source, Expression<Func<TSourceType, TNewResultType>> selector)
            where TSourceType : IDatastoreItem
        {
            if (source == null)
                throw new ArgumentNullException(ERROR_SOURCE);
            if (selector == null)
                throw new ArgumentNullException(nameof(selector));

            return source.Provider.CreateQuery<TNewResultType>(
                Expression.Call(
                    null,
                    GetMethodInfo(DatastoreExtensions.AggregateSelect, source, selector),
                    new Expression[] { source.Expression, Expression.Quote(selector) }
                ));
        }

        private static IDatastoreQueryable<TNewResultType> AggregateMin<TSourceType, TNewResultType>(this IDatastoreQueryable<TSourceType> source, Expression<Func<TSourceType, TNewResultType>> selector)
            where TSourceType : IDatastoreItem
        {
            if (source == null)
                throw new ArgumentNullException(ERROR_SOURCE);
            if (selector == null)
                throw new ArgumentNullException(nameof(selector));

            return source.Provider.CreateQuery<TNewResultType>(
                Expression.Call(
                    null,
                    GetMethodInfo(DatastoreExtensions.AggregateMin, source, selector),
                    new Expression[] { source.Expression, Expression.Quote(selector) }
                ));
        }

        private static IDatastoreQueryable<TNewResultType> AggregateMax<TSourceType, TNewResultType>(this IDatastoreQueryable<TSourceType> source, Expression<Func<TSourceType, TNewResultType>> selector)
            where TSourceType : IDatastoreItem
        {
            if (source == null)
                throw new ArgumentNullException(ERROR_SOURCE);
            if (selector == null)
                throw new ArgumentNullException(nameof(selector));

            return source.Provider.CreateQuery<TNewResultType>(
                Expression.Call(
                    null,
                    GetMethodInfo(DatastoreExtensions.AggregateMax, source, selector),
                    new Expression[] { source.Expression, Expression.Quote(selector) }
                ));
        }

        private static IDatastoreQueryable<TNewResultType> AggregateCount<TSourceType, TNewResultType>(this IDatastoreQueryable<TSourceType> source, Expression<Func<TSourceType, TNewResultType>> selector)
            where TSourceType : IDatastoreItem
        {
            if (source == null)
                throw new ArgumentNullException(ERROR_SOURCE);
            if (selector == null)
                throw new ArgumentNullException(nameof(selector));

            return source.Provider.CreateQuery<TNewResultType>(
                Expression.Call(
                    null,
                    GetMethodInfo(DatastoreExtensions.AggregateCount, source, selector),
                    new Expression[] { source.Expression, Expression.Quote(selector) }
                ));
        }

        private static IDatastoreQueryable<TNewResultType> AggregateCountDistinct<TSourceType, TNewResultType>(this IDatastoreQueryable<TSourceType> source, Expression<Func<TSourceType, TNewResultType>> selector)
            where TSourceType : IDatastoreItem
        {
            if (source == null)
                throw new ArgumentNullException(ERROR_SOURCE);
            if (selector == null)
                throw new ArgumentNullException(nameof(selector));

            return source.Provider.CreateQuery<TNewResultType>(
                Expression.Call(
                    null,
                    GetMethodInfo(DatastoreExtensions.AggregateCountDistinct, source, selector),
                    new Expression[] { source.Expression, Expression.Quote(selector) }
                ));
        }

        private static IDatastoreQueryable<TNewResultType> AggregateSum<TSourceType, TNewResultType>(this IDatastoreQueryable<TSourceType> source, Expression<Func<TSourceType, TNewResultType>> selector)
            where TSourceType : IDatastoreItem
        {
            if (source == null)
                throw new ArgumentNullException(ERROR_SOURCE);
            if (selector == null)
                throw new ArgumentNullException(nameof(selector));

            return source.Provider.CreateQuery<TNewResultType>(
                Expression.Call(
                    null,
                    GetMethodInfo(DatastoreExtensions.AggregateSum, source, selector),
                    new Expression[] { source.Expression, Expression.Quote(selector) }
                ));
        }

        private static TSourceType AggregateExecute<TSourceType>(this IDatastoreQueryable<TSourceType> source)
        {
            if (source == null)
                throw new ArgumentNullException(ERROR_SOURCE);

            try
            {
                var retval = default(TSourceType);
                RetryHelper.DefaultRetryPolicy(FailoverConfiguration.RetryOnFailCount)
                    .Execute(() =>
                    {
                        retval = source.Provider.Execute<TSourceType>(
                            Expression.Call(
                                null,
                                GetMethodInfo(DatastoreExtensions.AggregateExecute<TSourceType>, source),
                                new Expression[] { source.Expression }
                            ));
                    });
                return retval;
            }
            catch (Exception ex)
            {
                if (ex is System.ServiceModel.CommunicationObjectFaultedException ||
                    ex is System.ServiceModel.EndpointNotFoundException ||
                    ex is System.ServiceModel.CommunicationException)
                {
                    if (FailoverConfiguration.IsConfigured)
                    {
                        if (FailoverConfiguration.TryFailOver())
                            throw new Exceptions.FailoverException();
                    }
                }
                throw;
            }
        }

        private static TSourceType AggregateExecuteMin<TSourceType>(this IDatastoreQueryable<TSourceType> source)
        {
            if (source == null)
                throw new ArgumentNullException(ERROR_SOURCE);

            try
            {
                var retval = default(TSourceType);
                RetryHelper.DefaultRetryPolicy(FailoverConfiguration.RetryOnFailCount)
                    .Execute(() =>
                    {
                        retval = source.Provider.Execute<TSourceType>(
                            Expression.Call(
                                null,
                                GetMethodInfo(DatastoreExtensions.AggregateExecuteMin<TSourceType>, source),
                                new Expression[] { source.Expression }
                            ));
                    });
                return retval;
            }
            catch (Exception ex)
            {
                if (ex is System.ServiceModel.CommunicationObjectFaultedException ||
                    ex is System.ServiceModel.EndpointNotFoundException ||
                    ex is System.ServiceModel.CommunicationException)
                {
                    if (FailoverConfiguration.IsConfigured)
                    {
                        if (FailoverConfiguration.TryFailOver())
                            throw new Exceptions.FailoverException();
                    }
                }
                throw;
            }
        }

        private static TSourceType AggregateExecuteMax<TSourceType>(this IDatastoreQueryable<TSourceType> source)
        {
            if (source == null)
                throw new ArgumentNullException(ERROR_SOURCE);

            try
            {
                var retval = default(TSourceType);
                RetryHelper.DefaultRetryPolicy(FailoverConfiguration.RetryOnFailCount)
                    .Execute(() =>
                    {
                        retval = source.Provider.Execute<TSourceType>(
                            Expression.Call(
                                null,
                                GetMethodInfo(DatastoreExtensions.AggregateExecuteMax<TSourceType>, source),
                                new Expression[] { source.Expression }
                            ));
                    });
                return retval;
            }
            catch (Exception ex)
            {
                if (ex is System.ServiceModel.CommunicationObjectFaultedException ||
                    ex is System.ServiceModel.EndpointNotFoundException ||
                    ex is System.ServiceModel.CommunicationException)
                {
                    if (FailoverConfiguration.IsConfigured)
                    {
                        if (FailoverConfiguration.TryFailOver())
                            throw new Exceptions.FailoverException();
                    }
                }
                throw;
            }
        }

        private static TSourceType AggregateExecuteCount<TSourceType>(this IDatastoreQueryable<TSourceType> source)
        {
            if (source == null)
                throw new ArgumentNullException(ERROR_SOURCE);

            try
            {
                var retval = default(TSourceType);
                RetryHelper.DefaultRetryPolicy(FailoverConfiguration.RetryOnFailCount)
                    .Execute(() =>
                    {
                        retval = source.Provider.Execute<TSourceType>(
                            Expression.Call(
                                null,
                                GetMethodInfo(DatastoreExtensions.AggregateExecuteCount<TSourceType>, source),
                                new Expression[] { source.Expression }
                            ));
                    });
                return retval;
            }
            catch (Exception ex)
            {
                if (ex is System.ServiceModel.CommunicationObjectFaultedException ||
                    ex is System.ServiceModel.EndpointNotFoundException ||
                    ex is System.ServiceModel.CommunicationException)
                {
                    if (FailoverConfiguration.IsConfigured)
                    {
                        if (FailoverConfiguration.TryFailOver())
                            throw new Exceptions.FailoverException();
                    }
                }
                throw;
            }
        }

        private static TSourceType AggregateExecuteCountDistinct<TSourceType>(this IDatastoreQueryable<TSourceType> source)
        {
            if (source == null)
                throw new ArgumentNullException(ERROR_SOURCE);

            try
            {
                var retval = default(TSourceType);
                RetryHelper.DefaultRetryPolicy(FailoverConfiguration.RetryOnFailCount)
                    .Execute(() =>
                    {
                        retval = source.Provider.Execute<TSourceType>(
                            Expression.Call(
                                null,
                                GetMethodInfo(DatastoreExtensions.AggregateExecuteCountDistinct<TSourceType>, source),
                                new Expression[] { source.Expression }
                            ));
                    });
                return retval;
            }
            catch (Exception ex)
            {
                if (ex is System.ServiceModel.CommunicationObjectFaultedException ||
                    ex is System.ServiceModel.EndpointNotFoundException ||
                    ex is System.ServiceModel.CommunicationException)
                {
                    if (FailoverConfiguration.IsConfigured)
                    {
                        if (FailoverConfiguration.TryFailOver())
                            throw new Exceptions.FailoverException();
                    }
                }
                throw;
            }
        }

        private static TSourceType AggregateExecuteSum<TSourceType>(this IDatastoreQueryable<TSourceType> source)
        {
            if (source == null)
                throw new ArgumentNullException(ERROR_SOURCE);

            try
            {
                var retval = default(TSourceType);
                RetryHelper.DefaultRetryPolicy(FailoverConfiguration.RetryOnFailCount)
                    .Execute(() =>
                    {
                        retval = source.Provider.Execute<TSourceType>(
                            Expression.Call(
                                null,
                                GetMethodInfo(DatastoreExtensions.AggregateExecuteSum<TSourceType>, source),
                                new Expression[] { source.Expression }
                            ));
                    });
                return retval;
            }
            catch (Exception ex)
            {
                if (ex is System.ServiceModel.CommunicationObjectFaultedException ||
                    ex is System.ServiceModel.EndpointNotFoundException ||
                    ex is System.ServiceModel.CommunicationException)
                {
                    if (FailoverConfiguration.IsConfigured)
                    {
                        if (FailoverConfiguration.TryFailOver())
                            throw new Exceptions.FailoverException();
                    }
                }
                throw;
            }
        }

        #endregion

        #region DerivedFields

        /// <summary />
        public static IDatastoreQueryable<TSourceType> FieldMin<TSourceType, TKey>(this IDatastoreQueryable<TSourceType> source, Expression<Func<TSourceType, TKey>> member)
            where TSourceType : IDatastoreItem
        {
            if (source == null)
                throw new ArgumentNullException(ERROR_SOURCE);
            return source.Provider.CreateQuery<TSourceType>(
                Expression.Call(
                    null,
                    GetMethodInfo(DatastoreExtensions.FieldMin, source, member),
                    new Expression[] { source.Expression, member }
                ));
        }

        /// <summary />
        public static IDatastoreQueryable<TSourceType> FieldMax<TSourceType, TKey>(this IDatastoreQueryable<TSourceType> source, Expression<Func<TSourceType, TKey>> member)
            where TSourceType : IDatastoreItem
        {
            if (source == null)
                throw new ArgumentNullException(ERROR_SOURCE);
            return source.Provider.CreateQuery<TSourceType>(
                Expression.Call(
                    null,
                    GetMethodInfo(DatastoreExtensions.FieldMax, source, member),
                    new Expression[] { source.Expression, member }
                ));
        }

        /// <summary />
        public static IDatastoreQueryable<TSourceType> FieldCount<TSourceType, TKey>(this IDatastoreQueryable<TSourceType> source, Expression<Func<TSourceType, TKey>> member)
            where TSourceType : IDatastoreItem
        {
            if (source == null)
                throw new ArgumentNullException(ERROR_SOURCE);
            return source.Provider.CreateQuery<TSourceType>(
                Expression.Call(
                    null,
                    GetMethodInfo(DatastoreExtensions.FieldCount, source, member),
                    new Expression[] { source.Expression, member }
                ));
        }

        /// <summary />
        public static IDatastoreQueryable<TSourceType> FieldCountDistinct<TSourceType, TKey>(this IDatastoreQueryable<TSourceType> source, Expression<Func<TSourceType, TKey>> member)
            where TSourceType : IDatastoreItem
        {
            if (source == null)
                throw new ArgumentNullException(ERROR_SOURCE);
            return source.Provider.CreateQuery<TSourceType>(
                Expression.Call(
                    null,
                    GetMethodInfo(DatastoreExtensions.FieldCountDistinct, source, member),
                    new Expression[] { source.Expression, member }
                ));
        }

        /// <summary />
        public static IDatastoreQueryable<TSourceType> FieldSum<TSourceType, TKey>(this IDatastoreQueryable<TSourceType> source, Expression<Func<TSourceType, TKey>> member)
            where TSourceType : IDatastoreItem
        {
            if (source == null)
                throw new ArgumentNullException(ERROR_SOURCE);

            return source.Provider.CreateQuery<TSourceType>(
                Expression.Call(
                    null,
                    GetMethodInfo(DatastoreExtensions.FieldSum, source, member),
                    new Expression[] { source.Expression, member }
                ));
        }

        #endregion

        #region Items

        /// <summary>
        /// Gets a list of paginated items excluding all dimension data
        /// </summary>
        public static DatastoreItems<TSourceType> Items<TSourceType>(this IDatastoreQueryable<TSourceType> source)
        {
            if (source == null)
                throw new ArgumentNullException(ERROR_SOURCE);

            try
            {
                DatastoreItems<TSourceType> retval = null;
                RetryHelper.DefaultRetryPolicy(FailoverConfiguration.RetryOnFailCount)
                    .Execute(() =>
                    {
                        retval = source.Provider.Execute<DatastoreItems<TSourceType>>(
                            Expression.Call(
                                null,
                                GetMethodInfo(DatastoreExtensions.Items<TSourceType>, source),
                                new Expression[] { source.Expression }
                            ));
                    });
                return retval;
            }
            catch (Exception ex)
            {
                if (ex is System.ServiceModel.CommunicationObjectFaultedException ||
                    ex is System.ServiceModel.EndpointNotFoundException ||
                    ex is System.ServiceModel.CommunicationException)
                {
                    if (FailoverConfiguration.IsConfigured)
                    {
                        if (FailoverConfiguration.TryFailOver())
                            throw new Exceptions.FailoverException();
                    }
                }
                throw;
            }
        }

        /// <summary />
        public static IDatastoreGroupable<IDatastoreGrouping<TKey, TSourceType>> GroupBy<TSourceType, TKey>(this IDatastoreQueryable<TSourceType> source, Expression<Func<TSourceType, TKey>> member)
            where TSourceType : IDatastoreItem
        {
            if (source == null)
                throw new ArgumentNullException(ERROR_SOURCE);

            try
            {
                var retval = source.Provider.CreateGroupingQuery<TKey, TSourceType>(
                    Expression.Call(
                        null,
                        GetMethodInfo(DatastoreExtensions.GroupBy, source, member),
                        new Expression[] { source.Expression, member }
                    ));
                return retval;
            }
            catch (Exception ex)
            {
                if (ex is System.ServiceModel.CommunicationObjectFaultedException ||
                    ex is System.ServiceModel.EndpointNotFoundException ||
                    ex is System.ServiceModel.CommunicationException)
                {
                    if (FailoverConfiguration.IsConfigured)
                    {
                        if (FailoverConfiguration.TryFailOver())
                            throw new Exceptions.FailoverException();
                    }
                }
                throw;
            }
        }

        /// <summary />
        public static IDatastoreGroupable<TResult2> Select<TSourceType, TResult, TResult2>(this IDatastoreGroupable<IDatastoreGrouping<TResult, TSourceType>> source, Expression<Func<IDatastoreGrouping<TResult, TSourceType>, TResult2>> selector)
            where TSourceType : IDatastoreItem
        {
            if (source == null)
                throw new ArgumentNullException(ERROR_SOURCE);
            if (selector == null)
                throw new ArgumentNullException(nameof(selector));

            try
            {
                return source.Provider.CreateGroupingSelectQuery<TResult, TResult2>(
                            Expression.Call(
                            null,
                            GetMethodInfo(DatastoreExtensions.Select, source, selector),
                            new Expression[] { source.Expression, selector }
                    ));
            }
            catch (Exception ex)
            {
                if (ex is System.ServiceModel.CommunicationObjectFaultedException ||
                    ex is System.ServiceModel.EndpointNotFoundException ||
                    ex is System.ServiceModel.CommunicationException)
                {
                    if (FailoverConfiguration.IsConfigured)
                    {
                        if (FailoverConfiguration.TryFailOver())
                            throw new Exceptions.FailoverException();
                    }
                }
                throw;
            }
        }

        /// <summary>
        /// Gets a list of paginated items excluding all dimension data
        /// </summary>
        public static DatastoreItems<TSourceType> Items<TSourceType>(this IDatastoreGroupable<TSourceType> source)
        {
            if (source == null)
                throw new ArgumentNullException(ERROR_SOURCE);

            try
            {
                DatastoreItems<TSourceType> retval = null;
                RetryHelper.DefaultRetryPolicy(FailoverConfiguration.RetryOnFailCount)
                    .Execute(() =>
                    {
                        retval = source.Provider.Execute<DatastoreItems<TSourceType>>(
                            Expression.Call(
                                null,
                                GetMethodInfo(DatastoreExtensions.Items, source),
                                new Expression[] { source.Expression }
                            ));
                    });
                return retval;
            }
            catch (Exception ex)
            {
                if (ex is System.ServiceModel.CommunicationObjectFaultedException ||
                    ex is System.ServiceModel.EndpointNotFoundException ||
                    ex is System.ServiceModel.CommunicationException)
                {
                    if (FailoverConfiguration.IsConfigured)
                    {
                        if (FailoverConfiguration.TryFailOver())
                            throw new Exceptions.FailoverException();
                    }
                }
                throw;
            }
        }

        #endregion

        #region AllDimensions

        /// <summary>
        /// Gets the set of available dimensions based on the specified query
        /// </summary>
        public static DimensionStore<TSourceType> AllDimensions<TSourceType>(this IDatastoreQueryable<TSourceType> source)
            where TSourceType : IDatastoreItem
        {
            if (source == null)
                throw new ArgumentNullException(ERROR_SOURCE);

            try
            {
                DimensionStore<TSourceType> retval = null;
                RetryHelper.DefaultRetryPolicy(FailoverConfiguration.RetryOnFailCount)
                    .Execute(() =>
                    {
                        retval = source.Provider.Execute<DimensionStore<TSourceType>>(
                            Expression.Call(
                                null,
                                GetMethodInfo(DatastoreExtensions.AllDimensions<TSourceType>, source),
                                new Expression[] { source.Expression }
                            ));
                    });
                return retval;
            }
            catch (Exception ex)
            {
                if (ex is System.ServiceModel.CommunicationObjectFaultedException ||
                    ex is System.ServiceModel.EndpointNotFoundException ||
                    ex is System.ServiceModel.CommunicationException)
                {
                    if (FailoverConfiguration.IsConfigured)
                    {
                        if (FailoverConfiguration.TryFailOver())
                            throw new Exceptions.FailoverException();
                    }
                }
                throw;
            }
        }

        #endregion

        #region Field

        /// <summary />
        public static IDatastoreUpdatable<TSourceType> Field<TSourceType, TPropertyType>(this IDatastoreUpdatable<TSourceType> source, Expression<Func<TSourceType, TPropertyType>> member, TPropertyType value)
            where TSourceType : IDatastoreItem
        {
            if (source == null)
                throw new ArgumentNullException(ERROR_SOURCE);

            return source.Provider.CreateUpdateQuery<TSourceType>(
                Expression.Call(
                    null,
                    GetMethodInfo(DatastoreExtensions.Field, source, member, value),
                    new Expression[] { source.Expression, member, Expression.Constant(value, typeof(TPropertyType)) }
                ));
        }

        /// <summary />
        public static IDatastoreUpdatable<TSourceType> Field<TSourceType, TPropertyType>(this IDatastoreUpdatable<TSourceType> source, string member, TPropertyType value)
            where TSourceType : IDatastoreItem
        {
            if (source == null)
                throw new ArgumentNullException(ERROR_SOURCE);

            return source.Provider.CreateUpdateQuery<TSourceType>(
                Expression.Call(
                    null,
                    GetMethodInfo(DatastoreExtensions.Field, source, member, value),
                    new Expression[] { source.Expression, Expression.Constant(member), Expression.Constant(value, typeof(TPropertyType)) }
                ));
        }

        #endregion

        #region Commit

        /// <summary>
        /// Submits the queued actions to the repository
        /// </summary>
        public static ActionDiagnostics Commit<TSourceType>(this IDatastoreUpdatable<TSourceType> source)
        {
            if (source == null)
                throw new ArgumentNullException(ERROR_SOURCE);

            try
            {
                ActionDiagnostics retval = null;
                RetryHelper.DefaultRetryPolicy(FailoverConfiguration.RetryOnFailCount)
                    .Execute(() =>
                    {
                        retval = source.Provider.Execute<ActionDiagnostics>(
                            Expression.Call(
                                null,
                                GetMethodInfo(DatastoreExtensions.Commit<TSourceType>, source),
                                new Expression[] { source.Expression }
                            ));
                    });
                return retval;
            }
            catch (Exception ex)
            {
                if (ex is System.ServiceModel.CommunicationObjectFaultedException ||
                    ex is System.ServiceModel.EndpointNotFoundException ||
                    ex is System.ServiceModel.CommunicationException)
                {
                    if (FailoverConfiguration.IsConfigured)
                    {
                        if (FailoverConfiguration.TryFailOver())
                            throw new Exceptions.FailoverException();
                    }
                }
                throw;
            }
        }

        /// <summary>
        /// Submits the queued actions to the repository
        /// </summary>
        public static ActionDiagnostics Commit<TSourceType>(this IDatastoreDeletable<TSourceType> source)
        {
            if (source == null)
                throw new ArgumentNullException(ERROR_SOURCE);

            try
            {
                ActionDiagnostics retval = null;
                RetryHelper.DefaultRetryPolicy(FailoverConfiguration.RetryOnFailCount)
                    .Execute(() =>
                    {
                        retval = source.Provider.Execute<ActionDiagnostics>(
                            Expression.Call(
                                null,
                                GetMethodInfo(DatastoreExtensions.Commit<TSourceType>, source),
                                new Expression[] { source.Expression }
                            ));
                    });
                return retval;
            }
            catch (Exception ex)
            {
                if (ex is System.ServiceModel.CommunicationObjectFaultedException ||
                    ex is System.ServiceModel.EndpointNotFoundException ||
                    ex is System.ServiceModel.CommunicationException)
                {
                    if (FailoverConfiguration.IsConfigured)
                    {
                        if (FailoverConfiguration.TryFailOver())
                            throw new Exceptions.FailoverException();
                    }
                }
                throw;
            }
        }

        #endregion

        #region Slice

        /// <summary />
        public static SummarySliceValue ToSlice<TSourceType>(this IDatastoreSliceable<TSourceType> source)
        {
            if (source == null)
                throw new ArgumentNullException(ERROR_SOURCE);

            try
            {
                SummarySliceValue retval = null;
                RetryHelper.DefaultRetryPolicy(FailoverConfiguration.RetryOnFailCount)
                    .Execute(() =>
                    {
                        retval = (SummarySliceValue)source.Provider.Execute<SummarySliceValue>(
                            Expression.Call(
                                null,
                                GetMethodInfo(DatastoreExtensions.ToSlice<TSourceType>, source),
                                new Expression[] { source.Expression }
                            ));
                    });
                return retval;
            }
            catch (Exception ex)
            {
                if (ex is System.ServiceModel.CommunicationObjectFaultedException ||
                    ex is System.ServiceModel.EndpointNotFoundException ||
                    ex is System.ServiceModel.CommunicationException)
                {
                    if (FailoverConfiguration.IsConfigured)
                    {
                        if (FailoverConfiguration.TryFailOver())
                            throw new Exceptions.FailoverException();
                    }
                }
                throw;
            }
        }

        /// <summary />
        public static IDatastoreSliceable<TSourceType> GroupField<TSourceType, TProperty>(this IDatastoreSliceable<TSourceType> source, Expression<Func<TSourceType, TProperty>> member)
            where TSourceType : IDatastoreItem
        {
            if (source == null)
                throw new ArgumentNullException(ERROR_SOURCE);
            if (member == null)
                throw new ArgumentNullException("member");

            try
            {
                return source.Provider.CreateSliceQuery<TSourceType>(
                    Expression.Call(
                        null,
                        GetMethodInfo(DatastoreExtensions.GroupField, source, member),
                        new Expression[] { source.Expression, member }
                    ));
            }
            catch (Exception ex)
            {
                if (ex is System.ServiceModel.CommunicationObjectFaultedException ||
                    ex is System.ServiceModel.EndpointNotFoundException ||
                    ex is System.ServiceModel.CommunicationException)
                {
                    if (FailoverConfiguration.IsConfigured)
                    {
                        if (FailoverConfiguration.TryFailOver())
                            throw new Exceptions.FailoverException();
                    }
                }
                throw;
            }
        }

        /// <summary />
        public static IDatastoreSliceable<TSourceType> SliceDimension<TSourceType, TProperty>(this IDatastoreSliceable<TSourceType> source, Expression<Func<TSourceType, TProperty>> member)
            where TSourceType : IDatastoreItem
        {
            if (source == null)
                throw new ArgumentNullException(ERROR_SOURCE);
            if (member == null)
                throw new ArgumentNullException("member");

            try
            {
                return source.Provider.CreateSliceQuery<TSourceType>(
                    Expression.Call(
                        null,
                        GetMethodInfo(DatastoreExtensions.SliceDimension, source, member),
                        new Expression[] { source.Expression, member }
                    ));
            }
            catch (Exception ex)
            {
                if (ex is System.ServiceModel.CommunicationObjectFaultedException ||
                    ex is System.ServiceModel.EndpointNotFoundException ||
                    ex is System.ServiceModel.CommunicationException)
                {
                    if (FailoverConfiguration.IsConfigured)
                    {
                        if (FailoverConfiguration.TryFailOver())
                            throw new Exceptions.FailoverException();
                    }
                }
                throw;
            }
        }

        #endregion

        #region SelectDerivedValue

        /// <summary>
        /// Get a derived field value from a results set
        /// </summary>
        public static TKey SelectDerivedValue<TSource, TKey>(this DatastoreResults<TSource> results, Expression<Func<TSource, TKey>> field)
            where TSource : IDatastoreItem
        {
            var f = field.ToString().Split(new string[] { "=>" }, StringSplitOptions.RemoveEmptyEntries);
            if (f.Length != 2) throw new Exception("Unknown filter");
            f = f[1].Split(new char[] { '.' });
            if (f.Length != 2) throw new Exception("Unknown filter");
            if (results.DerivedFieldList == null) throw new Exception("DerivedFieldList not defined.");
            var lambda = results.DerivedFieldList.Where(x => x.Field == f[1]).Where(x => x != null);
            if (lambda.Count() == 0) throw new Exception("Derived field not found.");
            return lambda.Select(x => x.Value).Cast<TKey>().FirstOrDefault();
        }

        #endregion

        /// <summary />
        public static string ToFieldName<TSourceType, TProperty>(this TSourceType t, Expression<Func<TSourceType, TProperty>> member)
        {
            var memberName = ExpressionHelper.GetMemberName(member);
            if (memberName == null)
                return null;
            return memberName;
        }

        #region Other

        /// <summary />
        public static bool Any<T>(this IDatastoreQueryable<T> query)
              where T : IDatastoreItem
        {
            return query.Count() > 0;
        }

        /// <summary />
        public static DatastoreResults<T> DimensionsOnly<T>(this IDatastoreQueryable<T> query)
            where T : IDatastoreItem
        {
            return query.IncludeDimensions(true)
                        .IncludeRecords(false)
                        .ExcludeCount(true)
                        .Results();
        }

        /// <summary />
        public static DimensionStore<T> DimensionStore<T>(this IDatastoreQueryable<T> query)
            where T : IDatastoreItem
        {
            return query.DimensionsOnly()?.DimensionStore;
        }

        /// <summary />
        public static DatastoreItems<T> RecordsOnly<T>(this IDatastoreQueryable<T> query)
            where T : IDatastoreItem
        {
            return query.IncludeRecords(true)
                        .IncludeDimensions(false)
                        .ExcludeCount(true)
                        .Results();
        }

        /// <summary />
        public static T SingleItem<T>(this IDatastoreQueryable<T> query)
            where T : IDatastoreItem
        {
            var result = query.RecordsPerPage(1)
                              .RecordsOnly();
            return result?.Items != null ? result.Items.FirstOrDefault() : default(T);
        }

        #endregion

    }

    #endregion

}