using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using Gravitybox.Datastore.Common;
using System.ComponentModel;

namespace Gravitybox.Datastore.Common.Queryable
{
    /// <summary />
    internal class DatastoreProvider : IDatastoreProvider
    {
        private DatastoreService dsService;

        private static Dictionary<string, Func<MethodCallExpression, Type, DatastoreService, object>> _functionMap;

        static DatastoreProvider()
        {
            _functionMap = new Dictionary<string, Func<MethodCallExpression, Type, DatastoreService, object>>();

            //The old way of calling "Items" has been removed as it is handled by the global Aggregate methods
            //The new way only works with the new GroupBy extension that actually performs a real SQL "group by"
            //_functionMap.Add("Items", ExecuteItems);
            _functionMap.Add("Items", ExecuteItems2);

            _functionMap.Add("Count", ExecuteCount);
            _functionMap.Add("Results", ExecuteResults);
            _functionMap.Add("ResultsAsync", ExecuteResultsAsync);
            _functionMap.Add("ToUrl", ExecuteToUrl);
            _functionMap.Add("ToQuery", ExecuteToQuery);
            _functionMap.Add("ToSlice", ExecuteToSlice);
            _functionMap.Add("GetDataVersion", GetDataVersion);
            _functionMap.Add("Commit", ExecuteCommit);
            _functionMap.Add("AllDimensions", ExecuteAllDimensions);
            _functionMap.Add("AggregateExecute", ExecuteAggregate);
            _functionMap.Add("AggregateExecuteMin", ExecuteAggregateMin);
            _functionMap.Add("AggregateExecuteMax", ExecuteAggregateMax);
            _functionMap.Add("AggregateExecuteCount", ExecuteAggregateCount);
            _functionMap.Add("AggregateExecuteCountDistinct", ExecuteAggregateCountDistinct);
            _functionMap.Add("AggregateExecuteSum", ExecuteAggregateSum);
        }

        /// <summary />
        public DatastoreProvider(DatastoreService service)
        {
            dsService = service;
        }

        /// <summary />
        public IDatastoreQueryable<TResult> CreateQuery<TResult>(Expression expression)
        {
            return new DatastoreQueryable<TResult>(this, expression, dsService);
        }

        /// <summary />
        public IDatastoreUpdatable<TResult> CreateUpdateQuery<TResult>(Expression expression)
        {
            return new DatastoreUpdatable<TResult>(this, expression, dsService);
        }

        /// <summary />
        public IDatastoreDeletable<TResult> CreateDeleteQuery<TResult>(Expression expression)
        {
            return new DatastoreDeletable<TResult>(this, expression, dsService);
        }

        /// <summary />
        public IDatastoreGroupable<IDatastoreGrouping<TKey, TResult>> CreateGroupingQuery<TKey, TResult>(Expression expression)
        {
            return new DatastoreGroupable<IDatastoreGrouping<TKey, TResult>>(this, expression, dsService);
        }

        /// <summary />
        public IDatastoreGroupable<TResult> CreateGroupingSelectQuery<TKey, TResult>(Expression expression)
        {
            return new DatastoreGroupable<TResult>(this, expression, dsService);
        }

        /// <summary />
        public TResult Execute<TResult>(Expression expression)
        {
            return (TResult)ExecuteMethodCall<TResult>(expression);
        }

        private object ExecuteMethodCall<T>(Expression expression)
        {
            var returnType = typeof(T);

            var methodExpression = expression as MethodCallExpression;
            if (methodExpression == null)
                throw new InvalidOperationException("The expression was not a method call.");

            var methodName = methodExpression.Method.Name;
            if (!_functionMap.ContainsKey(methodName))
                throw new NotImplementedException($"Method {methodName} not implemented!");

            return _functionMap[methodName](methodExpression, returnType, dsService);
        }

        private static DataQuery BuildDataQueryFromParser(DatastoreExpressionParser parser)
        {
            var dataQuery = new DataQuery(string.IsNullOrEmpty(parser.Url) ? "?" : parser.Url);

            if (parser.Query.FieldFilters != null)
            {
                dataQuery.FieldFilters = parser.Query.FieldFilters;
            }

            if (parser.Query.FieldSorts != null)
            {
                dataQuery.FieldSorts = parser.Query.FieldSorts;
            }

            dataQuery.PageOffset = parser.Query.PageOffset;
            dataQuery.RecordsPerPage = parser.Query.RecordsPerPage;
            dataQuery.NonParsedFieldList = parser.Query.NonParsedFieldList;

            dataQuery.IncludeDimensions = parser.Query.IncludeDimensions;
            dataQuery.IncludeRecords = parser.Query.IncludeRecords;
            dataQuery.ExcludeCount = parser.Query.ExcludeCount;
            dataQuery.IncludeEmptyDimensions = parser.Query.IncludeEmptyDimensions;
            dataQuery.IPMask = parser.Query.IPMask;
            dataQuery.DerivedFieldList = parser.Query.DerivedFieldList;

            if (parser.Query.Keyword != null)
            {
                dataQuery.Keyword = parser.Query.Keyword;
            }

            if (parser.Users != null)
            {
                dataQuery.UserList = parser.Users;
            }

            if (parser.Query.DimensionValueList != null)
            {
                dataQuery.DimensionValueList = parser.Query.DimensionValueList;
            }

            if (parser.Query.SkipDimensions != null)
            {
                dataQuery.SkipDimensions = parser.Query.SkipDimensions;
            }

            if (parser.SelectList != null)
            {
                dataQuery.FieldSelects = new List<string>();
                dataQuery.FieldSelects.AddRange(parser.SelectList);
            }
            //else
            //{
            //    if (parser.DerivedFieldList != null)
            //    {
            //        dataQuery.DerivedFieldList = parser.DerivedFieldList;
            //    }
            //}

            if (parser.Query.GroupFields != null)
            {
                dataQuery.GroupFields = new List<string>();
                dataQuery.GroupFields.AddRange(parser.Query.GroupFields);
            }

            dataQuery.IncludeEmptyDimensions = parser.Query.IncludeEmptyDimensions;

            return dataQuery;
        }

        private static SummarySlice BuildDataSliceQueryFromParser(DatastoreExpressionParser parser)
        {
            var dataQuery = new SummarySlice();

            if (parser.Query.FieldFilters != null)
            {
                dataQuery.Query.FieldFilters = parser.Query.FieldFilters;
            }

            if (parser.Query.FieldSorts != null)
            {
                dataQuery.Query.FieldSorts = parser.Query.FieldSorts;
            }

            dataQuery.Query.PageOffset = parser.Query.PageOffset;
            dataQuery.Query.RecordsPerPage = parser.Query.RecordsPerPage;

            if (parser.Query.Keyword != null)
            {
                dataQuery.Query.Keyword = parser.Query.Keyword;
            }

            if (parser.Users != null)
            {
                dataQuery.Query.UserList = parser.Users;
            }

            if (parser.Query.DimensionValueList != null)
            {
                dataQuery.Query.DimensionValueList = parser.Query.DimensionValueList;
            }

            if (parser.Query.SkipDimensions != null)
            {
                dataQuery.Query.SkipDimensions = parser.Query.SkipDimensions;
            }

            dataQuery.GroupFields = parser.SliceQuery.GroupFields;
            dataQuery.SpliceDName = parser.SliceQuery.SpliceDName;

            return dataQuery;
        }

        private static object ExecuteItems(MethodCallExpression methodExpression, Type returnType, DatastoreService dsService)
        {
            if (methodExpression.Arguments.Count < 1)
                throw new InvalidOperationException("Missing arguments for the Items expression.");

            var parser = new DatastoreExpressionParser();
            parser.Visit(methodExpression.Arguments[0]);

            var dataQuery = BuildDataQueryFromParser(parser);
            dataQuery.IncludeDimensions = false;
            dataQuery.IncludeRecords = true;
            dataQuery.ExcludeCount = false;

            var itemType = GetGenericArgument(returnType, 0);
            var dsItems = Activator.CreateInstance(typeof(DatastoreItems<>).MakeGenericType(itemType));
            var results = dsService.Query(dataQuery);
            SetResultItems(dsItems, itemType, dataQuery, results, parser.SelectExpression);
            return dsItems;
        }

        private static object ExecuteItems2(MethodCallExpression methodExpression, Type returnType, DatastoreService dsService)
        {
            if (methodExpression.Arguments.Count < 1)
                throw new InvalidOperationException("Missing arguments for the Items expression.");

            var parser = new DatastoreExpressionParser();
            parser.Visit(methodExpression.Arguments[0]);

            var dataQuery = BuildDataQueryFromParser(parser);
            dataQuery.IncludeDimensions = false;
            dataQuery.IncludeRecords = true;
            dataQuery.ExcludeCount = false;

            var itemType = GetGenericArgument(returnType, 0);
            var dsItems = Activator.CreateInstance(typeof(DatastoreItems<>).MakeGenericType(itemType));
            var results = dsService.Query(dataQuery);
            SetResultItems(dsItems, itemType, dataQuery, results, parser.SelectExpression);
            return dsItems;
        }

        private static object ExecuteAggregate(MethodCallExpression methodExpression, Type returnType, DatastoreService dsService)
        {
            if (methodExpression.Arguments.Count < 1)
                throw new InvalidOperationException("Missing arguments for the Aggregate expression.");

            var dataQuery = GetAggregateParser(methodExpression);
            var results = dsService.Query(dataQuery);
            return BuildItemFromDerivedFields(results, returnType);
        }

        private static DataQuery GetAggregateParser(MethodCallExpression methodExpression)
        {
            var parser = new DatastoreExpressionParser();
            parser.Visit(methodExpression.Arguments[0]);

            var dataQuery = BuildDataQueryFromParser(parser);
            dataQuery.IncludeDimensions = false;
            dataQuery.IncludeRecords = true;
            dataQuery.ExcludeCount = false;
            dataQuery.FieldSelects = null;

            return dataQuery;
        }

        private static object ExecuteAggregateMin(MethodCallExpression methodExpression, Type returnType, DatastoreService dsService)
        {
            if (methodExpression.Arguments.Count < 1)
                throw new InvalidOperationException("Missing arguments for the Aggregate expression.");

            var dataQuery = GetAggregateParser(methodExpression);
            var results = dsService.Query(dataQuery);
            return BuildItemFromDerivedFields2(results, returnType);
        }

        private static object ExecuteAggregateMax(MethodCallExpression methodExpression, Type returnType, DatastoreService dsService)
        {
            if (methodExpression.Arguments.Count < 1)
                throw new InvalidOperationException("Missing arguments for the Aggregate expression.");

            var dataQuery = GetAggregateParser(methodExpression);
            var results = dsService.Query(dataQuery);
            return BuildItemFromDerivedFields2(results, returnType);
        }

        private static object ExecuteAggregateCount(MethodCallExpression methodExpression, Type returnType, DatastoreService dsService)
        {
            if (methodExpression.Arguments.Count < 1)
                throw new InvalidOperationException("Missing arguments for the Aggregate expression.");

            var dataQuery = GetAggregateParser(methodExpression);
            var results = dsService.Query(dataQuery);
            return BuildItemFromDerivedFields2(results, returnType);
        }

        private static object ExecuteAggregateCountDistinct(MethodCallExpression methodExpression, Type returnType, DatastoreService dsService)
        {
            if (methodExpression.Arguments.Count < 1)
                throw new InvalidOperationException("Missing arguments for the Aggregate expression.");

            var dataQuery = GetAggregateParser(methodExpression);
            var results = dsService.Query(dataQuery);
            return BuildItemFromDerivedFields2(results, returnType);
        }

        private static object ExecuteAggregateSum(MethodCallExpression methodExpression, Type returnType, DatastoreService dsService)
        {
            if (methodExpression.Arguments.Count < 1)
                throw new InvalidOperationException("Missing arguments for the Aggregate expression.");

            var dataQuery = GetAggregateParser(methodExpression);
            var results = dsService.Query(dataQuery);
            return BuildItemFromDerivedFields2(results, returnType);
        }

        private static object ExecuteCount(MethodCallExpression methodExpression, Type returnType, DatastoreService dsService)
        {
            if (methodExpression.Arguments.Count < 1)
                throw new InvalidOperationException("Missing arguments for the Count expression.");

            var parser = new DatastoreExpressionParser();
            parser.Visit(methodExpression.Arguments[0]);

            var dataQuery = BuildDataQueryFromParser(parser);
            dataQuery.IncludeDimensions = false;
            dataQuery.IncludeRecords = false;
            dataQuery.ExcludeCount = false;

            var result = dsService.Query(dataQuery);
            return result.TotalRecordCount;
        }

        private static object ExecuteAllDimensions(MethodCallExpression methodExpression, Type returnType, DatastoreService dsService)
        {
            if (methodExpression.Arguments.Count < 1)
                throw new InvalidOperationException("Missing arguments for the AllDimensions expression.");

            // This comes from IDatastoreQueryable<TSourceType, TResultType>
            var sourceType = GetGenericArgument(returnType, 0);

            var parser = new DatastoreExpressionParser();
            parser.Visit(methodExpression.Arguments[0]);

            var dataQuery = BuildDataQueryFromParser(parser);
            dataQuery.IncludeDimensions = true;
            dataQuery.IncludeRecords = false;
            dataQuery.ExcludeCount = true;
            var results = dsService.Query(dataQuery);

            var allDimensions = new List<DimensionItem>();
            foreach (var d in results.DimensionList)
            {
                allDimensions.Add((DimensionItem)((ICloneable)d).Clone());
            }

            var dimensionStore = Activator.CreateInstance(
                typeof(DimensionStore<>).MakeGenericType(sourceType),
                new object[] { allDimensions, results.Query.NonParsedFieldList });

            return dimensionStore;
        }

        private static object GetDataVersion(MethodCallExpression methodExpression, Type returnType, DatastoreService dsService)
        {
            return dsService.GetDataVersion();
        }

        private static string ExecuteToUrl(MethodCallExpression methodExpression, Type returnType, DatastoreService dsService)
        {
            var dataQuery = ExecuteToQuery(methodExpression, returnType, dsService);
            return dataQuery.ToString();
        }

        private static DataQuery ExecuteToQuery(MethodCallExpression methodExpression, Type returnType, DatastoreService dsService)
        {
            if (methodExpression.Arguments.Count < 1)
                throw new InvalidOperationException("Missing arguments for the Results expression.");

            var parser = new DatastoreExpressionParser();
            parser.Visit(methodExpression.Arguments[0]);

            var dataQuery = BuildDataQueryFromParser(parser);
            return dataQuery;
        }

        private static object ExecuteToSlice(MethodCallExpression methodExpression, Type returnType, DatastoreService dsService)
        {
            if (methodExpression.Arguments.Count < 1)
                throw new InvalidOperationException("Missing arguments for the Results expression.");

            var parser = new DatastoreExpressionParser();
            parser.Visit(methodExpression.Arguments[0]);

            var dataQuery = BuildDataSliceQueryFromParser(parser);
            return dsService.CalculateSlice(dataQuery);
        }

        private static object ExecuteResults(MethodCallExpression methodExpression, Type returnType, DatastoreService dsService)
        {
            if (methodExpression.Arguments.Count < 1)
                throw new InvalidOperationException("Missing arguments for the Results expression.");

            var parser = new DatastoreExpressionParser();
            parser.Visit(methodExpression.Arguments[0]);

            var dataQuery = BuildDataQueryFromParser(parser);
            var results = dsService.Query(dataQuery);
            var sourceType = GetGenericArgument(returnType, 0);

            #region Create the dimensions
            var dimensions = new List<DimensionItem>();
            foreach (var d in results.DimensionList)
            {
                dimensions.Add((DimensionItem)((ICloneable)d).Clone());
            }

            var dimensionStore = Activator.CreateInstance(
                typeof(DimensionStore<>).MakeGenericType(sourceType),
                new object[] { dimensions, results.Query.NonParsedFieldList });
            #endregion

            #region Create the ALL dimensions
            var allDimensions = new List<DimensionItem>();
            foreach (var d in results.AllDimensionList)
            {
                allDimensions.Add((DimensionItem)((ICloneable)d).Clone());
            }

            var allDimensionStore = Activator.CreateInstance(
                typeof(DimensionStore<>).MakeGenericType(sourceType),
                new object[] { allDimensions, results.Query.NonParsedFieldList });
            #endregion

            #region Create the APPLIED dimensions
            var appliedDimensions = new List<DimensionItem>();
            foreach (var d in results.AppliedDimensionList)
            {
                appliedDimensions.Add((DimensionItem)((ICloneable)d).Clone());
            }

            var appliedDimensionstore = Activator.CreateInstance(
                typeof(DimensionStore<>).MakeGenericType(sourceType),
                new object[] { appliedDimensions, results.Query.NonParsedFieldList });
            #endregion

            var dsResults = Activator.CreateInstance(typeof(DatastoreResults<>).MakeGenericType(sourceType));
            SetProperty(dsResults, "DimensionStore", dimensionStore);
            SetProperty(dsResults, "AppliedDimensionList", appliedDimensionstore);
            SetProperty(dsResults, "AppliedFieldFilterList", results.Query.FieldFilters ?? new List<IFieldFilter>());
            SetProperty(dsResults, "AllDimensions", allDimensionStore);
            SetProperty(dsResults, "DerivedFieldList", results.DerivedFieldList?.ToList()); 
            SetResultItems(dsResults, sourceType, dataQuery, results, parser.SelectExpression);

            var diag = new ResultDiagnostics
            {
                CacheHit = results.CacheHit,
                ComputeTime = results.ComputeTime,
                LockTime = results.LockTime,
                DataVersion = results.DataVersion,
                VersionHash = results.VersionHash,
                RepositoryId = dsService.RepositoryId,
            };
            SetProperty(dsResults, "Diagnostics", diag);

            SetProperty(dsResults, "Query", ((ICloneable)results.Query).Clone());

            return dsResults;
        }

        private static object ExecuteResultsAsync(MethodCallExpression methodExpression, Type returnType, DatastoreService dsService)
        {
            if (methodExpression.Arguments.Count < 1)
                throw new InvalidOperationException("Missing arguments for the Results expression.");

            var parser = new DatastoreExpressionParser();
            parser.Visit(methodExpression.Arguments[0]);

            var dataQuery = BuildDataQueryFromParser(parser);
            return new DatastoreResultsAsync(dsService, dataQuery);
        }

        private static void SetResultItems(object resultObj, Type itemType, DataQuery dataQuery, DataQueryResults results, Expression selectExpression)
        {
            if (dataQuery.RecordsPerPage > 0)
            {
                var totalPageCount = (results.TotalRecordCount / dataQuery.RecordsPerPage) + (results.TotalRecordCount % dataQuery.RecordsPerPage == 0 ? 0 : 1);
                SetProperty(resultObj, "TotalPageCount", totalPageCount);
            }

            SetProperty(resultObj, "TotalRecordCount", results.TotalRecordCount);
            SetProperty(resultObj, "Query", results.Query);
            SetProperty(resultObj, "Items", BuildItemList(results, itemType, selectExpression));
        }

        private static void SetProperty(object target, string name, object value)
        {
            var property = target.GetType().GetProperty(name);
            if (property != null)
                property.SetValue(target, value);
        }

        private static Type GetGenericArgument(Type type)
        {
            return GetGenericArgument(type, 0);
        }

        private static Type GetGenericArgument(Type type, int index)
        {
            var list = type.GetGenericArguments();
            if (list.Length < (index + 1))
                throw new ArgumentException("The index for the requested generic type was out of range.");

            return list[index];
        }

        private static object BuildItemList(DataQueryResults results, Type itemType, Expression selectExpression)
        {
            if(results.Query.GroupFields?.Count>0)
            {
                //GroupBy
                return BuildItemListWithSelect2(results, itemType, selectExpression);
            }
            else
            {
                //Normal projection
                if (selectExpression == null)
                {
                    return BuildItemList(results, itemType);
                }
                return BuildItemListWithSelect(results, itemType, selectExpression);
            }
        }

        private static object BuildItemList(DataQueryResults results, Type itemType)
        {
            var itemList = (IList)Activator.CreateInstance(typeof(List<>).MakeGenericType(itemType));
            if (itemList == null) return null;

            foreach (var dataItem in results.RecordList)
            {
                var obj = Activator.CreateInstance(itemType);

                var dataStoreItem = obj as IDatastoreItem;
                if (dataStoreItem != null)
                {
                    var index = 0;
                    foreach (var fieldset in results.Fieldset)
                    {
                        var value = dataItem.ItemArray[index];
                        var property = itemType.GetProperty(fieldset.Name);
                        if (property != null && property.CanWrite)
                        {
                            property.SetValue(obj, value);
                        }
                        else
                        {
                            if (dataStoreItem.ExtraValues == null)
                                dataStoreItem.ExtraValues = new Dictionary<string, string>();
                            dataStoreItem.ExtraValues.Add(fieldset.Name, value?.ToString());
                        }
                        index++;
                    }

                    dataStoreItem.__OrdinalPosition = dataItem.__OrdinalPosition;
                    dataStoreItem.__RecordIndex = dataItem.__RecordIndex;
                    dataStoreItem.__Timestamp = dataItem.__Timestamp;
                    itemList.Add(obj);
                }
            }
            return itemList;
        }

        private static object BuildItemListWithSelect(DataQueryResults results, Type itemType, Expression selectExpression)
        {
            var itemList = (IList)Activator.CreateInstance(typeof(List<>).MakeGenericType(itemType));
            if (itemList == null) return null;

            var converter = new SelectExpressionConverter(results.Fieldset);
            foreach (var dataItem in results.RecordList)
            {
                // I take the original select expression and then substitute the member expressions for constant data values
                //var newExpression = converter.Convert(selectExpression, dataItem.ItemArray.Take(dataItem.ItemArray.Length - 2).ToArray());
                var newExpression = converter.Convert(selectExpression, dataItem.ItemArray);

                // Magic happens here.
                // We then compile and execute the expression which create a new object that has been properly initialized
                var objectMember = Expression.Convert(newExpression, typeof(object));
                var getterLambda = Expression.Lambda<Func<object>>(objectMember);
                var getter = getterLambda.Compile();
                var obj = getter();

                itemList.Add(obj);
            }
            return itemList;
        }

        private static object BuildItemListWithSelect2(DataQueryResults results, Type itemType, Expression selectExpression)
        {
            var itemList = (IList)Activator.CreateInstance(typeof(List<>).MakeGenericType(itemType));
            if (itemList == null) return null;

            foreach (var record in results.RecordList)
            {
                //Load all derived fields first
                var objArray = new object[results.Query.FieldSelects.Count];
                var index = 0;
                foreach (var fieldName in results.Query.FieldSelects)
                {
                    if (fieldName == "Key")
                    {
                        objArray[index] = record.ItemArray[0];
                    }
                    else if (results.Query.DerivedFieldList.Any(x => x.Alias == fieldName))
                    {
                        var dfIndex = results.Query.DerivedFieldList.IndexOf(x => x.Alias == fieldName) + results.Query.GroupFields.Count;
                        objArray[index] = record.ItemArray[dfIndex];
                    }
                    index++;
                }

                //Create the actual object
                var obj = Activator.CreateInstance(itemType, objArray);
                itemList.Add(obj);
            }
            return itemList;
        }

        private static object BuildItemFromDerivedFields(DataQueryResults results, Type itemType)
        {
            var index = 0;
            var constructorParams = new object[results.DerivedFieldList.Length];
            foreach (var derivedField in results.DerivedFieldList)
            {
                constructorParams[index++] = derivedField.Value;
            }
            return Activator.CreateInstance(itemType, constructorParams);
        }

        private static object BuildItemFromDerivedFields2(DataQueryResults results, Type itemType)
        {
            var constructorParams = new object[results.DerivedFieldList.Length];
            if (results.DerivedFieldList.Any())
                return results.DerivedFieldList.First().Value;
            return null;
        }

        private static ActionDiagnostics ExecuteCommit(MethodCallExpression methodExpression, Type returnType, DatastoreService dsService)
        {
            if (methodExpression.Arguments.Count < 1)
                throw new InvalidOperationException("Invalid argument count for the Commit expression.");

            if (typeof(IDatastoreUpdatable).IsAssignableFrom(methodExpression.Arguments[0].Type))
            {
                return ExecuteUpdate(methodExpression, returnType, dsService);
            }
            else if (typeof(IDatastoreDeletable).IsAssignableFrom(methodExpression.Arguments[0].Type))
            {
                return ExecuteDelete(methodExpression, returnType, dsService);
            }
            throw new InvalidOperationException("Commit not available");
        }

        private static ActionDiagnostics ExecuteUpdate(MethodCallExpression methodExpression, Type returnType, DatastoreService dsService)
        {
            if (methodExpression.Arguments.Count < 1)
                throw new InvalidOperationException("Invalid argument count for the Update expression.");

            var parser = new DatastoreExpressionParser();
            parser.Visit(methodExpression.Arguments[0]);

            var dataQuery = BuildDataQueryFromParser(parser);

            if (parser.FieldUpdates == null || parser.FieldUpdates.Count == 0)
                return new ActionDiagnostics { IsSuccess = false }; //throw new InvalidOperationException("No field updates specified");

            return dsService.UpdateDataWhere(dataQuery, parser.FieldUpdates);
        }

        private static ActionDiagnostics ExecuteDelete(MethodCallExpression methodExpression, Type returnType, DatastoreService dsService)
        {
            if (methodExpression.Arguments.Count < 1)
                throw new InvalidOperationException("Invalid argument count for the Delete expression.");

            var parser = new DatastoreExpressionParser();
            parser.Visit(methodExpression.Arguments[0]);
            var dataQuery = BuildDataQueryFromParser(parser);
            return dsService.DeleteData(dataQuery);
        }

        /// <summary />
        public IDatastoreSliceable<TSourceType> CreateSliceQuery<TSourceType>(Expression expression)
        {
            return new DatastoreSliceable<TSourceType>(this, expression, dsService);
        }
    }
}
