using Gravitybox.Datastore.Common;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Linq.Expressions;

namespace Gravitybox.Datastore.Common.Queryable
{
    internal class DatastoreExpressionParser : ExpressionVisitor
    {
        internal static string[] SupportedAggMethods = new string[] { "Max", "Min", "Count", "Sum" };

        public DataQuery Query { get; } = new DataQuery();

        public SummarySlice SliceQuery { get; } = new SummarySlice();

        public List<string> SelectList { get; set; }

        public List<DataFieldUpdate> FieldUpdates { get; set; } = new List<DataFieldUpdate>();

        public string Url { get; set; }

        public List<int> Users { get; set; }

        public Expression SelectExpression { get; set; }

        public DatastoreExpressionParser()
        {
            this.Query.IncludeDimensions = true;
            this.Query.IncludeRecords = true;
            this.Query.ExcludeCount = false;
        }

        protected override Expression VisitMethodCall(MethodCallExpression node)
        {
            base.VisitMethodCall(node);
            this.VisitMethodCallInternal(node);
            return node;
        }

        private void VisitMethodCallInternal(MethodCallExpression node)
        {
            if (node.Method.Name == "Where")
            {
                HandleWhereExpression(node.Arguments);
            }
            else if (node.Method.Name == "WhereKeyword")
            {
                HandleWhereKeywordExpression(node.Arguments);
            }
            else if (node.Method.Name == "WhereUrl")
            {
                HandleWhereUrlExpression(node.Arguments);
            }
            else if (node.Method.Name == "WhereUser")
            {
                HandleWhereUserExpression(node.Arguments);
            }
            else if (node.Method.Name == "WhereDimensionValue")
            {
                HandleWhereDimensionValueExpression(node.Arguments);
            }
            else if (node.Method.Name == "IncludeDimensions")
            {
                HandleIncludeDimensionsExpression(node.Arguments);
            }
            else if (node.Method.Name == "IncludeAllDimensions")
            {
                HandleIncludeAllDimensionsExpression(node.Arguments);
            }
            else if (node.Method.Name == "IncludeRecords")
            {
                HandleIncludeRecordsExpression(node.Arguments);
            }
            else if (node.Method.Name == "IncludeIpAddress")
            {
                HandleIncludeIpAddressExpression(node.Arguments);
            }
            else if (node.Method.Name == "ExcludeCount")
            {
                HandleExcludeCountExpression(node.Arguments);
            }
            else if (node.Method.Name == "RemoveDimensionValue")
            {
                HandleRemoveDimensionValueExpression(node.Arguments);
            }
            else if (node.Method.Name == "RecordsPerPage")
            {
                HandleRecordsPerPageExpression(node.Arguments);
            }
            else if (node.Method.Name == "PageOffset")
            {
                HandlePageOffsetExpression(node.Arguments);
            }
            else if (node.Method.Name == "OrderBy")
            {
                HandleOrderByExpression(node.Arguments, SortDirectionConstants.Asc);
            }
            else if (node.Method.Name == "ThenBy")
            {
                HandleOrderByExpression(node.Arguments, SortDirectionConstants.Asc);
            }
            else if (node.Method.Name == "OrderByDescending")
            {
                HandleOrderByExpression(node.Arguments, SortDirectionConstants.Desc);
            }
            else if (node.Method.Name == "ThenByDescending")
            {
                HandleOrderByExpression(node.Arguments, SortDirectionConstants.Desc);
            }
            else if (node.Method.Name == "SkipDimension")
            {
                HandleSkipDimensionExpression(node.Arguments);
            }
            else if (node.Method.Name == "IncludeEmptyDimensions")
            {
                this.Query.IncludeEmptyDimensions = true;
            }
            else if (node.Method.Name == "ExcludeDimensionCount")
            {
                this.Query.ExcludeDimensionCount = true;
            }
            else if (node.Method.Name == "AggregateMin")
            {
                HandleAggregateShortcutExpression(node.Arguments, AggregateOperationConstants.Min);
            }
            else if (node.Method.Name == "AggregateMax")
            {
                HandleAggregateShortcutExpression(node.Arguments, AggregateOperationConstants.Max);
            }
            else if (node.Method.Name == "AggregateCount")
            {
                HandleAggregateShortcutExpression(node.Arguments, AggregateOperationConstants.Count);
            }
            else if (node.Method.Name == "AggregateCountDistinct")
            {
                HandleAggregateShortcutExpression(node.Arguments, AggregateOperationConstants.Distinct);
            }
            else if (node.Method.Name == "AggregateSum")
            {
                HandleAggregateShortcutExpression(node.Arguments, AggregateOperationConstants.Sum);
            }
            else if (node.Method.Name == "AggregateSelect")
            {
                HandleAggregateSelectExpression(node.Arguments);
            }
            else if (node.Method.Name == "Field")
            {
                HandleFieldExpression(node.Arguments);
            }
            else if (node.Method.Name == "Select")
            {
                //Path for GroupBy or Simple Projection
                if (this.Query.GroupFields?.Count > 0)
                    HandleSelect2Expression(node.Arguments);
                else
                    HandleSelectExpression(node.Arguments);
            }
            else if (node.Method.Name == "AddNonParsedField")
            {
                HandleAddNonParsedFieldExpression(node.Arguments);
            }
            else if (node.Method.Name == "RemoveNonParsedField")
            {
                HandleRemoveNonParsedFieldExpression(node.Arguments);
            }
            else if (node.Method.Name == "AddFieldFilter")
            {
                HandleAddFieldFilterExpression(node.Arguments);
            }
            else if (node.Method.Name == "RemoveFieldFilter")
            {
                HandleRemoveFieldFilterExpression(node.Arguments);
            }
            else if (node.Method.Name == "GroupField")
            {
                HandleGroupFieldExpression(node.Arguments);
            }
            else if (node.Method.Name == "GroupBy" || node.Method.Name == "GroupBy2")
            {
                HandleGroupByExpression(node.Arguments);
            }
            else if (node.Method.Name == "SliceDimension")
            {
                HandleSliceDimensionExpression(node.Arguments);
            }
            else if (node.Method.Name == "FromQuery")
            {
                HandleFromQueryExpression(node.Arguments);
            }
            else if (node.Method.Name == "ClearSorts")
            {
                HandleClearSortsExpression(node.Arguments);
            }
            else if (node.Method.Name == "ClearFilters")
            {
                HandleClearFiltersExpression(node.Arguments);
            }
            else if (node.Method.Name == "ClearDimensions")
            {
                HandleClearDimensionsExpression(node.Arguments);
            }
            else if (node.Method.Name == "RemoveSort")
            {
                HandleRemoveSortExpression(node.Arguments);
            }
            else if (node.Method.Name == "FieldMin")
            {
                HandleDerivedFieldExpression(node.Arguments, AggregateOperationConstants.Min);
            }
            else if (node.Method.Name == "FieldMax")
            {
                HandleDerivedFieldExpression(node.Arguments, AggregateOperationConstants.Max);
            }
            else if (node.Method.Name == "FieldSum")
            {
                HandleDerivedFieldExpression(node.Arguments, AggregateOperationConstants.Sum);
            }
            else if (node.Method.Name == "FieldCount")
            {
                HandleDerivedFieldExpression(node.Arguments, AggregateOperationConstants.Count);
            }
            else if (node.Method.Name == "FieldCountDistinct")
            {
                HandleDerivedFieldExpression(node.Arguments, AggregateOperationConstants.Distinct);
            }

            //Group by aggregations
            if (node.NodeType == ExpressionType.Call && SupportedAggMethods.Any(x => x == node.Method.Name))
            {
                HandleAggregateGroupingSelectExpression(node);
            }

        }

        private void HandleRecordsPerPageExpression(ReadOnlyCollection<Expression> arguments)
        {
            if (arguments.Count == 2)
            {
                var constant = arguments[1] as ConstantExpression;
                if (constant == null)
                    throw new InvalidOperationException("Invalid RecordsPerPage expression. Missing RecordsPerPage.");

                this.Query.RecordsPerPage = (int)constant.Value;
                return; // Success
            }
            throw new InvalidOperationException("Invalid Paging expression.");
        }

        private void HandlePageOffsetExpression(ReadOnlyCollection<Expression> arguments)
        {
            if (arguments.Count == 2)
            {
                var constant = arguments[1] as ConstantExpression;
                if (constant == null)
                    throw new InvalidOperationException("Invalid PageOffset expression. Missing PageOffset.");

                this.Query.PageOffset = (int)constant.Value;
                return; // Success
            }
            throw new InvalidOperationException("Invalid PageOffset expression.");
        }

        private void HandleWhereExpression(ReadOnlyCollection<Expression> arguments)
        {
            if (arguments.Count == 2)
            {
                var vistory = new WhereExpressionParser();
                vistory.Visit(arguments[1]);

                foreach (var filter in vistory.FieldFilters)
                {
                    this.Query.FieldFilters.Add(filter);
                }

                return; // Success
            }
            throw new InvalidOperationException("Invalid Where expression arguments.");
        }

        private void HandleFieldExpression(ReadOnlyCollection<Expression> arguments)
        {
            if (arguments.Count == 3)
            {
                var memberName = String.Empty;
                var unaryExpr = arguments[1] as UnaryExpression;
                if (unaryExpr != null)
                {
                    memberName = ExpressionHelper.GetMemberName(unaryExpr.Operand);
                }
                else
                {
                    var memberNameConstant = arguments[1] as ConstantExpression;
                    if (memberNameConstant != null)
                    {
                        memberName = (string)memberNameConstant.Value;
                    }
                }

                if (String.IsNullOrEmpty(memberName))
                    throw new ArgumentNullException("Field member name missing.");

                var constantExpr = arguments[2] as ConstantExpression;
                if (constantExpr == null)
                    throw new ArgumentNullException("Field value missing.");

                var fieldUpdate = new DataFieldUpdate();
                fieldUpdate.FieldName = memberName;
                fieldUpdate.FieldValue = constantExpr.Value;

                if (FieldUpdates == null)
                    FieldUpdates = new List<DataFieldUpdate>();

                FieldUpdates.Add(fieldUpdate);
                return; // Success
            }
            throw new InvalidOperationException("Invalid Field expression arguments.");
        }

        private void HandleSelectExpression(ReadOnlyCollection<Expression> arguments)
        {
            if (arguments.Count == 2)
            {
                var unaryExpr = arguments[1] as UnaryExpression;
                if (unaryExpr != null)
                {
                    var lambda = unaryExpr.Operand as LambdaExpression;
                    if (lambda != null)
                    {
                        if (this.SelectList == null)
                            this.SelectList = new List<string>();

                        if (lambda.Body is NewExpression || lambda.Body is MemberInitExpression)
                        {
                            SelectExpression = lambda.Body;

                            var memberVistor = new MemberExpressionVisitor();
                            memberVistor.Visit(SelectExpression);

                            if (memberVistor.Members == null)
                                throw new InvalidOperationException("No members selected");

                            this.SelectList.AddRange(memberVistor.Members.Distinct());

                            return; // Success
                        }
                    }
                }
            }
            throw new InvalidOperationException("Invalid Select expression arguments.");
        }

        private void HandleSelect2Expression(ReadOnlyCollection<Expression> arguments)
        {
            if (arguments.Count == 2)
            {
                var unaryExpr = arguments[1] as UnaryExpression;
                if (unaryExpr != null)
                {
                    var lambda = unaryExpr.Operand as LambdaExpression;
                    if (lambda != null)
                    {
                        if (this.SelectList == null)
                            this.SelectList = new List<string>();

                        if (lambda.Body is NewExpression)
                        {
                            var newExpr = lambda.Body as NewExpression;
                            var index = 0;
                            var derivedIndex = 0;
                            foreach (var item in newExpr.Arguments)
                            {
                                var memExpr = item as MemberExpression;
                                var methodExpr = item as MethodCallExpression;

                                if (memExpr != null)
                                {
                                    this.SelectList.Add(memExpr.Member.Name);
                                }
                                else if (methodExpr != null)
                                {
                                    var name = newExpr.Members[index].Name;
                                    this.SelectList.Add(name);
                                    this.Query.DerivedFieldList[derivedIndex].Alias = name;
                                    derivedIndex++;
                                }
                                index++;
                            }
                            
                            if (!this.SelectList.Any())
                                throw new InvalidOperationException("No members selected");

                            return; // Success
                        }
                    }
                }
            }
            throw new InvalidOperationException("Invalid Select expression arguments.");
        }

        private void HandleAddNonParsedFieldExpression(ReadOnlyCollection<Expression> arguments)
        {
            if (arguments.Count == 3)
            {
                var constExpr1 = arguments[1] as ConstantExpression;
                var constExpr2 = arguments[2] as ConstantExpression;
                if (constExpr1 != null && constExpr2 != null)
                {
                    this.Query.NonParsedFieldList.RemoveAll(x => x.Key == (string)constExpr1.Value);
                    this.Query.NonParsedFieldList.Add((string)constExpr1.Value, (string)constExpr2.Value);
                    return; // Success
                }
            }
            throw new InvalidOperationException("Invalid expression.");
        }

        private void HandleRemoveNonParsedFieldExpression(ReadOnlyCollection<Expression> arguments)
        {
            if (arguments.Count == 2)
            {
                var constExpr1 = arguments[1] as ConstantExpression;
                if (constExpr1 != null)
                {
                    this.Query.NonParsedFieldList.RemoveAll(x => x.Key == (string)constExpr1.Value);
                    return; // Success
                }
            }
            throw new InvalidOperationException("Invalid expression.");
        }

        private void HandleAddFieldFilterExpression(ReadOnlyCollection<Expression> arguments)
        {
            if (arguments.Count == 2)
            {
                var constExpr1 = arguments[1] as ConstantExpression;
                if (constExpr1 != null)
                {
                    this.Query.FieldFilters.Add((IFieldFilter)constExpr1.Value);
                    return; // Success
                }
            }
            throw new InvalidOperationException("Invalid expression.");
        }

        private void HandleRemoveFieldFilterExpression(ReadOnlyCollection<Expression> arguments)
        {
            if (arguments.Count == 2)
            {
                var unaryExpr = arguments[1] as UnaryExpression;
                if (unaryExpr != null)
                {
                    var lambda = unaryExpr.Operand as LambdaExpression;
                    if (lambda != null)
                    {
                        var memExpr = lambda.Body as MemberExpression;
                        if (memExpr != null)
                        {
                            this.Query.FieldFilters.RemoveAll(x => x.Name == memExpr.Member.Name);
                            return; // Success
                        }
                    }
                }

                var constExpr = arguments[1] as ConstantExpression;
                if (constExpr != null)
                {
                    this.Query.FieldFilters.RemoveAll(x => x.Name == (string)constExpr.Value);
                    return; // Success
                }

            }
            throw new InvalidOperationException("Invalid expression.");
        }

        private void HandleRemoveSortExpression(ReadOnlyCollection<Expression> arguments)
        {
            if (arguments.Count == 2)
            {
                var unaryExpr = arguments[1] as UnaryExpression;
                if (unaryExpr != null)
                {
                    var lambda = unaryExpr.Operand as LambdaExpression;
                    if (lambda != null)
                    {
                        var memExpr = lambda.Body as MemberExpression;
                        if (memExpr != null)
                        {
                            this.Query.FieldSorts.RemoveAll(x => x.Name == memExpr.Member.Name);
                            return; // Success
                        }
                    }
                }

                var constExpr = arguments[1] as ConstantExpression;
                if (constExpr != null)
                {
                    this.Query.FieldSorts.RemoveAll(x => x.Name == (string)constExpr.Value);
                    return; // Success
                }

            }
            throw new InvalidOperationException("Invalid expression.");
        }

        private void HandleDerivedFieldExpression(ReadOnlyCollection<Expression> arguments, AggregateOperationConstants operation)
        {
            if (arguments.Count == 2)
            {
                var unaryExpr = arguments[1] as UnaryExpression;
                if (unaryExpr != null)
                {
                    var lambda = unaryExpr.Operand as LambdaExpression;
                    var memExpr = lambda?.Body as MemberExpression;
                    if (memExpr != null)
                    {
                        if (this.Query.DerivedFieldList == null)
                            this.Query.DerivedFieldList = new List<IDerivedField>();
                        this.Query.DerivedFieldList.Add(new DerivedField { Field = memExpr.Member.Name, Action = operation });
                        return; // Success
                    }
                    var newExpr = lambda?.Body as NewExpression;
                    if (newExpr != null)
                    {
                        if (this.Query.DerivedFieldList == null)
                            this.Query.DerivedFieldList = new List<IDerivedField>();
                        this.Query.DerivedFieldList.Add(new DerivedField { Field = newExpr.Members[0].Name, Action = operation });
                        return; // Success
                    }
                }
            }
            throw new InvalidOperationException("Invalid expression.");

        }

        /// <summary>
        /// This is to handle the Items group by
        /// </summary>
        private void HandleGroupByExpression(ReadOnlyCollection<Expression> arguments)
        {
            if (arguments.Count == 2)
            {
                var unaryExpr = arguments[1] as UnaryExpression;
                var lambda = unaryExpr?.Operand as LambdaExpression;
                var memExpr = lambda?.Body as MemberExpression;
                if (memExpr != null)
                {
                    if (this.Query.GroupFields == null)
                    {
                        this.Query.GroupFields = (new string[] { memExpr.Member.Name }).ToList();
                    }
                    else
                    {
                        var l = new List<string>(this.SliceQuery.GroupFields);
                        l.Add(memExpr.Member.Name);
                        this.Query.GroupFields = l.Distinct().ToList();
                    }
                    return; // Success
                }
            }
            throw new InvalidOperationException("Invalid expression.");
        }

        /// <summary>
        /// This is to handle the Slice group by
        /// </summary>
        private void HandleGroupFieldExpression(ReadOnlyCollection<Expression> arguments)
        {
            if (arguments.Count == 2)
            {
                var unaryExpr = arguments[1] as UnaryExpression;
                var lambda = unaryExpr?.Operand as LambdaExpression;
                var memExpr = lambda?.Body as MemberExpression;
                if (memExpr != null)
                {
                    if (this.SliceQuery.GroupFields == null)
                    {
                        this.SliceQuery.GroupFields = new string[] { memExpr.Member.Name };
                    }
                    else
                    {
                        var l = new List<string>(this.SliceQuery.GroupFields);
                        l.Add(memExpr.Member.Name);
                        this.SliceQuery.GroupFields = l.Distinct().ToArray();
                    }
                    return; // Success
                }
            }
            throw new InvalidOperationException("Invalid expression.");
        }

        private void HandleSliceDimensionExpression(ReadOnlyCollection<Expression> arguments)
        {
            if (arguments.Count == 2)
            {
                var unaryExpr = arguments[1] as UnaryExpression;
                var lambda = unaryExpr?.Operand as LambdaExpression;
                var memExpr = lambda?.Body as MemberExpression;
                if (memExpr != null)
                {
                    this.SliceQuery.SpliceDName = memExpr.Member.Name;
                    return; // Success
                }
            }
            throw new InvalidOperationException("Invalid expression.");
        }

        private void HandleAddUserExpression(ReadOnlyCollection<Expression> arguments)
        {
            if (arguments.Count == 2)
            {
            }
            throw new InvalidOperationException("Invalid expression.");
        }

        private void HandleAggregateSelectExpression(ReadOnlyCollection<Expression> arguments)
        {
            if (arguments.Count == 2)
            {
                var unaryExpr = arguments[1] as UnaryExpression;
                var lambda = unaryExpr?.Operand as LambdaExpression;
                if (lambda != null)
                {
                    if (this.Query.DerivedFieldList == null)
                        this.Query.DerivedFieldList = new List<IDerivedField>();

                    var newExpr = lambda.Body as NewExpression;
                    if (newExpr != null)
                    {
                        foreach (var expr in newExpr.Arguments)
                        {
                            var method = expr as MethodCallExpression;
                            if (method == null)
                                throw new InvalidOperationException("Aggregate argument must be a method call.");

                            if (method.Arguments.Count < 1)
                                throw new InvalidOperationException("Invalid arguments for the specified aggregate function.");

                            var derivedField = new DerivedField();
                            derivedField.Field = ExpressionHelper.GetMemberName(method.Arguments[0]);
                            derivedField.Action = GetAggregationType(method.Method.Name);
                            this.Query.DerivedFieldList.Add(derivedField);
                        }

                        return; // Success
                    }
                }
            }
            throw new InvalidOperationException("Invalid Aggregate expression arguments.");
        }

        private void HandleAggregateGroupingSelectExpression(MethodCallExpression expression)
        {
            if (this.Query.DerivedFieldList == null)
                this.Query.DerivedFieldList = new List<IDerivedField>();

            if (expression.Arguments.Count == 2)
            {
                //Try property expression
                var lambda = expression.Arguments[1] as LambdaExpression;
                var memExpr = lambda.Body as MemberExpression;
                if (memExpr != null)
                {
                    var derivedField = new DerivedField();
                    derivedField.Field = memExpr.Member.Name;
                    derivedField.Action = GetAggregationType(expression.Method.Name);
                    this.Query.DerivedFieldList.Add(derivedField);
                    return; // Success
                }
            }
            else if (expression.Method.Name == "Count")
            {
                var derivedField = new DerivedField();
                derivedField.Field = "{GroupBy}";
                derivedField.Action = AggregateOperationConstants.Count;
                this.Query.DerivedFieldList.Add(derivedField);
                return; // Success
            }
            throw new InvalidOperationException("Invalid Aggregate expression arguments.");
        }

        private AggregateOperationConstants GetAggregationType(string methodName)
        {
            switch (methodName)
            {
                case "Sum":
                    return AggregateOperationConstants.Sum;
                case "Min":
                    return AggregateOperationConstants.Min;
                case "Max":
                    return AggregateOperationConstants.Max;
                case "Count":
                    return AggregateOperationConstants.Count;
                case "CountDistinct":
                    return AggregateOperationConstants.Distinct;
                default:
                    throw new InvalidOperationException($"Invalid aggregate function {methodName}.");
            }
        }

        private void HandleAggregateShortcutExpression(ReadOnlyCollection<Expression> arguments, AggregateOperationConstants aggType)
        {
            if (arguments.Count == 2)
            {
                var unaryExpr = arguments[1] as UnaryExpression;
                var lambda = unaryExpr?.Operand as LambdaExpression;
                if (lambda != null)
                {
                    if (this.Query.DerivedFieldList == null)
                        this.Query.DerivedFieldList = new List<IDerivedField>();

                    var memExpr = lambda.Body as MemberExpression;
                    if (memExpr != null)
                    {
                        var derivedField = new DerivedField();
                        derivedField.Field = memExpr.Member.Name;
                        derivedField.Action = aggType;
                        this.Query.DerivedFieldList.Add(derivedField);

                        if (this.Query.DerivedFieldList == null)
                            throw new InvalidOperationException("No aggregate methods specified.");

                        return; // Success
                    }
                }
            }
            throw new InvalidOperationException("Invalid Aggregate expression arguments.");
        }

        private void HandleOrderByExpression(ReadOnlyCollection<Expression> arguments, SortDirectionConstants sortDir)
        {
            if (arguments.Count == 2)
            {
                var unaryExpr = arguments[1] as UnaryExpression;
                if (unaryExpr != null)
                {
                    var fieldSort = new FieldSort();
                    fieldSort.Name = ExpressionHelper.GetMemberName(unaryExpr.Operand);
                    fieldSort.SortDirection = sortDir;

                    //Do not throw errow for now, just week it out
                    //if (this.Query.FieldSorts.FindIndex(x => x.Name == fieldSort.Name) >= 0)
                    //    throw new InvalidOperationException($"Duplicate sort field \"{fieldSort.Name}\" was encountered.");

                    //Only add if not exists
                    if (this.Query.FieldSorts.FindIndex(x => x.Name == fieldSort.Name) == -1)
                        this.Query.FieldSorts.Add(fieldSort);

                    return; // Success
                }

                var constantExpr = arguments[1] as ConstantExpression;
                if (constantExpr != null)
                {
                    var fieldSort = new FieldSort();
                    fieldSort.Name = (string)constantExpr.Value;
                    fieldSort.SortDirection = sortDir;

                    this.Query.FieldSorts.RemoveAll(x => x.Name == fieldSort.Name);
                    this.Query.FieldSorts.Add(fieldSort);
                    return; // Success
                }

                throw new InvalidOperationException("Invalid OrderBy expression arguments.");
            }
        }

        private void HandleIncludeDimensionsExpression(ReadOnlyCollection<Expression> arguments)
        {
            if (arguments.Count == 2)
            {
                var constantExpr = arguments[1] as ConstantExpression;
                if (constantExpr != null)
                {
                    this.Query.IncludeDimensions = (bool)constantExpr.Value;
                    return; // Success
                }
                throw new InvalidOperationException("Invalid IncludeDimensions expression arguments.");
            }
        }

        private void HandleIncludeAllDimensionsExpression(ReadOnlyCollection<Expression> arguments)
        {
            if (arguments.Count == 2)
            {
                var constantExpr = arguments[1] as ConstantExpression;
                if (constantExpr != null)
                {
                    this.Query.IncludeAllDimensions = (bool)constantExpr.Value;
                    return; // Success
                }
                throw new InvalidOperationException("Invalid HandleIncludeAllDimensionsExpression expression arguments.");
            }
        }

        private void HandleIncludeIpAddressExpression(ReadOnlyCollection<Expression> arguments)
        {
            if (arguments.Count == 2)
            {
                var constantExpr = arguments[1] as ConstantExpression;
                if (constantExpr != null && constantExpr.Value != null)
                {
                    this.Query.IPMask = constantExpr.Value.ToString();
                    return; // Success
                }
                //throw new InvalidOperationException("Invalid IpAddress expression arguments.");
            }
        }

        private void HandleIncludeRecordsExpression(ReadOnlyCollection<Expression> arguments)
        {
            if (arguments.Count == 2)
            {
                var constantExpr = arguments[1] as ConstantExpression;
                if (constantExpr != null)
                {
                    this.Query.IncludeRecords = (bool)constantExpr.Value;
                    return; // Success
                }
                throw new InvalidOperationException("Invalid IncludeRecords expression arguments.");
            }
        }

        private void HandleExcludeCountExpression(ReadOnlyCollection<Expression> arguments)
        {
            if (arguments.Count == 2)
            {
                var constantExpr = arguments[1] as ConstantExpression;
                if (constantExpr != null)
                {
                    this.Query.ExcludeCount = (bool)constantExpr.Value;
                    return; // Success
                }
                throw new InvalidOperationException("Invalid ExcludeCount expression arguments.");
            }
        }

        private void HandleWhereKeywordExpression(ReadOnlyCollection<Expression> arguments)
        {
            if (arguments.Count == 2)
            {
                var constExpr = arguments[1] as ConstantExpression;
                if (constExpr != null)
                {
                    this.Query.Keyword = (string)constExpr.Value;
                }
                return; // Success
            }
            throw new InvalidOperationException("Invalid WhereKeyword expression arguments.");
        }

        private void HandleWhereUrlExpression(ReadOnlyCollection<Expression> arguments)
        {
            if (arguments.Count == 2)
            {
                var constExpr = arguments[1] as ConstantExpression;
                if (constExpr != null)
                {
                    this.Url = (string)constExpr.Value;
                    var u = new DataQuery(this.Url);

                    this.Query.PageOffset = u.PageOffset;
                    this.Query.RecordsPerPage = u.RecordsPerPage;
                    this.Query.Keyword = u.Keyword;
                    this.Query.ExcludeCount = u.ExcludeCount;
                    this.Query.IncludeDimensions = u.IncludeDimensions;
                    this.Query.IncludeEmptyDimensions = u.IncludeEmptyDimensions;
                    this.Query.ExcludeDimensionCount = u.ExcludeDimensionCount;
                    this.Query.IncludeRecords = u.IncludeRecords;
                    this.Query.NonParsedFieldList = u.NonParsedFieldList;

                    //Field filters
                    if (u.FieldFilters != null)
                        this.Query.FieldFilters.AddRange(u.FieldFilters);

                    //Dimensions
                    if (u.DimensionValueList != null)
                        this.Query.DimensionValueList.AddRange(u.DimensionValueList);

                    //DerivedFieldList
                    if (u.DerivedFieldList != null)
                        this.Query.DerivedFieldList.AddRange(u.DerivedFieldList);

                    //FieldSorts
                    if (u.FieldSorts != null)
                        this.Query.FieldSorts.AddRange(u.FieldSorts);

                    //SkipDimensions
                    if (u.SkipDimensions != null)
                        this.Query.SkipDimensions.AddRange(u.SkipDimensions);

                    return; // Success
                }
            }
            throw new InvalidOperationException("Invalid WhereUrl expression arguments.");
        }

        private void HandleFromQueryExpression(ReadOnlyCollection<Expression> arguments)
        {
            if (arguments.Count == 2)
            {
                var constExpr = arguments[1] as ConstantExpression;
                if (constExpr != null)
                {
                    var u = (DataQuery)constExpr.Value;

                    this.Query.PageOffset = u.PageOffset;
                    this.Query.RecordsPerPage = u.RecordsPerPage;
                    this.Query.Keyword = u.Keyword;
                    this.Query.ExcludeCount = u.ExcludeCount;
                    this.Query.IncludeDimensions = u.IncludeDimensions;
                    this.Query.IncludeEmptyDimensions = u.IncludeEmptyDimensions;
                    this.Query.ExcludeDimensionCount = u.ExcludeDimensionCount;
                    this.Query.IncludeRecords = u.IncludeRecords;
                    this.Query.NonParsedFieldList = u.NonParsedFieldList;

                    //Field filters
                    if (u.FieldFilters != null)
                        this.Query.FieldFilters.AddRange(u.FieldFilters);

                    //Dimensions
                    if (u.DimensionValueList != null)
                        this.Query.DimensionValueList.AddRange(u.DimensionValueList);

                    //DerivedFieldList
                    if (u.DerivedFieldList != null)
                        this.Query.DerivedFieldList.AddRange(u.DerivedFieldList);

                    //FieldSorts
                    if (u.FieldSorts != null)
                        this.Query.FieldSorts.AddRange(u.FieldSorts);

                    //FieldSorts
                    if (u.FieldSelects != null)
                    {
                        if (this.Query.FieldSelects == null) this.Query.FieldSelects = new List<string>();
                        this.Query.FieldSelects.AddRange(u.FieldSelects);
                    }

                    //SkipDimensions
                    if (u.SkipDimensions != null)
                        this.Query.SkipDimensions.AddRange(u.SkipDimensions);
                    return; // Success
                }

            }
            throw new InvalidOperationException("Invalid WhereUrl expression arguments.");
        }

        private void HandleClearSortsExpression(ReadOnlyCollection<Expression> arguments)
        {
            this.Query.FieldSorts.Clear();
        }

        private void HandleClearFiltersExpression(ReadOnlyCollection<Expression> arguments)
        {
            this.Query.FieldFilters.Clear();
        }

        private void HandleClearDimensionsExpression(ReadOnlyCollection<Expression> arguments)
        {
            this.Query.DimensionValueList.Clear();
        }

        private void HandleWhereUserExpression(ReadOnlyCollection<Expression> arguments)
        {
            if (arguments.Count == 2)
            {
                var constExpr = arguments[1] as ConstantExpression;
                if (constExpr != null)
                {
                    if (this.Users == null)
                        this.Users = new List<int>();

                    // make sure we don't add dups
                    var toAdd = (IEnumerable<int>)constExpr.Value;
                    var notExisting = toAdd.Where(x => !this.Users.Contains(x));
                    this.Users.AddRange(notExisting);
                }
                return; // Success
            }
            throw new InvalidOperationException("Invalid WhereUser expression arguments.");
        }

        private void HandleWhereDimensionValueExpression(ReadOnlyCollection<Expression> arguments)
        {
            if (arguments.Count == 2)
            {
                var constExpr = arguments[1] as ConstantExpression;
                if (constExpr != null)
                {
                    if (this.Query.DimensionValueList == null)
                        this.Query.DimensionValueList = new List<long>();

                    // make sure we don't add dups
                    var toAdd = (IEnumerable<long>)constExpr.Value;
                    var notExisting = toAdd.Where(x => !this.Query.DimensionValueList.Contains(x));
                    this.Query.DimensionValueList.AddRange(notExisting);
                }
                return; // Success
            }
            throw new InvalidOperationException("Invalid WhereDimensionValue expression arguments.");
        }

        private void HandleSkipDimensionExpression(ReadOnlyCollection<Expression> arguments)
        {
            if (arguments.Count == 2)
            {
                var constExpr = arguments[1] as ConstantExpression;
                if (constExpr != null)
                {
                    if (this.Query.SkipDimensions == null)
                        this.Query.SkipDimensions = new List<long>();

                    // make sure we don't add dups
                    var toAdd = (IEnumerable<long>)constExpr.Value;
                    var notExisting = toAdd.Where(x => !this.Query.SkipDimensions.Contains(x));
                    this.Query.SkipDimensions.AddRange(notExisting);
                }
                return; // Success
            }
            throw new InvalidOperationException("Invalid SkipDimension expression arguments.");
        }

        private void HandleRemoveDimensionValueExpression(ReadOnlyCollection<Expression> arguments)
        {
            if (arguments.Count == 2)
            {
                var constExpr = arguments[1] as ConstantExpression;
                if (constExpr != null)
                {
                    if (this.Query.DimensionValueList == null)
                        this.Query.DimensionValueList = new List<long>();

                    // make sure we don't add dups
                    var toAdd = (IEnumerable<long>)constExpr.Value;
                    this.Query.DimensionValueList.RemoveAll(x => toAdd.Contains(x));
                }
                return; // Success
            }
            throw new InvalidOperationException("Invalid WhereDimensionValue expression arguments.");
        }

    }
}
