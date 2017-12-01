using Gravitybox.Datastore.Common;
using System;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace Gravitybox.Datastore.Common.Queryable
{
    internal class WhereExpressionParser : ExpressionVisitor
    {
        public List<IFieldFilter> FieldFilters { get; set; }

        protected override Expression VisitUnary(UnaryExpression node)
        {
            var lamdaExpr = node.Operand as LambdaExpression;
            if (lamdaExpr != null)
            {
                var binaryExpr = lamdaExpr.Body as BinaryExpression;
                if (binaryExpr != null)
                {
                    VisitBinary(binaryExpr);
                    return node;
                }

                if (lamdaExpr.Body is MemberExpression)
                {
                    VisitMember((MemberExpression)lamdaExpr.Body);
                    return node;
                }

                if (lamdaExpr.Body is UnaryExpression)
                {
                    VisitUnary((UnaryExpression)lamdaExpr.Body);
                    return node;
                }

                if (lamdaExpr.Body is MethodCallExpression)
                {
                    VisitMethodCall((MethodCallExpression)lamdaExpr.Body);
                    return node;
                }

            }

            if (node.NodeType == ExpressionType.Not)
            {
                var memberExpr = node.Operand as MemberExpression;
                if (memberExpr != null && memberExpr.Type == typeof(bool))
                {
                    var constantExpr = Expression.Constant(true);
                    ParseExpression(memberExpr, constantExpr, ComparisonConstants.NotEqual);
                    return node;
                }
            }

            throw new InvalidOperationException("Unsupported Where Syntax.");
        }

        protected override Expression VisitMethodCall(MethodCallExpression node)
        {
            if (node.Method.Name == "Contains")
            {
                var ce = node.Arguments[0] as ConstantExpression;
                if (ce != null)
                {
                    var v = (string)((ConstantExpression)node.Arguments[0]).Value;
                    AddFieldFilter(ExpressionHelper.GetMemberName(node.Object), v, ComparisonConstants.Like);
                    return node;
                }

                var fe = node.Arguments[0] as MemberExpression;
                if (fe != null)
                {
                    if (node.Object == null)
                    {
                        //x.List.Contains("v")
                        var objectMember = Expression.Convert(node.Arguments[1], typeof(object));
                        var getterLambda = Expression.Lambda<Func<object>>(objectMember);
                        var getter = getterLambda.Compile();
                        var v = getter();
                        AddFieldFilter(ExpressionHelper.GetMemberName(node.Arguments[0]), v, ComparisonConstants.Like);
                    }
                    else
                    {
                        //x.Single.Contains("v")
                        var v = ExpressionHelper.GetMemberExpressionValue(fe);
                        AddFieldFilter(ExpressionHelper.GetMemberName(node.Object), v, ComparisonConstants.Like);
                    }
                    return node;
                }

                var me = node.Arguments[0] as BinaryExpression;
                if (me != null)
                {
                    var v = ExpressionHelper.GetBinaryExpressionValue(me);
                    AddFieldFilter(ExpressionHelper.GetMemberName(node.Object), v, ComparisonConstants.Like);
                    return node;
                }

                var mc = node.Arguments[0] as MethodCallExpression;
                if (mc != null)
                {
                    var v = ExpressionHelper.GetMethodCallExpressionValue(mc);
                    AddFieldFilter(ExpressionHelper.GetMemberName(node.Object), v, ComparisonConstants.Like);
                    return node;
                }
            }

            if (node.Method.Name == "StartsWith")
            {
                var ce = node.Arguments[0] as ConstantExpression;
                if (ce != null)
                {
                    var v = (string)((ConstantExpression)node.Arguments[0]).Value;
                    AddFieldFilter(ExpressionHelper.GetMemberName(node.Object), v + "*", ComparisonConstants.Like);
                    return node;
                }

                var fe = node.Arguments[0] as MemberExpression;
                if (fe != null)
                {
                    var v = ExpressionHelper.GetMemberExpressionValue(fe);
                    AddFieldFilter(ExpressionHelper.GetMemberName(node.Object), v + "*", ComparisonConstants.Like);
                    return node;
                }

                var me = node.Arguments[0] as BinaryExpression;
                if (me != null)
                {
                    var v = ExpressionHelper.GetBinaryExpressionValue(me);
                    AddFieldFilter(ExpressionHelper.GetMemberName(node.Object), v + "*", ComparisonConstants.Like);
                    return node;
                }

                var mc = node.Arguments[0] as MethodCallExpression;
                if (mc != null)
                {
                    var v = ExpressionHelper.GetMethodCallExpressionValue(mc);
                    AddFieldFilter(ExpressionHelper.GetMemberName(node.Object), v + "*", ComparisonConstants.Like);
                    return node;
                }
            }

            if (node.Method.Name == "EndsWith")
            {
                var ce = node.Arguments[0] as ConstantExpression;
                if (ce != null)
                {
                    var v = (string)((ConstantExpression)node.Arguments[0]).Value;
                    AddFieldFilter(ExpressionHelper.GetMemberName(node.Object), "*" + v , ComparisonConstants.Like);
                    return node;
                }

                var fe = node.Arguments[0] as MemberExpression;
                if (fe != null)
                {
                    var v = ExpressionHelper.GetMemberExpressionValue(fe);
                    AddFieldFilter(ExpressionHelper.GetMemberName(node.Object), "*" + v , ComparisonConstants.Like);
                    return node;
                }

                var me = node.Arguments[0] as BinaryExpression;
                if (me != null)
                {
                    var v = ExpressionHelper.GetBinaryExpressionValue(me);
                    AddFieldFilter(ExpressionHelper.GetMemberName(node.Object), "*" + v , ComparisonConstants.Like);
                    return node;
                }

                var mc = node.Arguments[0] as MethodCallExpression;
                if (mc != null)
                {
                    var v = ExpressionHelper.GetMethodCallExpressionValue(mc);
                    AddFieldFilter(ExpressionHelper.GetMemberName(node.Object), "*" + v, ComparisonConstants.Like);
                    return node;
                }
            }

            throw new InvalidOperationException("Unsupported Where Syntax.");
        }

        protected override Expression VisitMember(MemberExpression node)
        {
            if (node.Type == typeof(bool))
            {
                var constantExpr = Expression.Constant(true);
                ParseExpression(node, constantExpr, ComparisonConstants.Equals);
            }
            return node;
        }

        protected override Expression VisitBinary(BinaryExpression node)
        {
            if (node.NodeType == ExpressionType.Equal)
            {
                ParseExpression(node, ComparisonConstants.Equals);
                return node;
            }
            else if (node.NodeType == ExpressionType.NotEqual)
            {
                ParseExpression(node, ComparisonConstants.NotEqual);
                return node;
            }
            else if (node.NodeType == ExpressionType.GreaterThan)
            {
                ParseExpression(node, ComparisonConstants.GreaterThan);
                return node;
            }
            else if (node.NodeType == ExpressionType.GreaterThanOrEqual)
            {
                ParseExpression(node, ComparisonConstants.GreaterThanOrEq);
                return node;
            }
            else if (node.NodeType == ExpressionType.LessThan)
            {
                ParseExpression(node, ComparisonConstants.LessThan);
                return node;
            }
            else if (node.NodeType == ExpressionType.LessThanOrEqual)
            {
                ParseExpression(node, ComparisonConstants.LessThanOrEq);
                return node;
            }
            else if (node.NodeType != ExpressionType.And && node.NodeType != ExpressionType.AndAlso)
            {
                throw new InvalidOperationException("Unsupport expression type. Only \"And\" is supported.");
            }
            return base.VisitBinary(node);
        }

        private void ParseExpression(BinaryExpression expression, ComparisonConstants comparison)
        {
            // Handles Where(x => X.Field == 5); Or
            // Handles Where(x => 5 == X.Field); Or
            // Handles Where(x => X.Field == variable); Or
            // Handles Where(x => variable == X.Field);

            var leftExpr = expression.Left;
            var rightExpr = expression.Right;

            #region Setup Left/Right
            //If a value and property are different nullable/not nullable there is a convert done so get the real memeber expression
            var convertPropertyExpr = leftExpr as UnaryExpression;
            if (convertPropertyExpr != null && convertPropertyExpr.NodeType == ExpressionType.Convert)
            {
                var memExp = convertPropertyExpr.Operand as MemberExpression;
                if (memExp != null) leftExpr = memExp;
            }
            convertPropertyExpr = rightExpr as UnaryExpression;
            if (convertPropertyExpr != null && convertPropertyExpr.NodeType == ExpressionType.Convert)
            {
                var memExp = convertPropertyExpr.Operand as MemberExpression;
                if (memExp != null) rightExpr = memExp;
            }
            #endregion

            if (!ParseExpression(leftExpr as MemberExpression, rightExpr, comparison))
            {
                //we are flipping this equaltion so flip the comparer
                //i.e. 1<X => X>1
                var newComp = comparison;
                switch (comparison)
                {
                    case ComparisonConstants.GreaterThan:
                        newComp = ComparisonConstants.LessThan;
                        break;
                    case ComparisonConstants.GreaterThanOrEq:
                        newComp = ComparisonConstants.LessThanOrEq;
                        break;
                    case ComparisonConstants.LessThan:
                        newComp = ComparisonConstants.GreaterThan;
                        break;
                    case ComparisonConstants.LessThanOrEq:
                        newComp = ComparisonConstants.GreaterThanOrEq;
                        break;
                }

                if (!ParseExpression(rightExpr as MemberExpression, leftExpr, newComp))
                    throw new InvalidOperationException("Invalid where condition expression.");
            }
        }

        private bool ParseExpression(MemberExpression memberExpr, Expression valueExpr, ComparisonConstants comparison)
        {
            try
            {
                if (memberExpr == null)
                    return false;

                var constantExpr = valueExpr as ConstantExpression;
                if (constantExpr != null)
                {
                    AddFieldFilter(memberExpr.Member.Name, constantExpr.Value, comparison);
                    return true;
                }

                // This is kinda of weird the compiler creates an object for the variables in a where conditions
                // For example, Where(x => x.Category != f) 

                var memberAccessValueExpr = valueExpr as MemberExpression;
                if (memberAccessValueExpr != null)
                {
                    var theValue = ExpressionHelper.GetMemberExpressionValue(memberAccessValueExpr);
                    AddFieldFilter(memberExpr.Member.Name, theValue, comparison);
                    return true;
                }

                var unaryExpressionExpr = valueExpr as UnaryExpression;
                if (unaryExpressionExpr != null)
                {
                    var theValue = ExpressionHelper.GetMemberExpressionValue(unaryExpressionExpr);
                    AddFieldFilter(memberExpr.Member.Name, theValue, comparison);
                    return true;
                }

                var methodExpressionExpr = valueExpr as MethodCallExpression;
                if (methodExpressionExpr != null)
                {
                    //Parse DateTime functions like AddDays, etc.
                    //if (methodExpressionExpr.Method.DeclaringType.FullName == "System.DateTime")
                    {
                        var objectMember = Expression.Convert(methodExpressionExpr, typeof(object));
                        var getterLambda = Expression.Lambda<Func<object>>(objectMember);
                        var getter = getterLambda.Compile();
                        var theValue = getter();
                        AddFieldFilter(memberExpr.Member.Name, theValue, comparison);
                        return true;
                    }
                }

                return false;
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex);
                return false;
            }
        }


        private void AddFieldFilter(string name, object value, ComparisonConstants comparsion)
        {
            //handle enums
            if (value != null)
            {
                if (value.GetType().BaseType.FullName == "System.Enum")
                    value = Convert.ToInt32(value);
            }

            var filter = (IFieldFilter)new FieldFilter();
            filter.Comparer = comparsion;
            filter.Name = name;
            filter.Value = value;

            if (FieldFilters == null)
                FieldFilters = new List<IFieldFilter>();

            //if (this.FieldFilters.FindIndex(x => x.Name == filter.Name) >= 0)
            //    throw new InvalidOperationException($"Duplicate filter field \"{filter.Name}\" was encountered.");

            FieldFilters.Add(filter);
        }
    }
}
