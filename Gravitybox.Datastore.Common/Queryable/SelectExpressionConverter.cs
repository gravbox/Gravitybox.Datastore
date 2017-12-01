using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace Gravitybox.Datastore.Common.Queryable
{
    internal class SelectExpressionConverter : ExpressionVisitor
    {
        private object[] _itemData;
        private Dictionary<string, int> _nameIndexes;

        public SelectExpressionConverter(FieldDefinition[] fieldset)
        {
            var index = 0;
            _nameIndexes = new Dictionary<string, int>();
            foreach (var field in fieldset)
                _nameIndexes.Add(field.Name, index++);
        }

        public Expression Convert(Expression expression, object[] itemData)
        {
            _itemData = itemData;
            return Visit(expression);
        }

        protected override Expression VisitMember(MemberExpression node)
        {
            var memberName = node.Member.Name;
            if (_nameIndexes.ContainsKey(memberName))
            {
                var index = _nameIndexes[memberName];
                if (node.Type == typeof(string))
                {
                    return Expression.Constant(_itemData[index]);
                }

                if (node.Type.GenericTypeArguments.Length == 1)
                {
                    if (node.Type.GenericTypeArguments[0] == typeof(DateTime))
                        return Expression.Constant(_itemData[index], typeof(DateTime?));

                    else if (node.Type.GenericTypeArguments[0] == typeof(int))
                        return Expression.Constant((int?)_itemData[index], typeof(int?));
                    else if (node.Type.GenericTypeArguments[0] == typeof(byte))
                        return Expression.Constant((int?)_itemData[index], typeof(int?));
                    
                    //Not handled
                    //else if (node.Type.GenericTypeArguments[0] == typeof(long))
                    //    return Expression.Constant((long?)_itemData[index], typeof(long?));

                    else if (node.Type.GenericTypeArguments[0] == typeof(bool))
                        return Expression.Constant((bool?)_itemData[index], typeof(bool?));

                    else if (node.Type.GenericTypeArguments[0] == typeof(double))
                        return Expression.Constant((double?)_itemData[index], typeof(double?));
                    else if (node.Type.GenericTypeArguments[0] == typeof(Single))
                        return Expression.Constant((double?)_itemData[index], typeof(double?));
                    else if (node.Type.GenericTypeArguments[0] == typeof(decimal))
                        return Expression.Constant((double?)_itemData[index], typeof(double?));
                }

                return Expression.Constant(_itemData[index]);
            }
            return node;
        }
    }
}
