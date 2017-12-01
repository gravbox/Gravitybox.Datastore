using System;
using System.Linq.Expressions;

namespace Gravitybox.Datastore.Common.Queryable
{
    /// <summary />
    internal class ExpressionHelper
    {
        /// <summary />
        public static string GetMemberName(Expression expression)
        {
            var memberExpr = expression as MemberExpression;
            if (memberExpr == null)
            {
                var lambda = expression as LambdaExpression;
                if (lambda != null)
                {
                    memberExpr = lambda.Body as MemberExpression;
                    if (memberExpr == null)
                    {
                        // This will handle a convert expression
                        var ue = lambda.Body as UnaryExpression;
                        if (ue != null)
                        {
                            memberExpr = ue.Operand as MemberExpression;
                        }
                    }
                }

                if (memberExpr == null)
                    throw new ArgumentException("Unable to convert Expression to a MemberExpression");
            }
            return memberExpr.Member.Name;
        }

        /// <summary />
        public static object GetMemberExpressionValue(MemberExpression member)
        {
            // Magic happens here.
            var objectMember = Expression.Convert(member, typeof(object));
            var getterLambda = Expression.Lambda<Func<object>>(objectMember);
            var getter = getterLambda.Compile();
            return getter();
        }

        /// <summary />
        public static object GetMemberExpressionValue(UnaryExpression member)
        {
            // Magic happens here.
            var objectMember = Expression.Convert(member, typeof(object));
            var getterLambda = Expression.Lambda<Func<object>>(objectMember);
            var getter = getterLambda.Compile();
            return getter();
        }
        
        public static object GetMethodCallExpressionValue(MethodCallExpression member)
        {
            // Magic happens here.
            var objectMember = Expression.Convert(member, typeof(object));
            var getterLambda = Expression.Lambda<Func<object>>(objectMember);
            var getter = getterLambda.Compile();
            return getter();
        }

        public static object GetBinaryExpressionValue(BinaryExpression member)
        {
            // Magic happens here.
            var objectMember = Expression.Convert(member, typeof(object));
            var getterLambda = Expression.Lambda<Func<object>>(objectMember);
            var getter = getterLambda.Compile();
            return getter();
        }

    }
}
