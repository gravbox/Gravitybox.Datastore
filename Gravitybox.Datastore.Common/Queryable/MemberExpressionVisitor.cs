using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace Gravitybox.Datastore.Common.Queryable
{
    /// <summary />
    internal class MemberExpressionVisitor : ExpressionVisitor
    {
        /// <summary />
        public List<string> Members { get; private set; }

        /// <summary />
        public MemberExpressionVisitor()
        {
            Members = new List<string>();
        }

        /// <summary />
        protected override Expression VisitMember(MemberExpression node)
        {
            if (typeof(IDatastoreItem).IsAssignableFrom(node.Expression.Type))
            {
                Members.Add(node.Member.Name);
            }
            return node;
        }
    }
}
