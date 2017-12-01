using System;
using System.Linq.Expressions;

namespace Gravitybox.Datastore.Common.Queryable
{
    /// <summary />
    internal class DatastoreQueryable<TSourceType> : IDatastoreOrderedQueryable<TSourceType>
    {
        private DatastoreService dsService;

        /// <summary />
        public DatastoreQueryable(DatastoreService service)
        {
            dsService = service;
            Provider = new DatastoreProvider(dsService);
            Expression = Expression.Constant(this);
        }

        /// <summary />
        public DatastoreQueryable(DatastoreProvider provider, Expression expression, DatastoreService service)
        {
            if (provider == null)
            {
                throw new ArgumentNullException(nameof(provider));
            }

            if (expression == null)
            {
                throw new ArgumentNullException(nameof(expression));
            }

            if (service == null)
            {
                throw new ArgumentNullException(nameof(service));
            }

            if (!typeof(IDatastoreQueryable<TSourceType>).IsAssignableFrom(expression.Type))
            {
                throw new InvalidOperationException("Expression is not of type IDatastoreQueryable");
            }

            Provider = provider;
            Expression = expression;
            dsService = service;
        }

        /// <summary />
        public Type ElementType
        {
            get { return typeof(TSourceType); }
        }

        /// <summary />
        public Expression Expression { get; private set; }

        /// <summary />
        public IDatastoreProvider Provider { get; private set; }

        /// <summary />
        public IDatastoreQueryable<TSourceType> Clone()
        {
            var retval = new DatastoreQueryable<TSourceType>(dsService);
            retval.Expression = this.Expression;
            retval.Provider = this.Provider;
            return retval;
        }
    }
}
