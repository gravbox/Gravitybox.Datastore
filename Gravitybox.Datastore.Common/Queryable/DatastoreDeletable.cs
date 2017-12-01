using System;
using System.Linq.Expressions;

namespace Gravitybox.Datastore.Common.Queryable
{
    /// <summary />
    public class DatastoreDeletable<TSourceType> : IDatastoreDeletable<TSourceType>
    {
        private DatastoreService dsService;

        /// <summary />
        internal DatastoreDeletable(DatastoreService service)
        {
            dsService = service;
            Provider = new DatastoreProvider(dsService);
            Expression = Expression.Constant(this);
        }

        /// <summary />
        internal DatastoreDeletable(DatastoreProvider provider, Expression expression, DatastoreService service)
        {
            if (provider == null)
            {
                throw new ArgumentNullException("provider");
            }

            if (expression == null)
            {
                throw new ArgumentNullException("expression");
            }

            if (service == null)
            {
                throw new ArgumentNullException("service");
            }

            if (!typeof(IDatastoreDeletable<TSourceType>).IsAssignableFrom(expression.Type))
            {
                throw new InvalidOperationException("Expression is not of type IDatastoreDeletable");
            }

            Provider = provider;
            Expression = expression;
            dsService = service;
        }

        /// <summary />
        public Expression Expression { get; private set; }

        /// <summary />
        public IDatastoreProvider Provider { get; private set; }
    }
}
