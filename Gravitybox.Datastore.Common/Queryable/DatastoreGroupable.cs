using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace Gravitybox.Datastore.Common.Queryable
{
    /// <summary />
    internal class DatastoreGroupable<TSourceType> : IDatastoreGroupable<TSourceType>
    {
        private DatastoreService dsService;

        /// <summary />
        public DatastoreGroupable(DatastoreService service)
        {
            dsService = service;
            Provider = new DatastoreProvider(dsService);
            Expression = Expression.Constant(this);
        }

        /// <summary />
        public DatastoreGroupable(DatastoreProvider provider, Expression expression, DatastoreService service)
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

            if (!typeof(IDatastoreGroupable<TSourceType>).IsAssignableFrom(expression.Type))
            {
                throw new InvalidOperationException("Expression is not of type IDatastoreGroupable");
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
