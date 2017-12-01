using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Runtime.Remoting.Channels;
using System.ServiceModel;
using System.Runtime.Serialization;

namespace Gravitybox.Datastore.Common
{
    /// <summary />
    [ServiceContract]
    public interface IRepositoryConnection
    {
        /// <summary />
        void Clear(UserCredentials credentials, Gravitybox.Datastore.Common.IDataModel service);
        /// <summary />
        RepositorySchema GetSchema(UserCredentials credentials, Gravitybox.Datastore.Common.IDataModel service);
        /// <summary />
        void UpdateSchema(RepositorySchema schema, UserCredentials credentials, Gravitybox.Datastore.Common.IDataModel service);
        /// <summary />
        void UpdateData(RepositorySchema schema, IListingItem item, UserCredentials credentials, Gravitybox.Datastore.Common.IDataModel service);
        /// <summary />
        void UpdateData(RepositorySchema schema, IEnumerable<IListingItem> list, UserCredentials credentials, Gravitybox.Datastore.Common.IDataModel service);
        /// <summary />
        bool IsValidFormat(IListingItem item, UserCredentials credentials, Gravitybox.Datastore.Common.IDataModel service);
        /// <summary />
        void DeleteData(RepositorySchema schema, IListingItem item, UserCredentials credentials, Gravitybox.Datastore.Common.IDataModel service);
        /// <summary />
        void DeleteData(RepositorySchema schema, IEnumerable<IListingItem> list, UserCredentials credentials, Gravitybox.Datastore.Common.IDataModel service);
        /// <summary />
        void DeleteData(RepositorySchema schema, IListingQuery query, Gravitybox.Datastore.Common.IDataModel service);
        /// <summary />
        IListingResults QueryData(IListingQuery query, Gravitybox.Datastore.Common.IDataModel service);
    }
}