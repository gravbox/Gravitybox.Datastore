using Gravitybox.Datastore.Common.Queryable;
using System;
using System.Collections.Generic;
using System.ServiceModel;

namespace Gravitybox.Datastore.Common
{
    /// <summary />
    [ServiceContract()]
    public interface IDataModel
    {
        /// <summary />
        [OperationContract]
        ActionDiagnostics UpdateData(RepositorySchema schema, IEnumerable<DataItem> list);

        /// <summary />
        [OperationContract]
        ActionDiagnostics UpdateDataWhere(RepositorySchema schema, DataQuery query, IEnumerable<DataFieldUpdate> list);

        /// <summary />
        [OperationContract]
        void UpdateDataWhereAsync(RepositorySchema schema, DataQuery query, IEnumerable<DataFieldUpdate> list);

        /// <summary />
        [OperationContract]
        ActionDiagnostics DeleteItems(RepositorySchema schema, IEnumerable<DataItem> item);

        /// <summary />
        [OperationContract]
        ActionDiagnostics DeleteData(RepositorySchema schema, DataQuery query);

        /// <summary />
        [OperationContract]
        DataQueryResults Query(Guid repositoryId, DataQuery query);

        /// <summary />
        [OperationContract]
        byte[] QueryAndStream(Guid repositoryId, DataQuery query);

        /// <summary />
        [OperationContract]
        int GetLastTimestamp(Guid repositoryId, DataQuery query);

        /// <summary />
        [OperationContract]
        int GetTimestamp();

        /// <summary />
        [OperationContract]
        SummarySliceValue CalculateSlice(Guid repositoryId, SummarySlice slice);

        /// <summary />
        [OperationContract]
        ActionDiagnostics Clear(Guid repositoryId);

        /// <summary />
        [OperationContract]
        bool IsValidFormat(Guid repositoryId, DataItem item);

        /// <summary />
        [OperationContract]
        long GetItemCount(Guid repositoryId);

        /// <summary />
        [OperationContract]
        ActionDiagnostics UpdateSchema(RepositorySchema repository);

        /// <summary />
        [OperationContract]
        RepositorySchema GetSchema(Guid repositoryId);

        /// <summary />
        [OperationContract]
        bool RepositoryExists(Guid repositoryId);

        /// <summary />
        [OperationContract]
        Guid QueryAsync(Guid repositoryId, DataQuery query);

        /// <summary />
        [OperationContract]
        bool QueryAsyncReady(Guid key);

        /// <summary />
        [OperationContract]
        byte[] QueryAsyncDownload(Guid key, long chunk);

        /// <summary />
        [OperationContract]
        int GetDataVersion(Guid repositoryId);

        /// <summary />
        [OperationContract]
        bool ResetDimensionValue(Guid repositoryId, long dvidx, string value);

        /// <summary />
        [OperationContract]
        bool DeleteDimensionValue(Guid repositoryId, long dvidx);

        /// <summary />
        [OperationContract]
        void AddPermission(Guid repositoryId, IEnumerable<PermissionItem> list);

        /// <summary />
        [OperationContract]
        void DeletePermission(Guid repositoryId, IEnumerable<PermissionItem> list);

        /// <summary />
        [OperationContract]
        void ClearPermissions(Guid repositoryId, string fieldValue);

        /// <summary />
        [OperationContract]
        void ClearUserPermissions(Guid repositoryId, int userId);
    }
}