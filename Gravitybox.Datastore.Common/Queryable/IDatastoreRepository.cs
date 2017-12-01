using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Gravitybox.Datastore.Common.Queryable
{
    /// <summary />
    public interface IDatastoreRepository<TSourceType> : IDatastoreRepository
        where TSourceType : IDatastoreItem
    {
        /// <summary />
        IDatastoreDeletable<TSourceType> Delete { get; }
        /// <summary />
        IDatastoreQueryable<TSourceType> Query { get; }
        /// <summary />
        IDatastoreSliceable<TSourceType> Slice { get; }
        /// <summary />
        IDatastoreUpdatable<TSourceType> Update { get; }

        /// <summary />
        ActionDiagnostics InsertOrUpdate(List<TSourceType> itemsToInsert);
        /// <summary />
        ActionDiagnostics InsertOrUpdate(TSourceType item);
        /// <summary />
        Task<ActionDiagnostics> InsertOrUpdateAsync(List<TSourceType> itemsToInsert);
        /// <summary />
        Task<ActionDiagnostics> InsertOrUpdateAsync(TSourceType item);
    }

    /// <summary />
    public interface IDatastoreRepository
    {
        /// <summary />
        DateTime InstanceCreated { get; }
        /// <summary />
        long ObjectLifetime { get; }
        /// <summary />
        Guid RepositoryId { get; }

        /// <summary />
        void AddPermissions(IEnumerable<PermissionItem> list);
        /// <summary />
        void ClearPermissions(int? fieldValue);
        /// <summary />
        void ClearPermissions(string fieldValue);
        /// <summary />
        void ClearUserPermissions(int userId);
        /// <summary />
        ActionDiagnostics ClearRepository();
        /// <summary />
        void CreateRepository(string name = null);
        /// <summary />
        void CreateRepository(Guid repositoryid, string name = null);
        /// <summary />
        bool DeleteDimensionValue(long dvidx);
        /// <summary />
        void DeletePermissions(IEnumerable<PermissionItem> list);
        /// <summary />
        bool DeleteRepository();
        /// <summary />
        long GetDataVersion();
        /// <summary />
        RepositorySchema GetSchema();
        /// <summary />
        int GetTimestamp();
        /// <summary />
        bool IsValid();
        /// <summary />
        RepositorySchema LoadSchemaTemplate();
        /// <summary />
        bool RepositoryExists();
        /// <summary />
        bool ResetDimensionValue(long dvidx, string value);
        /// <summary />
        void ResetSchema(RepositorySchema schema);
        /// <summary />
        ActionDiagnostics UpdateSchema(RepositorySchema schema);
        /// <summary />
        ActionDiagnostics UpdateSchema(Type datastoreType);
        /// <summary />
        ActionDiagnostics UpdateSchema(Type datastoreType, Guid repositoryId);
    }
}