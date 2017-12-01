using System;
using System.Collections.Generic;
using System.Linq;
using System.ServiceModel;
using System.Runtime.Serialization;
using Celeriq.Common;

namespace Celeriq.Server.Interfaces
{
    [ServiceContract]
    public interface IRepository
    {
        //[OperationContract]
        //int StartUp(ServiceStartup startup);

        //[OperationContract]
        //void ShutDown();

        //[OperationContract]
        //void Reset(ServiceStartup startup);

        [OperationContract]
        void LoadData(UserCredentials credentials);

        //[OperationContract]
        //Guid GetID();

        [OperationContract]
        void UpdateIndexList(IEnumerable<DataItem> list, UserCredentials credentials);

        [OperationContract]
        void DeleteData(IEnumerable<DataItem> list, UserCredentials credentials);

        [OperationContract]
        DataQueryResults Query(DataQuery query);

        [OperationContract]
        void Clear(UserCredentials credentials);

        [OperationContract]
        bool Backup(UserCredentials credentials, string backupFile);

        [OperationContract]
        bool ExportSchema(UserCredentials credentials, string backupFile);

        [OperationContract]
        bool Restore(UserCredentials credentials, string backupFile);

        [OperationContract]
        long GetDataDiskSize(UserCredentials credentials);

        [OperationContract]
        long GetDataMemorySize(UserCredentials credentials);

        [OperationContract]
        long GetItemCount(UserCredentials credentials);

        [OperationContract]
        void UnloadData();

        [OperationContract]
        bool IsValidFormat(DataItem item, UserCredentials credentials);

        [DataMember]
        DateTime LastAccess { get; }

        [OperationContract]
        ProfileItem[] GetProfile(UserCredentials credentials, long lastProfileId);
        
        [OperationContract]
        void FlushCache(bool useMemory = false);
        
        [DataMember]
        Celeriq.Utilities.CeleriqLock SyncObject { get; set; }
    }
}