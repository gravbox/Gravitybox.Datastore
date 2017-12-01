using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Runtime.Serialization;
using System.ServiceModel;
using Gravitybox.Datastore.Common;

namespace Gravitybox.Datastore.Common
{
    /// <summary />
    [ServiceContract]
    public interface ISystemCore
    {
        /// <summary />
        [OperationContract]
        int GetRepositoryCount(PagingInfo paging);

        /// <summary />
        [OperationContract]
        List<BaseRemotingObject> GetRepositoryPropertyList(PagingInfo paging);

        /// <summary />
        [OperationContract]
        BaseRemotingObject SaveRepository(RepositorySchema repository);

        /// <summary />
        [OperationContract]
        bool RepositoryExists(Guid repositoryId);

        /// <summary />
        [OperationContract]
        void DeleteRepository(RepositorySchema repository);

        /// <summary />
        [OperationContract]
        void ShutDown();

        /// <summary />
        [OperationContract]
        void NotifyLoad(Guid repositoryId, int elapsed, int itemsAffected);

        /// <summary />
        [OperationContract]
        void NotifyUnload(Guid repositoryId, int elapsed, int itemsAffected);

        /// <summary />
        [OperationContract]
        RealtimeStats[] PerformanceCounters(DateTime minDate, DateTime maxDate);

        /// <summary />
        [OperationContract]
        void LogRepositoryPerf(RepositorySummmaryStats stat);

        /// <summary />
        [OperationContract]
        void LogLockStat(LockInfoItem stat);

        /// <summary />
        [OperationContract]
        RepositorySummmaryStats GetRepositoryStats(Guid repositoryId, DateTime minDate, DateTime maxDate);

        /// <summary />
        [OperationContract]
        SystemStats GetSystemStats();

        /// <summary />
        [OperationContract]
        bool IsSystemReady();

    }
}