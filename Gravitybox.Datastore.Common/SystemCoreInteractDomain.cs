#pragma warning disable 0168
using Gravitybox.Datastore.Common.Queryable;
using System;
using System.Collections.Generic;
using System.Linq;
using System.ServiceModel;
using System.Text;

namespace Gravitybox.Datastore.Common
{
    /// <summary />
    public static class SystemCoreInteractDomain
    {
        private const int MaxItemSize = 20 * 1024 * 1024; //20 MB is default for all sizes

        /// <summary />
        public static ChannelFactory<ISystemCore> GetCoreFactory(string serverName)
        {
            if (string.IsNullOrEmpty(serverName))
                throw new Exception("Server not set");

            var arr = serverName.Split(':');
            if (arr.Length > 2)
                throw new Exception("Server not set");

            var port = 1973;
            if (arr.Length == 2)
            {
                port = arr[1].ToInt();
                serverName = arr[0];
            }

            //If configured for failover then grab the current server
            if (serverName == "@config")
            {
                if (FailoverConfiguration.CurrentServer != null)
                {
                    serverName = FailoverConfiguration.CurrentServer.Server;
                    port = FailoverConfiguration.CurrentServer.Port;
                }
                else
                    throw new Exception("Cannot find a configured server");
            }

            return GetCoreFactory(serverName, port);
        }

        /// <summary />
        public static ChannelFactory<ISystemCore> GetCoreFactory(string serverName, int port)
        {
            var myBinding = new NetTcpBinding() { MaxBufferSize = MaxItemSize, MaxReceivedMessageSize = MaxItemSize, MaxBufferPoolSize = 0 };
            myBinding.ReaderQuotas.MaxStringContentLength = MaxItemSize;
            myBinding.ReaderQuotas.MaxBytesPerRead = MaxItemSize;
            myBinding.ReaderQuotas.MaxArrayLength = MaxItemSize;
            myBinding.ReaderQuotas.MaxDepth = MaxItemSize;
            myBinding.ReaderQuotas.MaxNameTableCharCount = MaxItemSize;
            myBinding.Security.Mode = SecurityMode.None;
            var myEndpoint = new EndpointAddress("net.tcp://" + serverName + ":" + port + "/__datastore_core");
            return new ChannelFactory<ISystemCore>(myBinding, myEndpoint);
        }

        /// <summary />
        public static ChannelFactory<IDataModel> GetRepositoryFactory(string serverName)
        {
            return GetRepositoryFactory(serverName, 1973);
        }

        /// <summary />
        public static ChannelFactory<IDataModel> GetRepositoryFactory(string serverName, int port)
        {
            var myBinding = new NetTcpBinding() { MaxBufferSize = MaxItemSize, MaxReceivedMessageSize = MaxItemSize, MaxBufferPoolSize = 0 };
            myBinding.ReaderQuotas.MaxStringContentLength = MaxItemSize;
            myBinding.ReaderQuotas.MaxBytesPerRead = MaxItemSize;
            myBinding.ReaderQuotas.MaxArrayLength = MaxItemSize;
            myBinding.ReaderQuotas.MaxDepth = MaxItemSize;
            myBinding.ReaderQuotas.MaxNameTableCharCount = MaxItemSize;
            myBinding.Security.Mode = SecurityMode.None;
            var myEndpoint = new EndpointAddress("net.tcp://" + serverName + ":" + port + "/__datastore_engine");
            return new ChannelFactory<IDataModel>(myBinding, myEndpoint);
        }

        /// <summary />
        public static List<BaseRemotingObject> GetRepositoryPropertyList(string server, int port)
        {
            try
            {
                using (var factory = SystemCoreInteractDomain.GetCoreFactory(server, port))
                {
                    var s = factory.CreateChannel();

                    var paging = new PagingInfo() { PageOffset = 1, RecordsPerPage = 100 };
                    var retval = new List<BaseRemotingObject>();
                    do
                    {
                        var q = s.GetRepositoryPropertyList(paging);
                        if (q.Count > 0) retval.AddRange(q);
                        if (q.Count < paging.RecordsPerPage) break;
                        paging.PageOffset++;
                    } while (true);
                    return retval;
                }
            }
            catch (Exception ex)
            {
                //Logger.LogError(ex);
                throw;
            }
        }

        /// <summary />
        public static int GetRepositoryCount(string server, int port, PagingInfo paging)
        {
            try
            {
                using (var factory = SystemCoreInteractDomain.GetCoreFactory(server, port))
                {
                    var s = factory.CreateChannel();
                    return s.GetRepositoryCount(paging);
                }
            }
            catch (Exception ex)
            {
                //Logger.LogError(ex);
                throw;
            }
        }

        /// <summary />
        public static List<BaseRemotingObject> GetRepositoryPropertyList(string server, int port, PagingInfo paging)
        {
            try
            {
                using (var factory = SystemCoreInteractDomain.GetCoreFactory(server, port))
                {
                    var s = factory.CreateChannel();
                    var retval = new List<BaseRemotingObject>();
                    retval.AddRange(s.GetRepositoryPropertyList(paging));
                    return retval;
                }
            }
            catch (Exception ex)
            {
                //Logger.LogError(ex);
                throw;
            }
        }

        /// <summary />
        public static SystemStats GetSystemStats(string server, int port)
        {
            try
            {
                using (var factory = SystemCoreInteractDomain.GetCoreFactory(server, port))
                {
                    var s = factory.CreateChannel();
                    return s.GetSystemStats();
                }
            }
            catch (Exception ex)
            {
                //Logger.LogError(ex);
                throw;
            }
        }

    }
}