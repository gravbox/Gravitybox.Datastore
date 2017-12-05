using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Gravitybox.Datastore.Common.Queryable
{
    public class ServerConfig
    {
        public string Server { get; set; }
        public int Port { get; set; } = 1973;

        public override string ToString()
        {
            return $"{this.Server}:{this.Port}";
        }
    }

    public static class FailoverConfiguration
    {

        static FailoverConfiguration()
        {
        }

        private static int _RetryOnFailCount = 3;
        public static int RetryOnFailCount
        {
            get { return _RetryOnFailCount; }
            set
            {
                if (value < 0) _RetryOnFailCount = 0;
                else if (_RetryOnFailCount > 5) _RetryOnFailCount = 5;
                else _RetryOnFailCount = value;
            }
        }

        public static List<ServerConfig> Servers { get; private set; } = new List<ServerConfig>();

        private static int _index = 0;

        public static ServerConfig CurrentServer
        {
            get
            {
                if (!IsConfigured) return null;
                if (_index >= Servers.Count) _index = 0;
                return Servers[_index];
            }
        }

        public static bool IsConfigured => Servers.Any();

        internal static bool TryFailOver()
        {
            //Check the current server to ensure it is down
            try
            {
                using (var repo = new DatastoreRepository<TestItem>(Guid.Empty, CurrentServer.Server, CurrentServer.Port))
                {
                    //If we have found the live server then return
                    if (repo.IsServerMaster())
                        return true;
                }
            }
            catch (System.ServiceModel.EndpointNotFoundException ex)
            {
                //Ignore for now. We will assume this server is down
            }
            catch (Exception ex)
            {
                throw;
            }

            //Since the current server is down, check if there is a server running marked as master
            var startIndex = _index;
            var currentIndex = _index;
            var checkValue = CurrentServer;
            do
            {
                try
                {
                    using (var repo = new DatastoreRepository<TestItem>(Guid.Empty, checkValue.Server, checkValue.Port))
                    {
                        //If we have found the master server then return
                        if (repo.IsServerMaster())
                        {
                            _index = currentIndex;
                            return true;
                        }
                    }
                    currentIndex++; //next index
                    if (currentIndex >= Servers.Count) currentIndex = 0;
                    if (startIndex != currentIndex)
                        checkValue = Servers[currentIndex];
                    else
                        checkValue = null; //to end loop
                }
                catch (System.ServiceModel.EndpointNotFoundException ex)
                {
                    //There was an error connecting to the server to try to 
                    //The end point cannot be found so switch to next server
                    currentIndex++; //next index
                    if (currentIndex >= Servers.Count) currentIndex = 0;
                    if (startIndex != currentIndex)
                        checkValue = Servers[currentIndex];
                    else
                        checkValue = null; //to end loop
                }
            } while (checkValue != null);

            //If got to here there is no server marked as master to loop and find first server online and make it master
            startIndex = _index;
            currentIndex = _index;
            checkValue = CurrentServer;
            do
            {
                try
                {
                    using (var repo = new DatastoreRepository<TestItem>(Guid.Empty, checkValue.Server, checkValue.Port))
                    {
                        //If the server is alive then promote it to master
                        repo.IsServerAlive();
                        if (repo.ResetMaster())
                        {
                            _index = currentIndex;
                            return true;
                        }
                    }
                }
                catch (System.ServiceModel.EndpointNotFoundException ex)
                {
                    //There was an error connecting to the server to try to 
                    //The end point cannot be found so switch to next server
                    currentIndex++; //next index
                    if (currentIndex >= Servers.Count) currentIndex = 0;
                    if (startIndex != currentIndex)
                        checkValue = Servers[currentIndex];
                    else
                        checkValue = null; //to end loop
                }
            } while (checkValue != null);

            throw new Exception("No master server could be found.");
        }

        private class TestItem : IDatastoreItem
        {
            public Dictionary<string, string> ExtraValues { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }
            public long __RecordIndex { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }
            public long __OrdinalPosition { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }
            public int __Timestamp { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }
        }
    }
}
