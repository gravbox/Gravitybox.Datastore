using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Gravitybox.Datastore.Server.Core
{
    /// <summary>
    /// This tracks if the FTS has completed for each repository. 
    /// It will only check once per minute and only if an actual keyword search has come in.
    /// </summary>
    internal class FTSReadyCache
    {
        private static Dictionary<Guid, FTSReadyCacheItem> _lockCache = new Dictionary<Guid, FTSReadyCacheItem>();
        private static object _locker = new object();

        private FTSReadyCacheItem GetItem(Guid id)
        {
            //Get a node for the repository GUID and create one if need be
            if (!_lockCache.ContainsKey(id))
                _lockCache.Add(id, new FTSReadyCacheItem { ID = id });
            return _lockCache[id];
        }

        public void Clear(Guid id)
        {
            lock (_locker)
            {
                var item = GetItem(id);
                item.IsReady = false;
            }
        }

        public bool IsReady(Guid id)
        {
            lock (_locker)
            {
                var item = GetItem(id);
                if (item.IsReady) return true;
                //If not ready then do not check more than once per minute
                if (DateTime.Now.Subtract(item.LastUpdate).TotalMinutes < 1) return false;
                item.IsReady = SqlHelper.IsFTSReady(id);
                //Reset time no matter if true or false as this is an expensive operation and should nto be run too often
                item.LastUpdate = DateTime.Now;
                return item.IsReady;
            }
        }

        private class FTSReadyCacheItem
        {
            public Guid ID { get; set; }
            public DateTime LastUpdate { get; set; } = DateTime.MinValue;
            public bool IsReady { get; set; } = false;
        }
    }
}
