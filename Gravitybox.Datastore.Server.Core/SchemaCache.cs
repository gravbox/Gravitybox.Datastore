using System;
using System.Collections.Concurrent;
using System.Linq;
using Gravitybox.Datastore.Common;
using Gravitybox.Datastore.EFDAL;

namespace Gravitybox.Datastore.Server.Core
{
    internal class SchemaCache
    {
        private const int LockTimeOut = 60000;
        
        private readonly ConcurrentDictionary<Guid, RepositoryCacheItem> _schemaCache = new ConcurrentDictionary<Guid, RepositoryCacheItem>();
        private readonly ConcurrentDictionary<int, int?> _schemaParentCache = new ConcurrentDictionary<int, int?>();
        
        internal RepositorySchema GetSchema(Guid repositoryId, bool clear = false)
        {
            var schemaXml = GetSchemaValue(repositoryId, clear, c => c.Xml);
            return RepositorySchema.CreateFromXml(schemaXml);
        }

        private T GetSchemaValue<T>(Guid repositoryId, bool clear, Func<RepositoryCacheItem, T> valueGetter)
        {
            var cacheItem = GetSchemaCacheItem(repositoryId);
            if (!clear && TryGetCachedSchemaValue(cacheItem, valueGetter, out var value))
            {
                return value;
            }
            
            using (var locker = cacheItem.EnterUpgradeableLock(LockTimeOut))
            {
                // We have successfully entered the upgradeable lock, which means any other updates to the cache item
                // are finished and we have the exclusive right to update the cache if needed.  We might have been
                // blocked behind another upgradeable thread that was handling the reload, so first we double-check
                // that we still need to update the schema data.  We only escalate to a write lock when we know for
                // certain that we are going to update the cache, which means we won't block other cache reads if we
                // don't actually need to update.
                if (clear || !cacheItem.IsInitialized)
                {
                    // Yes, we do need to update, but first we need to escalate to a full write lock.
                    locker.EscalateLock();

                    if (clear) ClearSchemaCacheItem(cacheItem);
                    InitializeSchemaCacheItem(cacheItem);
                }
                
                return cacheItem.Exists ? valueGetter(cacheItem) : default(T);
            }
        }
        
        private static bool TryGetCachedSchemaValue<T>(RepositoryCacheItem cacheItem, Func<RepositoryCacheItem, T> valueGetter, out T value)
        {
            value = default(T);
            using (cacheItem.EnterReadLock(LockTimeOut))
            {
                if (cacheItem.IsInitialized)
                {
                    value = cacheItem.Exists ? valueGetter(cacheItem) : default(T);
                    return true;
                }

                return false;
            }
        }

        private RepositoryCacheItem GetSchemaCacheItem(Guid repositoryId)
        {
            return _schemaCache.GetOrAdd(repositoryId, k => new RepositoryCacheItem(k));
        }

        private static void InitializeSchemaCacheItem(RepositoryCacheItem cacheItem)
        {
            using (var context = new DatastoreEntities())
            {
                var r = context.Repository.FirstOrDefault(x => x.UniqueKey == cacheItem.Key && !x.IsDeleted && x.IsInitialized);
                if (r == null)
                {
                    cacheItem.IsInitialized = true;
                    cacheItem.Exists = false;
                    return;
                };
                
                var retval = new RepositorySchema
                {
                    InternalID = r.RepositoryId,
                    ChangeStamp = r.Changestamp
                };
                retval.LoadXml(r.DefinitionData);

                cacheItem.IsInitialized = true;
                cacheItem.Exists = true;
                cacheItem.Xml = retval.ToXml(true);
                cacheItem.VersionHash = retval.VersionHash; // Cache the version to reduce number of calculations
                cacheItem.InternalId = retval.InternalID;
                cacheItem.HasParent = (r.ParentId != null);
            }
        }

        internal long GetSchemaHash(Guid repositoryId)
        {
            return GetSchemaValue(repositoryId, false, c => c.VersionHash);
        }

        internal int? GetSchemaParentId(int repositoryId)
        {
            return _schemaParentCache.GetOrAdd(repositoryId, (q) =>
            {
                using (var context = new DatastoreEntities())
                {
                    return context.Repository
                        .Where(x => x.RepositoryId == repositoryId)
                        .Select(x => x.ParentId)
                        .FirstOrDefault();
                }
            });
        }

        internal void ClearSchemaCache(Guid repositoryId)
        {
            var cacheItem = GetSchemaCacheItem(repositoryId);
            using (cacheItem.EnterWriteLock(LockTimeOut))
            {
                ClearSchemaCacheItem(cacheItem);
            }
        }

        private void ClearSchemaCacheItem(RepositoryCacheItem cacheItem)
        {
            if (cacheItem.IsInitialized && cacheItem.Exists)
            {
                _schemaParentCache.TryRemove(cacheItem.InternalId, out var _);
            }

            cacheItem.IsInitialized = false;
            cacheItem.Xml = null;
        }

        internal void Reset()
        {
            // TODO: Locking?
            _schemaCache.Clear();
            _schemaParentCache.Clear();
        }

        internal void Populate()
        {
            try
            {
                using (var context = new DatastoreEntities())
                {
                    var list = context.Repository
                        .Select(x => new { x.RepositoryId, x.ParentId })
                        .ToList();

                    foreach (var item in list)
                    {
                        _schemaParentCache.TryAdd(item.RepositoryId, item.ParentId);
                    }
                }
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex, "Populate schema parent ID cache failed");
            }
        }
    }
}