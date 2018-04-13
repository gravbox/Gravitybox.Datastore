using System;
using System.Threading;

namespace Gravitybox.Datastore.Server.Core
{
    internal class RepositoryCacheItem
    {
        public RepositoryCacheItem(Guid repositoryKey)
        {
            Key = repositoryKey;
        }
            
        public Guid Key { get; }
            
        public bool IsInitialized { get; set; }
        public bool Exists { get; set; } = false;
        public string Xml { get; set; }
        public long VersionHash { get; set; }
        public int InternalId { get; set; }
        public bool HasParent { get; set; }

        private ReaderWriterLockSlim Lock { get; } = new ReaderWriterLockSlim();

        public RepositoryCacheLock EnterReadLock(int timeOut)
        {
            return new RepositoryCacheReadLock(Key, Lock, timeOut);
        }

        public RepositoryCacheUpgradeableLock EnterUpgradeableLock(int timeOut)
        {
            return new RepositoryCacheUpgradeableLock(Key, Lock, timeOut);
        }

        public RepositoryCacheLock EnterWriteLock(int timeOut)
        {
            return new RepositoryCacheWriteLock(Key, Lock, timeOut);
        }
    }

    internal abstract class RepositoryCacheLock : IDisposable
    {
        private readonly Guid _repositoryId;

        protected RepositoryCacheLock(Guid repositoryId, ReaderWriterLockSlim cacheLock)
        {
            _repositoryId = repositoryId;
            CacheLock = cacheLock;
        }

        protected ReaderWriterLockSlim CacheLock { get; }

        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                ExitLock();
            }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected void ThrowTimeoutException(string lockType)
        {
            throw new Exception($"Could not get schema cache {lockType} lock: " +
                                $"ID={_repositoryId}" +
                                $", CurrentReadCount={CacheLock.CurrentReadCount}" +
                                $", WaitingReadCount={CacheLock.WaitingReadCount}" +
                                $", WaitingUpgradeCount={CacheLock.WaitingUpgradeCount}" +
                                $", WaitingWriteCount={CacheLock.WaitingWriteCount}");
        }

        protected abstract void ExitLock();
    }
    
    internal class RepositoryCacheReadLock : RepositoryCacheLock
    {
        public RepositoryCacheReadLock(Guid repositoryId, ReaderWriterLockSlim cacheLock, int timeOut)
            : base(repositoryId, cacheLock)
        {
            if (!cacheLock.TryEnterReadLock(timeOut))
            {
                ThrowTimeoutException("read");
            }
        }

        protected override void ExitLock()
        {
            if (CacheLock.IsReadLockHeld)
            {
                CacheLock.ExitReadLock();
            }
        }
    }
    
    internal class RepositoryCacheUpgradeableLock : RepositoryCacheLock
    {
        private readonly int _timeOut;
        public RepositoryCacheUpgradeableLock(Guid repositoryId, ReaderWriterLockSlim cacheLock, int timeOut)
            : base(repositoryId, cacheLock)
        {
            if (!CacheLock.TryEnterUpgradeableReadLock(timeOut))
            {
                ThrowTimeoutException("upgradeable read");
            }
            _timeOut = timeOut;
        }

        protected override void ExitLock()
        {
            if (CacheLock.IsWriteLockHeld)
            {
                CacheLock.ExitWriteLock();
            }

            if (CacheLock.IsUpgradeableReadLockHeld)
            {
                CacheLock.ExitUpgradeableReadLock();
            }
        }

        public void EscalateLock()
        {
            if (!CacheLock.TryEnterWriteLock(_timeOut))
            {
                ThrowTimeoutException("escalated write");
            }
        }
    }
    
    internal class RepositoryCacheWriteLock : RepositoryCacheLock
    {
        public RepositoryCacheWriteLock(Guid repositoryId, ReaderWriterLockSlim cacheLock, int timeOut)
            : base(repositoryId, cacheLock)
        {
            if (!cacheLock.TryEnterWriteLock(timeOut))
            {
                ThrowTimeoutException("write");
            }
        }

        protected override void ExitLock()
        {
            if (CacheLock.IsWriteLockHeld)
            {
                CacheLock.ExitWriteLock();
            }
        }
    }
}
