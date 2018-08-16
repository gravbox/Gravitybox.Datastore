using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using Gravitybox.Datastore.Common;

namespace Gravitybox.Datastore.Server.Core
{
    internal static class LockingManager
    {
        private static Dictionary<Guid, DatastoreLock> _lockCache = new Dictionary<Guid, DatastoreLock>();
        private static object _locker = new object();

        internal static DatastoreLock GetLocker(Guid id)
        {
            lock (_locker)
            {
                if (!_lockCache.ContainsKey(id))
                    _lockCache.Add(id, new DatastoreLock(id));
                return _lockCache[id];
            }
        }
    }

    #region IDataLock

    internal interface IDataLock
    {
        int LockTime { get; }

        int WaitingLocksOnEntry { get; }

        int ReadLockCount { get; }
    }

    #endregion

    #region AcquireReaderLock

    internal class AcquireReaderLock : IDataLock, IDisposable
    {
        private DatastoreLock m_Lock = null;
        private bool m_Disposed = false;
        private static long _counter = 0;
        private long _lockIndex = 0;
        private DateTime _initTime = DateTime.Now;
        private const int TimeOut = 60000;
        private bool _inError = false;
        private Guid _id;

        /// <summary />
        public AcquireReaderLock(Guid id, string traceInfo)
        {
            this.LockTime = -1;
            m_Lock = LockingManager.GetLocker(id);
            _id = id;
            if (!ConfigHelper.AllowLocking) return;

            this.ReadLockCount = m_Lock.CurrentReadCount;
            this.WaitingLocksOnEntry = m_Lock.WaitingWriteCount;
            if (this.WaitingLocksOnEntry > 10)
                LoggerCQ.LogWarning($"AcquireReaderLock Waiting Writer Locks: Count={this.WaitingLocksOnEntry}, RepositoryId={id}, TraceInfo={traceInfo}");

            if (!m_Lock.TryEnterReadLock(TimeOut))
            {
                _inError = true;

                throw new Exception("Could not get reader lock: " +
                    ((m_Lock.ObjectId == Guid.Empty) ? string.Empty : "ID=" + m_Lock.ObjectId) +
                    $", CurrentReadCount={m_Lock.CurrentReadCount}" +
                    $", WaitingReadCount={m_Lock.WaitingReadCount}" +
                    $", WaitingWriteCount={m_Lock.WaitingWriteCount}" +
                    $", HoldingThread={m_Lock.HoldingThreadId}" +
                    $", TraceInfo={m_Lock.TraceInfo}" +
                    $", LockFailTime={(int)DateTime.Now.Subtract(_initTime).TotalMilliseconds}" +
                    $", WriteHeldTime={m_Lock.WriteHeldTime}");
            }

            this.LockTime = (int)DateTime.Now.Subtract(_initTime).TotalMilliseconds;
            Interlocked.Increment(ref _counter);
            _lockIndex = _counter;
            m_Lock.HeldReads.AddOrUpdate(_lockIndex, DateTime.Now, (key, value) => DateTime.Now);
            m_Lock.TraceInfo = traceInfo;
            m_Lock.HoldingThreadId = System.Threading.Thread.CurrentThread.ManagedThreadId;
        }

        public int LockTime { get; private set; }

        /// <summary>
        /// Returns the number of write locks that were in queue when creating this lock
        /// </summary>
        public int WaitingLocksOnEntry { get; private set; }

        /// <summary>
        /// Returns the number of read locks held on entry
        /// </summary>
        public int ReadLockCount { get; private set; }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <summary />
        protected virtual void Dispose(bool disposing)
        {
            if (!m_Disposed)
            {
                if (disposing && m_Lock != null)
                {
                    var traceInfo = m_Lock.TraceInfo;
                    var elapsed = (int)DateTime.Now.Subtract(_initTime).TotalMilliseconds;
                    if (!_inError)
                    {
                        if (_lockIndex != 0)
                        {
                            DateTime dt;
                            if (!m_Lock.HeldReads.TryRemove(_lockIndex, out dt))
                                LoggerCQ.LogWarning($"HeldReads was not released. ObjectId={m_Lock.ObjectId}, Index={_lockIndex}, TraceInfo={m_Lock.TraceInfo}, Elapsed={elapsed}");
                            _lockIndex = 0;
                        }
                        m_Lock.TraceInfo = null;
                        m_Lock.HoldingThreadId = null;
                    }

                    if (ConfigHelper.AllowLocking)
                        m_Lock.ExitReadLock();

                    if (elapsed > 60000)
                        LoggerCQ.LogWarning($"ReaderLock Long: Elapsed={elapsed}, ID={_id}");
                }
            }
            m_Disposed = true;
        }
    }

    #endregion

    #region AcquireWriterLock

    /// <summary />
    internal class AcquireWriterLock : IDataLock, IDisposable
    {
        private DatastoreLock m_Lock = null;
        private bool m_Disposed = false;
        private DateTime _initTime = DateTime.Now;
        private const int TimeOut = 60000;
        private bool _inError = false;

        public AcquireWriterLock(Guid id)
            : this(id, string.Empty)
        {
        }

        public AcquireWriterLock(Guid id, string traceInfo)
        {
            if (id == Guid.Empty) return;

            m_Lock = LockingManager.GetLocker(id);
            if (!ConfigHelper.AllowLocking) return;

            this.ReadLockCount = m_Lock.CurrentReadCount;
            this.WaitingLocksOnEntry = m_Lock.WaitingWriteCount;
            if (this.WaitingLocksOnEntry > 10)
                LoggerCQ.LogWarning($"AcquireWriterLock Waiting Writer Locks: Count={this.WaitingLocksOnEntry}, RepositoryId={id}, TraceInfo={traceInfo}");

            //If there is another write lock held then wait to enter and give the read locks to run
            //This is a hack to address the issue with the lock object: it prioritizes writes and starves reads
            var lockHeld = false;
            if (traceInfo == RepositoryManager.TraceInfoUpdateData)
            {
                while (m_Lock.IsWriteLockHeld && DateTime.Now.Subtract(_initTime).TotalMilliseconds < 10000)
                {
                    System.Threading.Thread.Sleep(20);
                    lockHeld = true;
                }
            }

            var delay = (int)DateTime.Now.Subtract(_initTime).TotalMilliseconds;
            if (lockHeld)
                LoggerCQ.LogDebug($"AcquireWriterLock: Held={delay}, RepositoryId={id}, TraceInfo={traceInfo}");

            var timeoutValue = TimeOut - delay;
            if (!m_Lock.TryEnterWriteLock(timeoutValue))
            {
                _inError = true;
                RepositoryManager.SystemCore.LogLockStat(new LockInfoItem
                {
                    CurrentReadCount = m_Lock.CurrentReadCount,
                    Elapsed = (int)DateTime.Now.Subtract(_initTime).TotalMilliseconds,
                    Failure = true,
                    IsWriteLockHeld = m_Lock.IsWriteLockHeld,
                    ThreadId = (m_Lock.HoldingThreadId == null) ? 0 : m_Lock.HoldingThreadId.Value,
                    WaitingReadCount = m_Lock.WaitingReadCount,
                    WaitingWriteCount = m_Lock.WaitingWriteCount,
                    TraceInfo = traceInfo,
                });

                var lapses = string.Join("-", m_Lock.HeldReads.Values.ToList().Select(x => (int)DateTime.Now.Subtract(x).TotalSeconds).ToList());
                throw new Exception("Could not get writer lock: " +
                    ((m_Lock.ObjectId == Guid.Empty) ? string.Empty : "ID=" + m_Lock.ObjectId) +
                    $", CurrentReadCount={m_Lock.CurrentReadCount}" +
                    $", WaitingReadCount={m_Lock.WaitingReadCount}" +
                    $", WaitingWriteCount={m_Lock.WaitingWriteCount}" +
                    $", HoldingThread={m_Lock.HoldingThreadId}" +
                    $", TraceInfo={m_Lock.TraceInfo}" +
                    $", WriteHeldTime={m_Lock.WriteHeldTime}" +
                    $", LockFailTime={(int)DateTime.Now.Subtract(_initTime).TotalMilliseconds}" +
                    $", Lapses={lapses}");
            }

            this.LockTime = (int)DateTime.Now.Subtract(_initTime).TotalMilliseconds;
            m_Lock.TraceInfo = traceInfo;
            m_Lock.WriteLockHeldTime = DateTime.Now;
            m_Lock.HoldingThreadId = System.Threading.Thread.CurrentThread.ManagedThreadId;
        }

        public int LockTime { get; private set; }

        /// <summary>
        /// Returns the number of write locks that were in queue when creating this lock
        /// </summary>
        public int WaitingLocksOnEntry { get; private set; }

        /// <summary>
        /// Returns the number of read locks held on entry
        /// </summary>
        public int ReadLockCount { get; private set; }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <summary />
        protected virtual void Dispose(bool disposing)
        {
            if (!m_Disposed && disposing && m_Lock != null)
            {
                var traceInfo = m_Lock.TraceInfo;
                if (!_inError)
                {
                    m_Lock.WriteLockHeldTime = null;
                    m_Lock.TraceInfo = null;
                    m_Lock.HoldingThreadId = null;
                }

                if (ConfigHelper.AllowLocking)
                {
                    m_Lock.ExitWriteLock();

                    var totalTime = (int)DateTime.Now.Subtract(_initTime).TotalMilliseconds;
                    if (totalTime > 60000)
                        LoggerCQ.LogWarning($"WriterLock Long: Elapsed={totalTime}");

                    RepositoryManager.SystemCore.LogLockStat(new LockInfoItem
                    {
                        CurrentReadCount = m_Lock.CurrentReadCount,
                        Elapsed = (int)DateTime.Now.Subtract(_initTime).TotalMilliseconds,
                        Failure = false,
                        IsWriteLockHeld = m_Lock.IsWriteLockHeld,
                        ThreadId = m_Lock.HoldingThreadId.GetValueOrDefault(),
                        WaitingReadCount = m_Lock.WaitingReadCount,
                        WaitingWriteCount = m_Lock.WaitingWriteCount,
                        TraceInfo = traceInfo
                    });

                }
            }
            m_Disposed = true;
        }
    }

    #endregion

    #region DatastoreLock

    internal class DatastoreLock : System.Threading.ReaderWriterLockSlim
    {
        public DatastoreLock(Guid objectId)
            : base(LockRecursionPolicy.SupportsRecursion)
        {
            this.ObjectId = objectId;
        }

        public bool AnyLocks()
        {
            return (this.CurrentReadCount == 0) && !this.IsWriteLockHeld;
        }

        public int WriteHeldTime
        {
            get
            {
                var retval = -1;
                if (this.WriteLockHeldTime.HasValue)
                    retval = (int)DateTime.Now.Subtract(this.WriteLockHeldTime.Value).TotalMilliseconds;
                return retval;
            }
        }

        public DateTime? WriteLockHeldTime { get; internal set; }

        public string TraceInfo { get; internal set; }

        public Guid LockID { get; private set; } = Guid.NewGuid();

        public Guid ObjectId { get; private set; }

        public int? HoldingThreadId { get; internal set; }

        public System.Collections.Concurrent.ConcurrentDictionary<long, DateTime> HeldReads { get; private set; } = new System.Collections.Concurrent.ConcurrentDictionary<long, DateTime>();
    }

    #endregion

}