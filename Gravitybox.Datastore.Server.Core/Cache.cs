using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace HP.Datastore.Server.Core
{
    public class Cache<K, T> : ICache<K, T>
    {
        protected TimeSpan _slidingExpiration = TimeSpan.MaxValue;
        private System.Threading.Timer _timer = null;
        private ConcurrentDictionary<K, CacheItem<T>> _internalCache = null;
        public event EventHandler ItemsRemoved;

        protected virtual void OnItemsRemoved(EventArgs e)
        {
            if (this.ItemsRemoved != null)
                this.ItemsRemoved(this, e);
        }

        public Cache(int initSize = 199)
        {
            _timer = new System.Threading.Timer(TimerTick, null, 60000, 60000);
            if (initSize < 199) initSize = 199;
            _internalCache = new ConcurrentDictionary<K, CacheItem<T>>(3, initSize);
        }

        public Cache(TimeSpan slidingExpiration, int initSize = 199)
            : this(initSize)
        {
            if (slidingExpiration.TotalMilliseconds <= 0)
                throw new Exception("Invalid expiration");

            _slidingExpiration = slidingExpiration;
        }

        public T Add(K key, T obj)
        {
            //if (string.IsNullOrEmpty(key))
            //    throw new Exception("The key must be set.");

            _internalCache.AddOrUpdate(key, new CacheItem<T>(obj), (k, v) => v);
            return obj;
        }

        public T GetOrAdd(K key, Func<K, T> valueFactory)
        {
            return _internalCache.GetOrAdd(key, (d) => { return new CacheItem<T>(valueFactory(key)); }).Item;
        }

        public T GetOrAdd(K key, T value)
        {
            return _internalCache.GetOrAdd(key, new CacheItem<T>(value)).Item;
        }

        public T Get(K key)
        {
            CacheItem<T> value;
            var b = _internalCache.TryGetValue(key, out value);
            if (value == null || value.Item == null) return default(T);
            return value.Item;
        }

        public bool Remove(K key)
        {
            CacheItem<T> result;
            return _internalCache.TryRemove(key, out result);
        }

        public int Clear()
        {
            var count = _internalCache.Count;
            _internalCache.Clear();
            return count;
        }

        private void TimerTick(object state)
        {
            lock (_internalCache)
            {
                var l = _internalCache
                    .Where(x => DateTime.Now.Subtract(x.Value.CreatedDate).TotalSeconds >= _slidingExpiration.TotalSeconds)
                    .ToList();

                var willRemove = (l.Count > 0);
                CacheItem<T> result;
                l.Select(x => x.Key).ToList().ForEach(x => _internalCache.TryRemove(x, out result));
                if (willRemove)
                    this.OnItemsRemoved(new EventArgs());
            }
        }

        private class CacheItem<R>
        {
            public CacheItem()
            {
                this.CreatedDate = DateTime.Now;
            }

            public CacheItem(R item)
                : this()
            {
                this.Item = item;
            }

            public R Item { get; set; }

            public DateTime CreatedDate { get; private set; }
        }

    }

    public interface ICache<K, T>
    {
        T Add(K key, T obj);

        T Get(K key);

        int Clear();

        bool Remove(K key);
    }

}