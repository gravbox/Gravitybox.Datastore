using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Gravitybox.Datastore.Common
{
    /// <summary />
    public class Cache<K, T> : ICache<K, T>
    {
        /// <summary />
        protected TimeSpan _slidingExpiration = TimeSpan.MaxValue;
        private System.Threading.Timer _timer = null;
        private ConcurrentDictionary<K, CacheItem<T>> _internalCache = null;
        /// <summary />
        [field: NonSerialized]
        public event EventHandler ItemsRemoved;

        /// <summary />
        protected virtual void OnItemsRemoved(EventArgs e)
        {
            if (this.ItemsRemoved != null)
                this.ItemsRemoved(this, e);
        }

        /// <summary />
        public Cache(int initSize = 199)
        {
            _timer = new System.Threading.Timer(TimerTick, null, 60000, 60000);
            if (initSize < 199) initSize = 199;
            _internalCache = new ConcurrentDictionary<K, CacheItem<T>>(3, initSize);
        }

        /// <summary />
        public Cache(TimeSpan slidingExpiration, int initSize = 199)
            : this(initSize)
        {
            if (slidingExpiration.TotalMilliseconds <= 0)
                throw new Exception("Invalid expiration");

            _slidingExpiration = slidingExpiration;
        }

        /// <summary />
        public T Add(K key, T value)
        {
            //if (string.IsNullOrEmpty(key))
            //    throw new Exception("The key must be set.");

            if (value == null)
                return value;

            _internalCache.AddOrUpdate(key, new CacheItem<T>(value), (k, v) => v);
            return value;
        }

        /// <summary />
        public T GetOrAdd(K key, Func<K, T> valueFactory)
        {
            return _internalCache.GetOrAdd(key, (d) => { return new CacheItem<T>(valueFactory(key)); }).Item;
        }

        /// <summary />
        public T GetOrAdd(K key, T value)
        {
            return _internalCache.GetOrAdd(key, new CacheItem<T>(value)).Item;
        }

        /// <summary />
        public T Get(K key)
        {
            CacheItem<T> value;
            var b = _internalCache.TryGetValue(key, out value);
            if (value == null || value.Item == null) return default(T);
            return value.Item;
        }

        /// <summary />
        public bool Remove(K key)
        {
            CacheItem<T> result;
            return _internalCache.TryRemove(key, out result);
        }

        /// <summary />
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

    internal interface ICache<K, T>
    {
        T Add(K key, T obj);

        T Get(K key);

        int Clear();

        bool Remove(K key);
    }

}